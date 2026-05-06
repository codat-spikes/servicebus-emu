using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var config = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
    .AddEnvironmentVariables()
    .Build();

var asbe = new AsbeOptions();
config.GetSection("Asbe").Bind(asbe);

await using var services = new ServiceCollection()
    .AddLogging(builder =>
    {
        builder.AddConfiguration(config.GetSection("Logging"));
        builder.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss.fff ";
        });
    })
    .BuildServiceProvider();

var loggerFactory = services.GetRequiredService<ILoggerFactory>();
var log = loggerFactory.CreateLogger("asbe");

TracerProvider? tracerProvider = null;
if (!string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT")))
{
    tracerProvider = Sdk.CreateTracerProviderBuilder()
        .ConfigureResource(r => r.AddService(
            serviceName: Environment.GetEnvironmentVariable("OTEL_SERVICE_NAME") ?? "asbe"))
        .AddSource(Telemetry.SourceName)
        .AddOtlpExporter()
        .Build();
    log.LogInformation("OpenTelemetry tracing enabled (OTLP)");
}

var port = config.GetValue<int?>("Asbe:Port") ?? AmqpServer.DefaultPort;
var host = config["Asbe:Host"] ?? "127.0.0.1";
var server = new AmqpServer(new Dictionary<string, QueueOptions>(), loggerFactory, port, host);

foreach (var q in asbe.Queues)
{
    if (string.IsNullOrWhiteSpace(q.Name))
    {
        log.LogWarning("Skipping queue entry with no Name");
        continue;
    }
    server.CreateQueue(q.Name, q.ToOptions());
    log.LogInformation("Configured queue '{Name}'", q.Name);
}

foreach (var t in asbe.Topics)
{
    if (string.IsNullOrWhiteSpace(t.Name))
    {
        log.LogWarning("Skipping topic entry with no Name");
        continue;
    }
    var subs = new Dictionary<string, SubscriptionOptions>(StringComparer.Ordinal);
    foreach (var s in t.Subscriptions)
    {
        if (string.IsNullOrWhiteSpace(s.Name))
        {
            log.LogWarning("Skipping subscription entry with no Name on topic '{Topic}'", t.Name);
            continue;
        }
        var rules = s.Rules
            .Select(r => r.ToFilter())
            .Where(f => f is not null)
            .Cast<RuleFilter>()
            .ToArray();
        subs[s.Name] = new SubscriptionOptions(s.ToQueueOptions(), rules.Length == 0 ? null : rules);
    }
    server.CreateTopic(t.Name, new TopicOptions(
        subs,
        t.RequiresDuplicateDetection,
        t.DuplicateDetectionHistoryTimeWindow));
    log.LogInformation("Configured topic '{Name}' with {Count} subscription(s)", t.Name, subs.Count);
}

log.LogInformation("Starting server on port {Port}", port);
server.Start();

log.LogInformation("asbe ready — press Ctrl+C to stop");
var stop = new TaskCompletionSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; stop.TrySetResult(); };
AppDomain.CurrentDomain.ProcessExit += (_, _) => stop.TrySetResult();
await stop.Task;
log.LogInformation("Shutting down");
tracerProvider?.Dispose();
await server.DisposeAsync();
