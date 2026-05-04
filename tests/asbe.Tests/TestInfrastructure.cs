using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Configuration;
using Xunit;

public enum Transport { Local, Azure }

internal static class TestData
{
    public static TheoryData<Transport> Transports => new() { Transport.Local, Transport.Azure };

    public static readonly QueueOptions DefaultOptions = QueueOptions.Default;
    public static readonly QueueOptions FastOptions = new(TimeSpan.FromSeconds(5), 3);
    public static readonly QueueOptions SessionOptions = QueueOptions.Default with { RequiresSession = true };
    public static readonly QueueOptions FastSessionOptions = new(TimeSpan.FromSeconds(5), 3, RequiresSession: true);
}

internal sealed class TestQueue : IAsyncDisposable
{
    public required ServiceBusClient Client { get; init; }
    public required string ConnectionString { get; init; }
    public required string Name { get; init; }
    public required Func<ValueTask> Cleanup { private get; init; }

    public ServiceBusClient NewClient() => new(ConnectionString);

    public static async Task<TestQueue> CreateAsync(Transport transport, QueueOptions options, CancellationToken ct)
    {
        var name = $"test-{Guid.NewGuid():N}";
        switch (transport)
        {
            case Transport.Local:
                LocalServer.EnsureStarted();
                LocalServer.Server.CreateQueue(name, options);
                return new TestQueue
                {
                    Client = new ServiceBusClient(AmqpServer.LocalConnectionString),
                    ConnectionString = AmqpServer.LocalConnectionString,
                    Name = name,
                    Cleanup = () => { LocalServer.Server.DeleteQueue(name); return ValueTask.CompletedTask; },
                };

            case Transport.Azure:
                var conn = TestConfig.Value["ServiceBus:ConnectionString"];
                Assert.SkipWhen(string.IsNullOrWhiteSpace(conn), "ServiceBus:ConnectionString not set; run `task sb:up` and put the connection string in tests/asbe.Tests/appsettings.test.json (see appsettings.test.example.json).");
                var admin = new ServiceBusAdministrationClient(conn);
                try
                {
                    await admin.CreateQueueAsync(new CreateQueueOptions(name)
                    {
                        LockDuration = options.LockDuration,
                        MaxDeliveryCount = options.MaxDeliveryCount,
                        RequiresSession = options.RequiresSession,
                    }, ct);
                }
                catch (Exception ex) when (options.RequiresSession && ex.Message.Contains("RequiresSession", StringComparison.Ordinal))
                {
                    // Sessions require Standard+ tier; the default `task sb:up` provisions Basic.
                    // Skip Azure parity for session tests until the namespace is upgraded.
                    Assert.Skip($"Azure namespace does not support sessions (likely Basic SKU). Upgrade the namespace to Standard to run session parity tests.");
                }
                return new TestQueue
                {
                    Client = new ServiceBusClient(conn!),
                    ConnectionString = conn!,
                    Name = name,
                    Cleanup = async () => { await admin.DeleteQueueAsync(name, CancellationToken.None); },
                };

            default:
                throw new ArgumentOutOfRangeException(nameof(transport));
        }
    }

    public async ValueTask DisposeAsync()
    {
        await Client.DisposeAsync();
        await Cleanup();
    }
}

internal sealed class TestTopic : IAsyncDisposable
{
    public required ServiceBusClient Client { get; init; }
    public required string TopicName { get; init; }
    public required IReadOnlyList<string> SubscriptionNames { get; init; }
    public required Func<ValueTask> Cleanup { private get; init; }

    public string SubscriptionPath(string subscription) => $"{TopicName}/Subscriptions/{subscription}";

    public static Task<TestTopic> CreateAsync(Transport transport, IReadOnlyList<(string Name, QueueOptions Options)> subscriptions, CancellationToken ct) =>
        CreateAsync(transport, subscriptions.Select(s => (s.Name, s.Options, (RuleFilter?)null)).ToArray(), ct);

    public static async Task<TestTopic> CreateAsync(Transport transport, IReadOnlyList<(string Name, QueueOptions Options, RuleFilter? Filter)> subscriptions, CancellationToken ct)
    {
        var topicName = $"topic-{Guid.NewGuid():N}";
        switch (transport)
        {
            case Transport.Local:
                LocalServer.EnsureStarted();
                LocalServer.Server.CreateTopic(topicName, new TopicOptions(
                    subscriptions.ToDictionary(
                        s => s.Name,
                        s => new SubscriptionOptions(s.Options, s.Filter is null ? null : [s.Filter]),
                        StringComparer.Ordinal)));
                return new TestTopic
                {
                    Client = new ServiceBusClient(AmqpServer.LocalConnectionString),
                    TopicName = topicName,
                    SubscriptionNames = subscriptions.Select(s => s.Name).ToArray(),
                    Cleanup = () => { LocalServer.Server.DeleteTopic(topicName); return ValueTask.CompletedTask; },
                };

            case Transport.Azure:
                var conn = TestConfig.Value["ServiceBus:ConnectionString"];
                Assert.SkipWhen(string.IsNullOrWhiteSpace(conn), "ServiceBus:ConnectionString not set; run `task sb:up` and put the connection string in tests/asbe.Tests/appsettings.test.json (see appsettings.test.example.json).");
                var admin = new ServiceBusAdministrationClient(conn);
                try
                {
                    await admin.CreateTopicAsync(new CreateTopicOptions(topicName), ct);
                    foreach (var (name, options, filter) in subscriptions)
                    {
                        var subOpts = new CreateSubscriptionOptions(topicName, name)
                        {
                            LockDuration = options.LockDuration,
                            MaxDeliveryCount = options.MaxDeliveryCount,
                            RequiresSession = options.RequiresSession,
                        };
                        if (filter is null)
                        {
                            await admin.CreateSubscriptionAsync(subOpts, ct);
                        }
                        else
                        {
                            await admin.CreateSubscriptionAsync(
                                subOpts,
                                new CreateRuleOptions("$Default", ToAzureFilter(filter)),
                                ct);
                        }
                    }
                }
                catch (Exception ex) when (ex.Message.Contains("'Basic' tier", StringComparison.Ordinal))
                {
                    // Topics require Standard tier; the default `task sb:up` provisions Basic.
                    Assert.Skip("Azure namespace does not support topics (Basic SKU). Upgrade the namespace to Standard to run topic parity tests.");
                }
                catch (Exception ex) when (subscriptions.Any(s => s.Options.RequiresSession) && ex.Message.Contains("RequiresSession", StringComparison.Ordinal))
                {
                    await admin.DeleteTopicAsync(topicName, CancellationToken.None);
                    Assert.Skip($"Azure namespace does not support sessions (likely Basic SKU). Upgrade the namespace to Standard to run session parity tests.");
                }
                return new TestTopic
                {
                    Client = new ServiceBusClient(conn!),
                    TopicName = topicName,
                    SubscriptionNames = subscriptions.Select(s => s.Name).ToArray(),
                    Cleanup = async () => { await admin.DeleteTopicAsync(topicName, CancellationToken.None); },
                };

            default:
                throw new ArgumentOutOfRangeException(nameof(transport));
        }
    }

    public async ValueTask DisposeAsync()
    {
        await Client.DisposeAsync();
        await Cleanup();
    }

    private static Azure.Messaging.ServiceBus.Administration.RuleFilter ToAzureFilter(RuleFilter filter) => filter switch
    {
        TrueRuleFilter => new Azure.Messaging.ServiceBus.Administration.TrueRuleFilter(),
        SqlRuleFilter sql => new Azure.Messaging.ServiceBus.Administration.SqlRuleFilter(sql.Expression),
        CorrelationRuleFilter c => BuildAzureCorrelation(c),
        _ => throw new NotSupportedException($"Cannot translate {filter.GetType().Name} to Azure SDK filter."),
    };

    private static Azure.Messaging.ServiceBus.Administration.CorrelationRuleFilter BuildAzureCorrelation(CorrelationRuleFilter c)
    {
        var f = new Azure.Messaging.ServiceBus.Administration.CorrelationRuleFilter
        {
            CorrelationId = c.CorrelationId,
            MessageId = c.MessageId,
            To = c.To,
            ReplyTo = c.ReplyTo,
            Subject = c.Label,
            SessionId = c.SessionId,
            ReplyToSessionId = c.ReplyToSessionId,
            ContentType = c.ContentType,
        };
        if (c.Properties is not null)
            foreach (var (k, v) in c.Properties) f.ApplicationProperties[k] = v;
        return f;
    }
}

internal static class TestConfig
{
    public static readonly IConfiguration Value = new ConfigurationBuilder()
        .AddJsonFile("appsettings.test.json", optional: true, reloadOnChange: false)
        .Build();
}

internal static class LocalServer
{
    private static readonly Lock _gate = new();
    private static AmqpServer? _server;

    public static AmqpServer Server => _server ?? throw new InvalidOperationException("LocalServer not started.");

    public static void EnsureStarted()
    {
        lock (_gate)
        {
            if (_server is not null) return;
            _server = new AmqpServer();
            _server.Start();
        }
    }
}
