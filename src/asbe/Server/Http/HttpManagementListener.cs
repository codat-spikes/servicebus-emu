using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

// Hosts a Kestrel HTTP listener that exposes the Service Bus management Atom-XML API.
// Mirrors the surface of ServiceBusAdministrationClient — read-only in this phase.
//
// The listener is a separate process surface from AmqpServer and runs on its own port
// (default 5300). It does not authenticate (matches AmqpServer's "accept any token"
// posture) and does not currently terminate TLS — see docs in HTTP_PLANE.md for the
// SDK construction pattern that lets ServiceBusAdministrationClient reach a plain-HTTP
// endpoint via a custom HttpPipelineTransport.
sealed class HttpManagementListener : IAsyncDisposable
{
    public const int DefaultPort = 5300;

    private readonly AmqpServer _broker;
    private readonly int _port;
    private readonly string _host;
    private readonly ILogger<HttpManagementListener> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private WebApplication? _app;
    private bool _started;
    private bool _disposed;

    public HttpManagementListener(AmqpServer broker, int port = DefaultPort, string host = "127.0.0.1", ILoggerFactory? loggerFactory = null)
    {
        _broker = broker;
        _port = port;
        _host = host;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<HttpManagementListener>();
    }

    public string ListenAddress => $"http://{_host}:{_port}";
    public string BaseUri => ListenAddress + "/";

    public async Task StartAsync(CancellationToken ct = default)
    {
        if (_started) throw new InvalidOperationException("HttpManagementListener already started.");
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(_loggerFactory);
        builder.Logging.AddProvider(new ForwardingLoggerProvider(_loggerFactory));
        builder.WebHost.UseKestrel(o => o.Listen(System.Net.IPAddress.Parse(_host), _port));

        var app = builder.Build();
        AdminRouter.Map(app, _broker, BaseUri, _loggerFactory.CreateLogger<HttpManagementListener>());
        await app.StartAsync(ct).ConfigureAwait(false);
        _app = app;
        _started = true;
        _logger.LogInformation("HTTP management listening on {Address}", ListenAddress);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        if (_app is not null)
        {
            try { await _app.StopAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false); }
            catch (Exception ex) { _logger.LogWarning(ex, "Kestrel stop threw."); }
            await _app.DisposeAsync().ConfigureAwait(false);
        }
    }

    // Bridge: the WebApplication builds its own logger pipeline; we want our existing
    // ILoggerFactory instance to receive Kestrel/aspnet logs too instead of running a
    // parallel logging pipeline. Trivial provider that delegates to the user's factory.
    sealed class ForwardingLoggerProvider(ILoggerFactory inner) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => inner.CreateLogger(categoryName);
        public void Dispose() { }
    }
}
