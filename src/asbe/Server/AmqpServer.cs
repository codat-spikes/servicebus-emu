using Amqp.Listener;
using Amqp.Sasl;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class AmqpServer
{
    public const string LocalConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=dev;SharedAccessKey=dev;UseDevelopmentEmulator=true;";
    private const string ListenerAddress = "amqp://127.0.0.1:5672";

    private readonly QueueStore _queues;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<AmqpServer> _logger;
    private readonly ContainerHost _host = new([ListenerAddress]);

    public AmqpServer() : this(new Dictionary<string, QueueOptions>(), null) { }

    public AmqpServer(ILoggerFactory? loggerFactory) : this(new Dictionary<string, QueueOptions>(), loggerFactory) { }

    public AmqpServer(IReadOnlyDictionary<string, QueueOptions> queues, ILoggerFactory? loggerFactory = null)
    {
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<AmqpServer>();
        _queues = new QueueStore(queues, RegisterManagement, _loggerFactory);
    }

    public void CreateQueue(string name, QueueOptions options) => _queues.CreateQueue(name, options);

    public bool DeleteQueue(string name)
    {
        // We deliberately don't call _host.UnregisterRequestProcessor here. AmqpNetLite's
        // ContainerHost.RequestProcessor.Dispose takes its requestLinks lock and then
        // synchronously closes each link, while the connection-close handler closes
        // links first and only then fires OnLinkClosed which itself tries to take
        // requestLinks. If a client disconnects around the same moment we delete the
        // queue, the two threads deadlock on opposite lock orderings. Leaving the
        // management endpoint registered is harmless (queue names are unique) and the
        // memory cost is negligible for the lifetime of a server instance.
        return _queues.DeleteQueue(name);
    }

    public void Start()
    {
        var listener = _host.Listeners[0];
        listener.SASL.EnableMechanism((Symbol)"MSSBCBS", SaslProfile.Anonymous);
        listener.HandlerFactory = _ => new LockTokenHandler();

        _host.Open();
        _host.RegisterRequestProcessor("$cbs", new CbsRequestProcessor(_loggerFactory.CreateLogger<CbsRequestProcessor>()));
        _host.RegisterLinkProcessor(new QueueLinkProcessor(_queues, _loggerFactory));
        _logger.LogInformation("AMQP server listening on {Address}", ListenerAddress);
    }

    private void RegisterManagement(string name, InMemoryQueue queue) =>
        _host.RegisterRequestProcessor(_queues.ManagementAddressFor(name), new ManagementRequestProcessor(queue, _loggerFactory.CreateLogger<ManagementRequestProcessor>()));
}
