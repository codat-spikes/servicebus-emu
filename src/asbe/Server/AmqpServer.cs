using Amqp.Listener;
using Amqp.Sasl;
using Amqp.Transactions;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class AmqpServer : IAsyncDisposable
{
    public const string LocalConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=dev;SharedAccessKey=dev;UseDevelopmentEmulator=true;";
    public const int DefaultPort = 5672;

    private readonly string _listenerAddress;
    private readonly QueueStore _queues;
    private readonly TxnManager _txnManager;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<AmqpServer> _logger;
    private readonly ContainerHost _host;
    private bool _started;
    private bool _disposed;

    public AmqpServer() : this(new Dictionary<string, QueueOptions>(), null, DefaultPort) { }

    public AmqpServer(ILoggerFactory? loggerFactory) : this(new Dictionary<string, QueueOptions>(), loggerFactory, DefaultPort) { }

    public AmqpServer(int port, ILoggerFactory? loggerFactory = null) : this(new Dictionary<string, QueueOptions>(), loggerFactory, port) { }

    public AmqpServer(IReadOnlyDictionary<string, QueueOptions> queues, ILoggerFactory? loggerFactory = null, int port = DefaultPort, string host = "127.0.0.1")
    {
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<AmqpServer>();
        _listenerAddress = $"amqp://{host}:{port}";
        _host = new ContainerHost([_listenerAddress]);
        _queues = new QueueStore(queues, RegisterManagement, _loggerFactory);
        _txnManager = new TxnManager(_loggerFactory);
    }

    public void CreateQueue(string name, QueueOptions options) => _queues.CreateQueue(name, options);

    public void CreateTopic(string name, TopicOptions options)
    {
        var topic = _queues.CreateTopic(name, options);
        _host.RegisterRequestProcessor(_queues.TopicManagementAddressFor(name),
            new TopicManagementRequestProcessor(topic, _loggerFactory.CreateLogger<TopicManagementRequestProcessor>()));
        foreach (var (subName, subQueue) in topic.Subscriptions)
        {
            var address = _queues.SubscriptionManagementAddressFor(name, subName);
            _host.RegisterRequestProcessor(address, new ManagementRequestProcessor(subQueue, topic.Rules[subName], _loggerFactory.CreateLogger<ManagementRequestProcessor>()));
        }
    }

    public bool DeleteTopic(string name) => _queues.DeleteTopic(name);

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

        // Coordinator-targeted attaches don't have a `Target.Address`, and ContainerHost's
        // default attach path casts attach.Target to `Target` unconditionally before
        // delegating to the link processor. Pre-resolve coordinator attaches to a
        // sentinel address so the cast is skipped; no processor is registered there,
        // so the flow falls through to QueueLinkProcessor where we recognise the
        // Coordinator target.
        _host.AddressResolver = (_, attach) =>
            !attach.Role && attach.Target is Coordinator ? "$coordinator" : null!;

        _host.Open();
        _host.RegisterRequestProcessor("$cbs", new CbsRequestProcessor(_loggerFactory.CreateLogger<CbsRequestProcessor>()));
        _host.RegisterLinkProcessor(new QueueLinkProcessor(_queues, _txnManager, _loggerFactory));
        _started = true;
        _logger.LogInformation("AMQP server listening on {Address}", _listenerAddress);
    }

    // The caller must dispose every connected ServiceBusClient first. ContainerHost
    // close runs synchronously, but if a client disconnect races with our teardown the
    // listener-side OnLinkClosed handler can deadlock against RequestProcessor.Dispose
    // — see docs/DELETE_QUEUE_DEADLOCK.md.
    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;
        if (_started)
        {
            try { _host.Close(); }
            catch (Exception ex) { _logger.LogWarning(ex, "ContainerHost close threw."); }
        }
        _queues.Dispose();
        return ValueTask.CompletedTask;
    }

    private void RegisterManagement(string name, InMemoryQueue queue)
    {
        _host.RegisterRequestProcessor(_queues.QueueManagementAddressFor(name), new ManagementRequestProcessor(queue, _loggerFactory.CreateLogger<ManagementRequestProcessor>()));
        _host.RegisterRequestProcessor(_queues.DeadLetterManagementAddressFor(name), new ManagementRequestProcessor(queue.DeadLetter, _loggerFactory.CreateLogger<ManagementRequestProcessor>()));
    }
}
