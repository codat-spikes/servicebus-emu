using Amqp.Listener;
using Amqp.Sasl;
using Amqp.Types;

sealed class AmqpServer
{
    public const string LocalConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=dev;SharedAccessKey=dev;UseDevelopmentEmulator=true;";

    private readonly QueueStore _queues;
    private readonly ContainerHost _host = new(["amqp://127.0.0.1:5672"]);

    public AmqpServer() : this(new Dictionary<string, QueueOptions>()) { }

    public AmqpServer(IReadOnlyDictionary<string, QueueOptions> queues)
    {
        _queues = new QueueStore(queues);
    }

    public void CreateQueue(string name, QueueOptions options) => _queues.CreateQueue(name, options);

    public bool DeleteQueue(string name) => _queues.DeleteQueue(name);

    public void Start()
    {
        var listener = _host.Listeners[0];
        listener.SASL.EnableMechanism((Symbol)"MSSBCBS", SaslProfile.Anonymous);
        listener.HandlerFactory = _ => new LockTokenHandler();
        _host.Open();
        _host.RegisterRequestProcessor("$cbs", new CbsRequestProcessor());
        _host.RegisterLinkProcessor(new QueueLinkProcessor(_queues));
    }
}
