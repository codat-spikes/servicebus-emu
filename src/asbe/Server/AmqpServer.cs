using Amqp.Listener;
using Amqp.Sasl;
using Amqp.Types;

sealed class AmqpServer
{
    public const string LocalConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=dev;SharedAccessKey=dev;UseDevelopmentEmulator=true;";

    private readonly QueueStore _queues = new();
    private readonly ContainerHost _host = new(["amqp://127.0.0.1:5672"]);

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
