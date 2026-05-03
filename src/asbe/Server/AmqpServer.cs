using Amqp;
using Amqp.Handler;
using Amqp.Listener;
using Amqp.Sasl;
using Amqp.Types;

sealed class AmqpServer
{
    private readonly ContainerHost _host;
    private const string _connString = "amqp://127.0.0.1:5672";

    public AmqpServer()
    {
        _host = new ContainerHost([_connString]);
    }

    public void Start()
    {
        var listener = _host.Listeners[0];
        listener.SASL.EnableMechanism((Symbol)"MSSBCBS", SaslProfile.Anonymous);
        listener.HandlerFactory = _ => new LockTokenHandler();
        _host.Open();
        _host.RegisterRequestProcessor("$cbs", new CbsRequestProcessor());
        _host.RegisterLinkProcessor(new QueueLinkProcessor());
    }
}

sealed class LockTokenHandler : IHandler
{
    public bool CanHandle(EventId id) => id == EventId.SendDelivery;

    public void Handle(Event @event)
    {
        if (@event.Context is IDelivery delivery && delivery.Tag is null)
        {
            delivery.Tag = Guid.NewGuid().ToByteArray();
        }
    }
}
