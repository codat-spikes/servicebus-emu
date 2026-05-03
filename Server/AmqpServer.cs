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
        _host.Listeners[0].SASL.EnableMechanism((Symbol)"MSSBCBS", SaslProfile.Anonymous);
        _host.Open();
        _host.RegisterRequestProcessor("$cbs", new CbsRequestProcessor());
        _host.RegisterLinkProcessor(new QueueLinkProcessor());
    }
}
