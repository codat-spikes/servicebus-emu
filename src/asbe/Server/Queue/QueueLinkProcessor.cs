using Amqp;
using Amqp.Framing;
using Amqp.Listener;

sealed class QueueLinkProcessor : ILinkProcessor
{
    private const uint MaxMessageSize = 256 * 1024;

    public void Process(AttachContext attachContext)
    {
        var attach = attachContext.Attach;
        var address = attach.Role
            ? (attach.Source as Source)?.Address
            : (attach.Target as Target)?.Address;

        Console.WriteLine($"Attach: link={attach.LinkName} role={(attach.Role ? "receiver" : "sender")} address={address}");

        if (string.IsNullOrEmpty(address))
        {
            attachContext.Complete(new Error(ErrorCode.InvalidField) { Description = "Address is required." });
            return;
        }

        attach.MaxMessageSize = MaxMessageSize;
        var queue = QueueStore.Get(address);

        if (!attach.Role)
        {
            attachContext.Complete(new TargetLinkEndpoint(new QueueMessageSink(queue), attachContext.Link), 100);
        }
        else
        {
            attachContext.Complete(new DrainAwareSourceLinkEndpoint(new QueueMessageSource(queue), attachContext.Link), 0);
        }
    }
}

sealed class DrainAwareSourceLinkEndpoint(IMessageSource source, ListenerLink link) : SourceLinkEndpoint(source, link)
{
    public override void OnFlow(FlowContext flowContext)
    {
        if (flowContext.Link.IsDraining)
        {
            flowContext.Link.CompleteDrain();
            return;
        }
        base.OnFlow(flowContext);
    }
}
