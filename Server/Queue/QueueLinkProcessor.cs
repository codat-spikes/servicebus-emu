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
            attachContext.Complete(new SourceLinkEndpoint(new QueueMessageSource(queue), attachContext.Link), 0);
        }
    }
}
