using Amqp.Handler;
using Amqp.Listener;

// Two small adapters that paper over gaps between AMQPNetLite's listener APIs and what
// the Azure.Messaging.ServiceBus client expects on the wire. Both are server-side only.

// Service Bus encodes the lock token as the AMQP delivery tag, and the SDK rejects any
// tag that isn't 16 bytes (Guid). AMQPNetLite's default tag is a 4-byte counter assigned
// inside ListenerLink.SendMessage, so PeekLock receivers see Guid.Empty as their lock
// token and CompleteMessageAsync throws "not supported for peeked messages." The send
// path raises an IHandler event with the Delivery before the default tag is filled in,
// which is the only public seam where we can substitute a 16-byte GUID. We also need
// the wire tag to match the LockToken on our queue-side Delivery so $management requests
// (renew-lock, etc.) can find the same delivery the SDK is referring to.
sealed class LockTokenHandler : IHandler
{
    public bool CanHandle(EventId id) => id == EventId.SendDelivery;

    public void Handle(Event @event)
    {
        if (@event.Context is IDelivery delivery && delivery.Tag is null)
        {
            // SourceLinkEndpoint passes the ReceiveContext as the wire delivery's UserToken,
            // and QueueMessageSource stuffs our Delivery into ReceiveContext.UserToken.
            var token = ((delivery.UserToken as ReceiveContext)?.UserToken as Delivery)?.LockToken
                        ?? Guid.NewGuid();
            delivery.Tag = token.ToByteArray();
        }
    }
}

// SourceLinkEndpoint serves messages by awaiting IMessageSource.GetMessageAsync in a
// loop, so when the queue is empty there is always a pending GetMessageAsync parked on
// a TaskCompletionSource. The SDK sends flow{drain=true} on receiver close to settle
// remaining credit; SourceLinkEndpoint's built-in drain handling only runs after the
// next GetMessageAsync completes, which never happens for a parked waiter, so the SDK
// times out at 60s. We intercept drain at the link level and ack it immediately with
// CompleteDrain, leaving the TCS waiter to be satisfied (and harmlessly dropped) by a
// future Enqueue.
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
