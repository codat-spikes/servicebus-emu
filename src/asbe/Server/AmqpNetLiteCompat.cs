using System.Reflection;
using Amqp;
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
// times out at 60s.
//
// AMQPNetLite's ListenerLink.CompleteDrain advances delivery-count and clears credit,
// but emits the drain-ack flow with drain=false (it zeroes the local field before the
// SendFlow call). The Service Bus SDK reads the drain flag strictly and ignores that
// ack, so ReceiveMessageAsync hangs until its internal 60s operation timeout. We work
// around it by letting CompleteDrain update link state, then reaching into the internal
// SendFlow to emit a spec-compliant drain=true flow on top. Reflection here is the
// pragmatic price for staying on the upstream NuGet package; the targeted members
// (ListenerLink.deliveryCount field, Link.SendFlow(uint,uint,bool)) have been stable
// for years and a missing-member break would surface immediately on first test run.
sealed class DrainAwareSourceLinkEndpoint(IMessageSource source, ListenerLink link) : SourceLinkEndpoint(source, link)
{
    private static readonly FieldInfo DeliveryCountField = typeof(ListenerLink)
        .GetField("deliveryCount", BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new InvalidOperationException("ListenerLink.deliveryCount field not found.");

    private static readonly FieldInfo SequenceNumberInnerField = DeliveryCountField.FieldType
        .GetField("sequenceNumber", BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new InvalidOperationException("SequenceNumber.sequenceNumber field not found.");

    private static readonly MethodInfo SendFlowMethod = typeof(Link)
        .GetMethod("SendFlow", BindingFlags.NonPublic | BindingFlags.Instance,
            binder: null, types: [typeof(uint), typeof(uint), typeof(bool)], modifiers: null)
        ?? throw new InvalidOperationException("Link.SendFlow(uint,uint,bool) not found.");

    public override void OnFlow(FlowContext flowContext)
    {
        if (flowContext.Link.IsDraining)
        {
            flowContext.Link.CompleteDrain();
            SendDrainTrueAck(flowContext.Link);
            return;
        }
        base.OnFlow(flowContext);
    }

    private static void SendDrainTrueAck(ListenerLink link)
    {
        var seq = DeliveryCountField.GetValue(link)!;
        var deliveryCount = (uint)(int)SequenceNumberInnerField.GetValue(seq)!;
        SendFlowMethod.Invoke(link, [deliveryCount, 0u, true]);
    }
}
