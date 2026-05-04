using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;

sealed class QueueMessageSource(IQueueEndpoint endpoint) : IMessageSource
{
    private static readonly Symbol DeadLetterReasonKey = "DeadLetterReason";
    private static readonly Symbol DeadLetterErrorDescriptionKey = "DeadLetterErrorDescription";

    private readonly CancellationTokenSource _cts = new();
    private int _callbackRegistered;

    public async Task<ReceiveContext> GetMessageAsync(ListenerLink link)
    {
        if (Interlocked.Exchange(ref _callbackRegistered, 1) == 0)
            link.AddClosedCallback((_, _) => _cts.Cancel());
        var delivery = await endpoint.DequeueAsync(_cts.Token);
        Console.WriteLine($"Dispatch to {link.Name} (delivery={delivery.Id} count={delivery.DeliveryCount})");
        return new ReceiveContext(link, delivery.Message) { UserToken = delivery };
    }

    public void DisposeMessage(ReceiveContext receiveContext, DispositionContext dispositionContext)
    {
        var delivery = (Delivery)receiveContext.UserToken;
        switch (dispositionContext.DeliveryState)
        {
            case Accepted:
                endpoint.Complete(delivery.Id);
                break;
            case Released:
                endpoint.Abandon(delivery.Id);
                break;
            case Modified modified when modified.UndeliverableHere:
                endpoint.Reject(delivery.Id, DeadLetterInfo.DeadLetteredByReceiver);
                break;
            case Modified:
                endpoint.Abandon(delivery.Id);
                break;
            case Rejected rejected:
                endpoint.Reject(delivery.Id, ReadDeadLetterInfo(rejected));
                break;
        }
        dispositionContext.Complete();
    }

    private static DeadLetterInfo ReadDeadLetterInfo(Rejected rejected)
    {
        var info = rejected.Error?.Info;
        if (info is null) return DeadLetterInfo.DeadLetteredByReceiver;
        var reason = info[DeadLetterReasonKey] as string ?? DeadLetterInfo.DeadLetteredByReceiver.Reason;
        var description = info[DeadLetterErrorDescriptionKey] as string ?? "";
        return new DeadLetterInfo(reason, description);
    }
}
