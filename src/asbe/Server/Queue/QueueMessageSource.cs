using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;

sealed class QueueMessageSource(InMemoryQueue queue) : IMessageSource
{
    private readonly CancellationTokenSource _cts = new();
    private int _callbackRegistered;

    public async Task<ReceiveContext> GetMessageAsync(ListenerLink link)
    {
        if (Interlocked.Exchange(ref _callbackRegistered, 1) == 0)
            link.AddClosedCallback((_, _) => _cts.Cancel());
        var delivery = await queue.DequeueAsync(_cts.Token);
        Console.WriteLine($"Dispatch to {link.Name} (delivery={delivery.Id} count={delivery.DeliveryCount})");
        return new ReceiveContext(link, delivery.Message) { UserToken = delivery };
    }

    public void DisposeMessage(ReceiveContext receiveContext, DispositionContext dispositionContext)
    {
        var delivery = (Delivery)receiveContext.UserToken;
        switch (dispositionContext.DeliveryState)
        {
            case Accepted:
                queue.Complete(delivery.Id);
                break;
            case Released:
                queue.Abandon(delivery.Id);
                break;
            case Modified modified when modified.UndeliverableHere:
                queue.DeadLetter(delivery.Id);
                break;
            case Modified:
                queue.Abandon(delivery.Id);
                break;
            case Rejected rejected:
                var (reason, description) = ReadDeadLetterInfo(rejected);
                queue.DeadLetter(delivery.Id, reason, description);
                break;
        }
        dispositionContext.Complete();
    }

    private static readonly Symbol DeadLetterReasonKey = "DeadLetterReason";
    private static readonly Symbol DeadLetterErrorDescriptionKey = "DeadLetterErrorDescription";

    private static (string? Reason, string? Description) ReadDeadLetterInfo(Rejected rejected)
    {
        var info = rejected.Error?.Info;
        if (info is null) return (null, null);
        return (info[DeadLetterReasonKey] as string, info[DeadLetterErrorDescriptionKey] as string);
    }
}
