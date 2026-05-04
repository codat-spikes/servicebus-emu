using Amqp;
using Amqp.Framing;
using Amqp.Listener;

sealed class QueueMessageSource(InMemoryQueue queue) : IMessageSource
{
    public async Task<ReceiveContext> GetMessageAsync(ListenerLink link)
    {
        using var cts = new CancellationTokenSource();
        link.AddClosedCallback((_, _) => cts.Cancel());
        var delivery = await queue.DequeueAsync(cts.Token);
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

    private static (string? Reason, string? Description) ReadDeadLetterInfo(Rejected rejected)
    {
        var info = rejected.Error?.Info;
        if (info is null) return (null, null);
        var reason = info["DeadLetterReason"] as string;
        var description = info["DeadLetterErrorDescription"] as string;
        return (reason, description);
    }
}
