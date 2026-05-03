using Amqp.Framing;
using Amqp.Listener;

sealed class QueueMessageSource(InMemoryQueue queue) : IMessageSource
{
    public async Task<ReceiveContext> GetMessageAsync(ListenerLink link)
    {
        var message = await queue.DequeueAsync();
        Console.WriteLine($"Dispatch to {link.Name}");
        return new ReceiveContext(link, message);
    }

    public void DisposeMessage(ReceiveContext receiveContext, DispositionContext dispositionContext)
    {
        switch (dispositionContext.DeliveryState)
        {
            case Released:
                queue.Enqueue(receiveContext.Message);
                break;
            case Modified modified when modified.UndeliverableHere:
                queue.DeadLetter(receiveContext.Message);
                break;
            case Modified:
                queue.Enqueue(receiveContext.Message);
                break;
            case Rejected:
                queue.DeadLetter(receiveContext.Message);
                break;
        }
        dispositionContext.Complete();
    }
}
