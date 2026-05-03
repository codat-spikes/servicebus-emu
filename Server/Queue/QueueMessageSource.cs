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
        dispositionContext.Complete();
    }
}
