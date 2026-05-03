using Amqp.Listener;

sealed class QueueMessageSink(InMemoryQueue queue) : IMessageProcessor
{
    public int Credit => 100;

    public void Process(MessageContext messageContext)
    {
        Console.WriteLine($"Enqueue on {messageContext.Link.Name}");
        queue.Enqueue(messageContext.Message);
        messageContext.Complete();
    }
}
