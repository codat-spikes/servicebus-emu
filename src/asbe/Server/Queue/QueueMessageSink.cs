using Amqp.Listener;

sealed class QueueMessageSink(IQueueEndpoint endpoint) : IMessageProcessor
{
    public int Credit => 100;

    public void Process(MessageContext messageContext)
    {
        Console.WriteLine($"Enqueue on {messageContext.Link.Name}");
        endpoint.Enqueue(messageContext.Message);
        messageContext.Complete();
    }
}
