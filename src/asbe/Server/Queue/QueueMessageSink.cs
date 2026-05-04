using Amqp.Listener;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class QueueMessageSink(IQueueEndpoint endpoint, ILogger<QueueMessageSink>? logger = null) : IMessageProcessor
{
    private readonly ILogger<QueueMessageSink> _logger = logger ?? NullLogger<QueueMessageSink>.Instance;

    public int Credit => 100;

    public void Process(MessageContext messageContext)
    {
        try
        {
            foreach (var inner in BatchedMessage.Expand(messageContext.Message))
                endpoint.Enqueue(inner);
            _logger.LogTrace("Enqueue link={Link}", messageContext.Link.Name);
            messageContext.Complete();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Enqueue failed link={Link}", messageContext.Link.Name);
            throw;
        }
    }
}
