using Amqp.Listener;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class TopicMessageSink(Topic topic, ILogger<TopicMessageSink>? logger = null) : IMessageProcessor
{
    private readonly ILogger<TopicMessageSink> _logger = logger ?? NullLogger<TopicMessageSink>.Instance;

    public int Credit => 100;

    public void Process(MessageContext messageContext)
    {
        try
        {
            foreach (var inner in BatchedMessage.Expand(messageContext.Message))
                topic.Enqueue(inner);
            _logger.LogTrace("Topic enqueue link={Link} topic={Topic}", messageContext.Link.Name, topic.Name);
            messageContext.Complete();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Topic enqueue failed link={Link} topic={Topic}", messageContext.Link.Name, topic.Name);
            throw;
        }
    }
}
