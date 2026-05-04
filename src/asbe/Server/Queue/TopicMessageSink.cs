using Amqp.Framing;
using Amqp.Listener;
using Amqp.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class TopicMessageSink(Topic topic, TxnManager txnManager, ILogger<TopicMessageSink>? logger = null) : IMessageProcessor
{
    private readonly ILogger<TopicMessageSink> _logger = logger ?? NullLogger<TopicMessageSink>.Instance;

    public int Credit => 100;

    public void Process(MessageContext messageContext)
    {
        try
        {
            var expanded = BatchedMessage.Expand(messageContext.Message).ToArray();
            if (messageContext.DeliveryState is TransactionalState txnState)
            {
                var txn = txnManager.GetTransaction(txnState.TxnId);
                txn.AddOperation(fail =>
                {
                    if (fail) return;
                    foreach (var inner in expanded) topic.Enqueue(inner);
                });
                txnState.Outcome = new Accepted();
                messageContext.Link.DisposeMessage(messageContext.Message, txnState, true);
                _logger.LogTrace("Topic enqueue staged link={Link} topic={Topic} txn={Txn}", messageContext.Link.Name, topic.Name, BitConverter.ToInt32(txnState.TxnId, 0));
                return;
            }

            foreach (var inner in expanded) topic.Enqueue(inner);
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
