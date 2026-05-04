using Amqp.Framing;
using Amqp.Listener;
using Amqp.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class QueueMessageSink(IQueueEndpoint endpoint, TxnManager txnManager, ILogger<QueueMessageSink>? logger = null) : IMessageProcessor
{
    private readonly ILogger<QueueMessageSink> _logger = logger ?? NullLogger<QueueMessageSink>.Instance;

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
                    foreach (var inner in expanded) endpoint.Enqueue(inner);
                });
                // Per AMQP txn spec: outcome on the transactional-state is the proposed
                // outcome the broker would apply if the txn commits. Real flush happens
                // on Discharge.
                txnState.Outcome = new Accepted();
                messageContext.Link.DisposeMessage(messageContext.Message, txnState, true);
                _logger.LogTrace("Enqueue staged link={Link} txn={Txn}", messageContext.Link.Name, BitConverter.ToInt32(txnState.TxnId, 0));
                return;
            }

            foreach (var inner in expanded) endpoint.Enqueue(inner);
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
