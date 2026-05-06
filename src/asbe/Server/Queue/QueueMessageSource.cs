using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Transactions;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class QueueMessageSource(IQueueEndpoint endpoint, TxnManager txnManager, ILogger<QueueMessageSource>? logger = null) : IMessageSource
{
    private static readonly Symbol DeadLetterReasonKey = "DeadLetterReason";
    private static readonly Symbol DeadLetterErrorDescriptionKey = "DeadLetterErrorDescription";

    private readonly ILogger<QueueMessageSource> _logger = logger ?? NullLogger<QueueMessageSource>.Instance;
    private readonly CancellationTokenSource _cts = new();
    private int _callbackRegistered;

    public async Task<ReceiveContext> GetMessageAsync(ListenerLink link)
    {
        if (Interlocked.Exchange(ref _callbackRegistered, 1) == 0)
            link.AddClosedCallback((_, _) => _cts.Cancel());
        var delivery = await endpoint.DequeueAsync(_cts.Token);
        _logger.LogTrace("Dispatch link={Link} delivery={DeliveryId} seq={SequenceNumber} count={DeliveryCount}",
            link.Name, delivery.Id, delivery.SequenceNumber, delivery.DeliveryCount);
        return new ReceiveContext(link, delivery.Message) { UserToken = delivery };
    }

    public void DisposeMessage(ReceiveContext receiveContext, DispositionContext dispositionContext)
    {
        var delivery = (Delivery)receiveContext.UserToken;
        try
        {
            // Transactional disposition: stage the outcome against the txn instead of
            // applying it now. The lock stays held; on commit we apply the wrapped
            // outcome, on rollback we drop the closure (lock stays held until expiry,
            // matching Service Bus's at-least-once contract for in-flight txns).
            if (dispositionContext.DeliveryState is TransactionalState txnState)
            {
                var inner = txnState.Outcome;
                var txn = txnManager.GetTransaction(txnState.TxnId);
                txn.AddOperation(fail =>
                {
                    if (fail) return;
                    ApplyOutcome(delivery, inner);
                });
                _logger.LogTrace("Dispose staged delivery={DeliveryId} seq={SequenceNumber} txn={Txn}",
                    delivery.Id, delivery.SequenceNumber, BitConverter.ToInt32(txnState.TxnId, 0));
                dispositionContext.Complete();
                return;
            }

            ApplyOutcome(delivery, dispositionContext.DeliveryState);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Disposition failed delivery={DeliveryId} seq={SequenceNumber} state={State}",
                delivery.Id, delivery.SequenceNumber, dispositionContext.DeliveryState?.GetType().Name);
            throw;
        }
        dispositionContext.Complete();
    }

    private void ApplyOutcome(Delivery delivery, Amqp.Framing.DeliveryState? state)
    {
        switch (state)
        {
            case Accepted:
                _logger.LogTrace("Accept delivery={DeliveryId} seq={SequenceNumber}", delivery.Id, delivery.SequenceNumber);
                endpoint.Complete(delivery.Id);
                break;
            case Released:
                // AMQP `released` is implicit (e.g. AMQPNetLite emits it for unsettled
                // messages when the link closes). The Service Bus SDK uses `modified`
                // for AbandonMessageAsync, never `released` — so the broker holds the
                // lock until expiry rather than requeuing immediately. Match that.
                _logger.LogTrace("Released (lock held until expiry) delivery={DeliveryId} seq={SequenceNumber}", delivery.Id, delivery.SequenceNumber);
                break;
            case Modified modified when modified.UndeliverableHere:
                // The Service Bus SDK uses Modified{UndeliverableHere=true} for
                // DeferAsync. DeadLetter goes through Rejected with the DLQ error condition.
                _logger.LogTrace("Defer delivery={DeliveryId} seq={SequenceNumber}", delivery.Id, delivery.SequenceNumber);
                endpoint.Defer(delivery.Id);
                break;
            case Modified:
                _logger.LogTrace("Modified (abandon) delivery={DeliveryId} seq={SequenceNumber}", delivery.Id, delivery.SequenceNumber);
                endpoint.Abandon(delivery.Id);
                break;
            case Rejected rejected:
                _logger.LogTrace("Reject delivery={DeliveryId} seq={SequenceNumber}", delivery.Id, delivery.SequenceNumber);
                endpoint.Reject(delivery.Id, ReadDeadLetterInfo(rejected));
                break;
        }
    }

    private static DeadLetterInfo ReadDeadLetterInfo(Rejected rejected)
    {
        var info = rejected.Error?.Info;
        if (info is null) return DeadLetterInfo.DeadLetteredByReceiver;
        var reason = info[DeadLetterReasonKey] as string ?? DeadLetterInfo.DeadLetteredByReceiver.Reason;
        var description = info[DeadLetterErrorDescriptionKey] as string ?? "";
        return new DeadLetterInfo(reason, description);
    }
}
