using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class QueueMessageSource(IQueueEndpoint endpoint, ILogger<QueueMessageSource>? logger = null) : IMessageSource
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
            switch (dispositionContext.DeliveryState)
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
                    _logger.LogTrace("Modified-undeliverable delivery={DeliveryId} seq={SequenceNumber}", delivery.Id, delivery.SequenceNumber);
                    endpoint.Reject(delivery.Id, DeadLetterInfo.DeadLetteredByReceiver);
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
        catch (Exception ex)
        {
            _logger.LogError(ex, "Disposition failed delivery={DeliveryId} seq={SequenceNumber} state={State}",
                delivery.Id, delivery.SequenceNumber, dispositionContext.DeliveryState?.GetType().Name);
            throw;
        }
        dispositionContext.Complete();
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
