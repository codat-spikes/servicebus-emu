using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class InMemoryQueue : IQueueEndpoint
{
    private static readonly Symbol ScheduledEnqueueTimeAnnotation = "x-opt-scheduled-enqueue-time";

    public QueueOptions Options { get; }
    public MessageBuffer Primary { get; }
    public IQueueEndpoint DeadLetter => _deadLetterReceiver;

    private readonly MessageBuffer _deadLetter;
    private readonly DeadLetterReceiver _deadLetterReceiver;
    private readonly ScheduledStore _scheduled;
    private readonly ILogger<InMemoryQueue> _logger;

    public InMemoryQueue(QueueOptions options, ILoggerFactory? loggerFactory = null)
    {
        loggerFactory ??= NullLoggerFactory.Instance;
        _logger = loggerFactory.CreateLogger<InMemoryQueue>();
        Options = options;
        Primary = new MessageBuffer(options.LockDuration, OnPrimaryLockExpired, loggerFactory.CreateLogger<MessageBuffer>());
        _deadLetter = new MessageBuffer(options.LockDuration, OnDeadLetterLockExpired, loggerFactory.CreateLogger<MessageBuffer>());
        _deadLetterReceiver = new DeadLetterReceiver(_deadLetter);
        _scheduled = new ScheduledStore(Primary, loggerFactory.CreateLogger<ScheduledStore>());
    }

    public long Schedule(Message message)
    {
        var enqueueAt = message.MessageAnnotations?.Map[ScheduledEnqueueTimeAnnotation] is DateTime t
            ? t
            : DateTime.UtcNow;
        return _scheduled.Schedule(message, enqueueAt);
    }

    public bool CancelScheduled(long sequenceNumber) => _scheduled.Cancel(sequenceNumber);

    public void Enqueue(Message message) => Primary.Enqueue(message);

    public Task<Delivery> DequeueAsync(CancellationToken cancellation) => Primary.DequeueAsync(cancellation);

    public IReadOnlyList<Message> Peek(long fromSequenceNumber, int maxCount) =>
        Primary.Peek(fromSequenceNumber, maxCount);

    public void Complete(long deliveryId)
    {
        if (Primary.TryRelease(deliveryId, out var delivery)) Primary.Drop(delivery.SequenceNumber);
    }

    public void Abandon(long deliveryId)
    {
        if (Primary.TryRelease(deliveryId, out var delivery)) Reroute(delivery);
    }

    public void Reject(long deliveryId, DeadLetterInfo info)
    {
        if (Primary.TryRelease(deliveryId, out var delivery)) MoveToDeadLetter(delivery, info);
    }

    public bool TryRenewLock(Guid lockToken, out DateTime expiresAt) =>
        Primary.TryRenewLock(lockToken, out expiresAt);

    private void OnPrimaryLockExpired(Delivery delivery) => Reroute(delivery);

    private void OnDeadLetterLockExpired(Delivery delivery) => _deadLetter.Requeue(delivery);

    private void Reroute(Delivery delivery)
    {
        if (delivery.DeliveryCount >= Options.MaxDeliveryCount)
            MoveToDeadLetter(delivery, DeadLetterInfo.MaxDeliveryCountExceeded);
        else
            Primary.Requeue(delivery);
    }

    private void MoveToDeadLetter(Delivery delivery, DeadLetterInfo info)
    {
        Primary.Drop(delivery.SequenceNumber);
        var message = delivery.Message;
        message.ApplicationProperties ??= new ApplicationProperties();
        message.ApplicationProperties.Map["DeadLetterReason"] = info.Reason;
        if (info.Description.Length > 0)
            message.ApplicationProperties.Map["DeadLetterErrorDescription"] = info.Description;
        message.Header = null;
        // Force the DLQ buffer to assign its own sequence number.
        message.MessageAnnotations?.Map.Remove(MessageBuffer.SequenceNumberAnnotation);
        _deadLetter.Enqueue(message);
        _logger.LogInformation("Dead-lettered seq={SequenceNumber} reason={Reason} description={Description}",
            delivery.SequenceNumber, info.Reason, info.Description);
    }
}

// DLQ receivers see the same lock/abandon/complete surface as the primary, but a DLQ
// has no further DLQ — abandon just requeues, reject just settles. (Real Service Bus
// rejects dead-letter-on-DLQ; we settle silently for now since no test exercises it.)
sealed class DeadLetterReceiver(MessageBuffer buffer) : IQueueEndpoint
{
    public void Enqueue(Message message) => buffer.Enqueue(message);
    public Task<Delivery> DequeueAsync(CancellationToken cancellation) => buffer.DequeueAsync(cancellation);
    public IReadOnlyList<Message> Peek(long fromSequenceNumber, int maxCount) => buffer.Peek(fromSequenceNumber, maxCount);
    public void Complete(long deliveryId)
    {
        if (buffer.TryRelease(deliveryId, out var delivery)) buffer.Drop(delivery.SequenceNumber);
    }
    public void Abandon(long deliveryId)
    {
        if (buffer.TryRelease(deliveryId, out var delivery)) buffer.Requeue(delivery);
    }
    public void Reject(long deliveryId, DeadLetterInfo info)
    {
        if (buffer.TryRelease(deliveryId, out var delivery)) buffer.Drop(delivery.SequenceNumber);
    }
    public bool TryRenewLock(Guid lockToken, out DateTime expiresAt) => buffer.TryRenewLock(lockToken, out expiresAt);
}
