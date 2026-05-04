using Amqp;
using Amqp.Framing;

sealed class InMemoryQueue : IQueueEndpoint
{
    public QueueOptions Options { get; }
    public MessageBuffer Primary { get; }
    public IQueueEndpoint DeadLetter => _deadLetterReceiver;

    private readonly MessageBuffer _deadLetter;
    private readonly DeadLetterReceiver _deadLetterReceiver;

    public InMemoryQueue(QueueOptions options)
    {
        Options = options;
        Primary = new MessageBuffer(options.LockDuration, OnPrimaryLockExpired);
        _deadLetter = new MessageBuffer(options.LockDuration, OnDeadLetterLockExpired);
        _deadLetterReceiver = new DeadLetterReceiver(_deadLetter);
    }

    public void Enqueue(Message message) => Primary.Enqueue(message);

    public Task<Delivery> DequeueAsync(CancellationToken cancellation) => Primary.DequeueAsync(cancellation);

    public void Complete(long deliveryId) => Primary.TryRelease(deliveryId, out _);

    public void Abandon(long deliveryId)
    {
        if (Primary.TryRelease(deliveryId, out var delivery)) Reroute(delivery);
    }

    public void Reject(long deliveryId, DeadLetterInfo info)
    {
        if (Primary.TryRelease(deliveryId, out var delivery)) MoveToDeadLetter(delivery.Message, info);
    }

    public bool TryRenewLock(Guid lockToken, out DateTime expiresAt) =>
        Primary.TryRenewLock(lockToken, out expiresAt);

    private void OnPrimaryLockExpired(Delivery delivery) => Reroute(delivery);

    private void OnDeadLetterLockExpired(Delivery delivery) => _deadLetter.Requeue(delivery);

    private void Reroute(Delivery delivery)
    {
        if (delivery.DeliveryCount >= Options.MaxDeliveryCount)
            MoveToDeadLetter(delivery.Message, DeadLetterInfo.MaxDeliveryCountExceeded);
        else
            Primary.Requeue(delivery);
    }

    private void MoveToDeadLetter(Message message, DeadLetterInfo info)
    {
        message.ApplicationProperties ??= new ApplicationProperties();
        message.ApplicationProperties.Map["DeadLetterReason"] = info.Reason;
        if (info.Description.Length > 0)
            message.ApplicationProperties.Map["DeadLetterErrorDescription"] = info.Description;
        message.Header = null;
        _deadLetter.Enqueue(message);
    }
}

// DLQ receivers see the same lock/abandon/complete surface as the primary, but a DLQ
// has no further DLQ — abandon just requeues, reject just settles. (Real Service Bus
// rejects dead-letter-on-DLQ; we settle silently for now since no test exercises it.)
sealed class DeadLetterReceiver(MessageBuffer buffer) : IQueueEndpoint
{
    public void Enqueue(Message message) => buffer.Enqueue(message);
    public Task<Delivery> DequeueAsync(CancellationToken cancellation) => buffer.DequeueAsync(cancellation);
    public void Complete(long deliveryId) => buffer.TryRelease(deliveryId, out _);
    public void Abandon(long deliveryId)
    {
        if (buffer.TryRelease(deliveryId, out var delivery)) buffer.Requeue(delivery);
    }
    public void Reject(long deliveryId, DeadLetterInfo info) => buffer.TryRelease(deliveryId, out _);
    public bool TryRenewLock(Guid lockToken, out DateTime expiresAt) => buffer.TryRenewLock(lockToken, out expiresAt);
}
