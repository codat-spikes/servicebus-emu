using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class InMemoryQueue : IQueueEndpoint
{
    private static readonly Symbol ScheduledEnqueueTimeAnnotation = "x-opt-scheduled-enqueue-time";
    // Service Bus encodes a message's session-id on the AMQP Properties.GroupId field
    // (matches the standard AMQP message-grouping convention) — *not* on the
    // x-opt-session-id annotation. The annotation only appears on receive paths in some
    // legacy contexts; the SDK reads/writes SessionId via GroupId.
    public static string? ReadSessionId(Message message) => message.Properties?.GroupId;

    public QueueOptions Options { get; }
    public MessageBuffer Primary { get; }
    public IQueueEndpoint DeadLetter => _deadLetterReceiver;
    public SessionStore Sessions { get; }

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
        Sessions = new SessionStore(options.LockDuration, Primary, OnPrimaryLockExpired, loggerFactory);
    }

    public long Schedule(Message message)
    {
        var enqueueAt = message.MessageAnnotations?.Map[ScheduledEnqueueTimeAnnotation] is DateTime t
            ? t
            : DateTime.UtcNow;
        return _scheduled.Schedule(message, enqueueAt);
    }

    public bool CancelScheduled(long sequenceNumber) => _scheduled.Cancel(sequenceNumber);

    public void Enqueue(Message message)
    {
        var sessionId = ReadSessionId(message);
        if (!string.IsNullOrEmpty(sessionId))
            Sessions.Enqueue(sessionId, message);
        else
            Primary.Enqueue(message);
    }

    public Task<Delivery> DequeueAsync(CancellationToken cancellation) => Primary.DequeueAsync(cancellation);

    public IReadOnlyList<Message> Peek(long fromSequenceNumber, int maxCount) =>
        Primary.Peek(fromSequenceNumber, maxCount);

    public void Complete(long deliveryId)
    {
        if (Primary.TryRelease(deliveryId, out var delivery)) Primary.Drop(delivery.SequenceNumber);
    }

    public void Abandon(long deliveryId)
    {
        if (Primary.TryRelease(deliveryId, out var delivery)) Reroute(Primary, delivery);
    }

    public void Reject(long deliveryId, DeadLetterInfo info)
    {
        if (Primary.TryRelease(deliveryId, out var delivery)) MoveToDeadLetter(Primary, delivery, info);
    }

    public bool TryRenewLock(Guid lockToken, out DateTime expiresAt)
    {
        if (Primary.TryRenewLock(lockToken, out expiresAt)) return true;
        return Sessions.TryRenewMessageLock(lockToken, out expiresAt);
    }

    private void OnPrimaryLockExpired(Delivery delivery)
    {
        // Lock-expired delivery comes from either the primary buffer or a session sub-buffer;
        // both share the reroute/dead-letter path on the owning buffer.
        var sessionId = ReadSessionId(delivery.Message);
        var buffer = !string.IsNullOrEmpty(sessionId) ? Sessions.GetOrCreate(sessionId).Buffer : Primary;
        Reroute(buffer, delivery);
    }

    private void OnDeadLetterLockExpired(Delivery delivery) => _deadLetter.Requeue(delivery);

    internal void RerouteSessionDelivery(Session session, Delivery delivery) => Reroute(session.Buffer, delivery);

    internal void DeadLetterFromSession(Session session, Delivery delivery, DeadLetterInfo info) => MoveToDeadLetter(session.Buffer, delivery, info);

    private void Reroute(MessageBuffer source, Delivery delivery)
    {
        if (delivery.DeliveryCount >= Options.MaxDeliveryCount)
            MoveToDeadLetter(source, delivery, DeadLetterInfo.MaxDeliveryCountExceeded);
        else
            source.Requeue(delivery);
    }

    private void MoveToDeadLetter(MessageBuffer source, Delivery delivery, DeadLetterInfo info)
    {
        source.Drop(delivery.SequenceNumber);
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

// Wraps a Session for receivers that attached with a session filter. Only this endpoint
// can dequeue from the session; the parent queue's primary path stays untouched. The
// session lock is released when the receiver detaches (see QueueLinkProcessor).
sealed class SessionEndpoint(InMemoryQueue queue, Session session) : IQueueEndpoint
{
    public Session Session => session;

    public void Enqueue(Message message) => queue.Enqueue(message);
    public Task<Delivery> DequeueAsync(CancellationToken cancellation) => session.Buffer.DequeueAsync(cancellation);
    public IReadOnlyList<Message> Peek(long fromSequenceNumber, int maxCount) =>
        session.Buffer.Peek(fromSequenceNumber, maxCount);

    public void Complete(long deliveryId)
    {
        if (session.Buffer.TryRelease(deliveryId, out var delivery))
            session.Buffer.Drop(delivery.SequenceNumber);
    }

    public void Abandon(long deliveryId)
    {
        if (session.Buffer.TryRelease(deliveryId, out var delivery))
            queue.RerouteSessionDelivery(session, delivery);
    }

    public void Reject(long deliveryId, DeadLetterInfo info)
    {
        if (session.Buffer.TryRelease(deliveryId, out var delivery))
            queue.DeadLetterFromSession(session, delivery, info);
    }

    public bool TryRenewLock(Guid lockToken, out DateTime expiresAt) =>
        session.Buffer.TryRenewLock(lockToken, out expiresAt);
}
