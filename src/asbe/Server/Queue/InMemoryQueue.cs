using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class InMemoryQueue : IQueueEndpoint, IDisposable
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
    private readonly ExpiryStore _expiry;
    // Per-sequence reverse index: where the message lives so the expiry callback can
    // find the right MessageBuffer (primary or a session sub-buffer) to evict from.
    private readonly System.Collections.Concurrent.ConcurrentDictionary<long, MessageBuffer> _ownerBySeq = new();
    private readonly ILogger<InMemoryQueue> _logger;

    public InMemoryQueue(QueueOptions options, ILoggerFactory? loggerFactory = null)
    {
        loggerFactory ??= NullLoggerFactory.Instance;
        _logger = loggerFactory.CreateLogger<InMemoryQueue>();
        Options = options;
        Primary = new MessageBuffer(options.LockDuration, OnPrimaryLockExpired, loggerFactory.CreateLogger<MessageBuffer>());
        _deadLetter = new MessageBuffer(options.LockDuration, OnDeadLetterLockExpired, loggerFactory.CreateLogger<MessageBuffer>());
        _deadLetterReceiver = new DeadLetterReceiver(_deadLetter);
        _scheduled = new ScheduledStore(Primary.AssignSequenceNumber, EnqueueFromScheduled, loggerFactory.CreateLogger<ScheduledStore>());
        _expiry = new ExpiryStore(OnExpired, loggerFactory.CreateLogger<ExpiryStore>());
        Sessions = new SessionStore(options.LockDuration, Primary, OnPrimaryLockExpired, loggerFactory);
    }

    private void EnqueueFromScheduled(Message message) => Enqueue(message);

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
        {
            Sessions.Enqueue(sessionId, message);
            ApplyExpiry(message, Sessions.GetOrCreate(sessionId).Buffer);
        }
        else
        {
            Primary.Enqueue(message);
            ApplyExpiry(message, Primary);
        }
    }

    // Stamps Properties.AbsoluteExpiryTime from the effective TTL (min of message
    // Header.Ttl and queue default), and registers the expiry. No-op if neither side
    // sets a TTL — Service Bus treats that as "never expires" for our purposes.
    private void ApplyExpiry(Message message, MessageBuffer owner)
    {
        var seq = ReadSeq(message);
        if (seq == 0) return;

        var messageTtl = ReadHeaderTtl(message);
        var defaultTtl = Options.DefaultMessageTimeToLive;
        TimeSpan? effective = (messageTtl, defaultTtl) switch
        {
            ({ } a, { } b) => a < b ? a : b,
            ({ } a, null) => a,
            (null, { } b) => b,
            _ => null,
        };
        if (effective is null || effective.Value <= TimeSpan.Zero) return;

        var expiresAt = DateTime.UtcNow + effective.Value;
        message.Properties ??= new Properties();
        message.Properties.AbsoluteExpiryTime = expiresAt;
        _ownerBySeq[seq] = owner;
        _expiry.Register(seq, expiresAt);
    }

    private static long ReadSeq(Message message) =>
        message.MessageAnnotations?.Map[MessageBuffer.SequenceNumberAnnotation] is long s ? s : 0;

    private static TimeSpan? ReadHeaderTtl(Message message)
    {
        if (message.Header is null) return null;
        var ttl = message.Header.Ttl;
        if (ttl == 0 || ttl == uint.MaxValue) return null;
        return TimeSpan.FromMilliseconds(ttl);
    }

    private static bool IsExpired(Message message)
    {
        if (message.Properties is null) return false;
        var t = message.Properties.AbsoluteExpiryTime;
        return t != default && t <= DateTime.UtcNow;
    }

    private void OnExpired(long sequenceNumber)
    {
        if (!_ownerBySeq.TryGetValue(sequenceNumber, out var owner)) return;
        if (!owner.TryEvictReady(sequenceNumber, out var message)) return; // peek-locked; defer to Reroute
        _ownerBySeq.TryRemove(sequenceNumber, out _);
        ExpireMessage(message, sequenceNumber, deliveryCount: 0);
    }

    private void ExpireMessage(Message message, long sequenceNumber, int deliveryCount)
    {
        if (!Options.DeadLetteringOnMessageExpiration)
        {
            _logger.LogInformation("TTL-expired (dropped) seq={SequenceNumber}", sequenceNumber);
            return;
        }
        message.ApplicationProperties ??= new ApplicationProperties();
        message.ApplicationProperties.Map["DeadLetterReason"] = "TTLExpiredException";
        message.Header = null;
        message.MessageAnnotations?.Map.Remove(MessageBuffer.SequenceNumberAnnotation);
        _deadLetter.Enqueue(message);
        _logger.LogInformation("TTL-expired seq={SequenceNumber} deliveryCount={DeliveryCount}", sequenceNumber, deliveryCount);
    }

    public Task<Delivery> DequeueAsync(CancellationToken cancellation) => Primary.DequeueAsync(cancellation);

    public IReadOnlyList<Message> Peek(long fromSequenceNumber, int maxCount) =>
        Primary.Peek(fromSequenceNumber, maxCount);

    public void Complete(long deliveryId)
    {
        if (Primary.TryRelease(deliveryId, out var delivery))
        {
            Primary.Drop(delivery.SequenceNumber);
            CancelExpiry(delivery.SequenceNumber);
        }
    }

    private void CancelExpiry(long sequenceNumber)
    {
        _expiry.Cancel(sequenceNumber);
        _ownerBySeq.TryRemove(sequenceNumber, out _);
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
        if (IsExpired(delivery.Message))
        {
            source.Drop(delivery.SequenceNumber);
            _ownerBySeq.TryRemove(delivery.SequenceNumber, out _);
            ExpireMessage(delivery.Message, delivery.SequenceNumber, delivery.DeliveryCount);
            return;
        }
        if (delivery.DeliveryCount >= Options.MaxDeliveryCount)
            MoveToDeadLetter(source, delivery, DeadLetterInfo.MaxDeliveryCountExceeded);
        else
            source.Requeue(delivery);
    }

    public void Dispose()
    {
        Sessions.Dispose();
        _scheduled.Dispose();
        _expiry.Dispose();
        _deadLetter.Dispose();
        Primary.Dispose();
    }

    internal void OnSessionDeliveryCompleted(long sequenceNumber) => CancelExpiry(sequenceNumber);

    private void MoveToDeadLetter(MessageBuffer source, Delivery delivery, DeadLetterInfo info)
    {
        source.Drop(delivery.SequenceNumber);
        CancelExpiry(delivery.SequenceNumber);
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
        {
            session.Buffer.Drop(delivery.SequenceNumber);
            queue.OnSessionDeliveryCompleted(delivery.SequenceNumber);
        }
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
