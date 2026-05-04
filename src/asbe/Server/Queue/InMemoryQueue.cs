using System.Collections.Concurrent;
using System.Threading.Channels;
using Amqp;
using Amqp.Framing;

sealed class InMemoryQueue
{
    public QueueOptions Options { get; }
    public InMemoryQueue? DeadLetterQueue { get; }

    private readonly Channel<Pending> _ready = Channel.CreateUnbounded<Pending>();
    private readonly ConcurrentDictionary<long, Delivery> _inFlight = new();
    private readonly ConcurrentDictionary<Guid, Delivery> _byLockToken = new();
    private long _nextDeliveryId;

    public InMemoryQueue(QueueOptions options) : this(options, hasDeadLetterQueue: true) { }

    private InMemoryQueue(QueueOptions options, bool hasDeadLetterQueue)
    {
        Options = options;
        DeadLetterQueue = hasDeadLetterQueue ? new InMemoryQueue(options, hasDeadLetterQueue: false) : null;
    }

    public void Enqueue(Message message) => _ready.Writer.TryWrite(new Pending(message, 0));

    public async Task<Delivery> DequeueAsync(CancellationToken cancellation = default)
    {
        var pending = await _ready.Reader.ReadAsync(cancellation);
        return StartDelivery(pending);
    }

    public void Complete(long deliveryId) => TrySettle(deliveryId, out _);

    public void Abandon(long deliveryId)
    {
        if (TrySettle(deliveryId, out var delivery)) Redeliver(delivery);
    }

    public void DeadLetter(long deliveryId, string? reason = null, string? description = null)
    {
        if (TrySettle(deliveryId, out var delivery))
            SendToDeadLetter(delivery.Message, reason ?? "DeadLetteredByReceiver", description);
    }

    public DateTime? RenewLock(Guid lockToken)
    {
        if (!_byLockToken.TryGetValue(lockToken, out var delivery)) return null;
        if (Volatile.Read(ref delivery.State) != (int)DeliveryState.Pending) return null;
        var newExpiry = DateTime.UtcNow + Options.LockDuration;
        delivery.LockedUntil = newExpiry;
        delivery.LockTimer?.Change(Options.LockDuration, Timeout.InfiniteTimeSpan);
        return newExpiry;
    }

    private Delivery StartDelivery(Pending pending)
    {
        var deliveryId = Interlocked.Increment(ref _nextDeliveryId);
        SetWireDeliveryCount(pending.Message, pending.PriorDeliveries);
        var delivery = new Delivery(deliveryId, pending.Message, pending.PriorDeliveries + 1, Guid.NewGuid())
        {
            LockedUntil = DateTime.UtcNow + Options.LockDuration,
        };
        _inFlight[deliveryId] = delivery;
        _byLockToken[delivery.LockToken] = delivery;
        delivery.LockTimer = new Timer(_ => OnLockExpired(deliveryId), null, Options.LockDuration, Timeout.InfiniteTimeSpan);
        return delivery;
    }

    private bool TrySettle(long deliveryId, out Delivery delivery)
    {
        if (!_inFlight.TryGetValue(deliveryId, out delivery!)) return false;
        if (Interlocked.CompareExchange(ref delivery.State, (int)DeliveryState.Settled, (int)DeliveryState.Pending) != (int)DeliveryState.Pending) return false;
        Forget(delivery);
        return true;
    }

    private void OnLockExpired(long deliveryId)
    {
        if (!_inFlight.TryGetValue(deliveryId, out var delivery)) return;
        if (Interlocked.CompareExchange(ref delivery.State, (int)DeliveryState.Expired, (int)DeliveryState.Pending) != (int)DeliveryState.Pending) return;
        Forget(delivery);
        Redeliver(delivery);
    }

    private void Forget(Delivery delivery)
    {
        _inFlight.TryRemove(delivery.Id, out _);
        _byLockToken.TryRemove(delivery.LockToken, out _);
        delivery.LockTimer?.Dispose();
    }

    private void Redeliver(Delivery delivery)
    {
        if (DeadLetterQueue is not null && delivery.DeliveryCount >= Options.MaxDeliveryCount)
        {
            SendToDeadLetter(delivery.Message, "MaxDeliveryCountExceeded", null);
            return;
        }
        _ready.Writer.TryWrite(new Pending(delivery.Message, delivery.DeliveryCount));
    }

    private void SendToDeadLetter(Message message, string reason, string? description)
    {
        if (DeadLetterQueue is null)
        {
            _ready.Writer.TryWrite(new Pending(message, 0));
            return;
        }
        message.ApplicationProperties ??= new ApplicationProperties();
        message.ApplicationProperties.Map["DeadLetterReason"] = reason;
        if (description is not null)
            message.ApplicationProperties.Map["DeadLetterErrorDescription"] = description;
        message.Header = null;
        DeadLetterQueue.Enqueue(message);
    }

    private static void SetWireDeliveryCount(Message message, int priorDeliveries)
    {
        message.Header ??= new Header();
        message.Header.DeliveryCount = (uint)priorDeliveries;
    }

    private readonly record struct Pending(Message Message, int PriorDeliveries);
}

sealed class Delivery(long id, Message message, int deliveryCount, Guid lockToken)
{
    public long Id { get; } = id;
    public Message Message { get; } = message;
    public int DeliveryCount { get; } = deliveryCount;
    public Guid LockToken { get; } = lockToken;
    public DateTime LockedUntil;
    public int State;
    public Timer? LockTimer;
}

enum DeliveryState
{
    Pending = 0,
    Settled = 1,
    Expired = 2,
}
