using System.Collections.Concurrent;
using System.Threading.Channels;
using Amqp;
using Amqp.Framing;

sealed class MessageBuffer
{
    public TimeSpan LockDuration { get; }

    private readonly Channel<Pending> _ready = Channel.CreateUnbounded<Pending>();
    private readonly ConcurrentDictionary<long, Delivery> _inFlight = new();
    private readonly ConcurrentDictionary<Guid, Delivery> _byLockToken = new();
    private readonly Action<Delivery> _onLockExpired;
    private long _nextDeliveryId;

    public MessageBuffer(TimeSpan lockDuration, Action<Delivery> onLockExpired)
    {
        LockDuration = lockDuration;
        _onLockExpired = onLockExpired;
    }

    public void Enqueue(Message message) => _ready.Writer.TryWrite(new Pending(message, 0));

    public void Requeue(Delivery delivery) => _ready.Writer.TryWrite(new Pending(delivery.Message, delivery.DeliveryCount));

    public async Task<Delivery> DequeueAsync(CancellationToken cancellation = default)
    {
        var pending = await _ready.Reader.ReadAsync(cancellation);
        return StartDelivery(pending);
    }

    public bool TryRelease(long deliveryId, out Delivery delivery)
    {
        if (!_inFlight.TryGetValue(deliveryId, out delivery!)) return false;
        if (Interlocked.CompareExchange(ref delivery.State, (int)DeliveryState.Settled, (int)DeliveryState.Pending) != (int)DeliveryState.Pending)
            return false;
        Forget(delivery);
        return true;
    }

    public bool TryRenewLock(Guid lockToken, out DateTime expiresAt)
    {
        expiresAt = default;
        if (!_byLockToken.TryGetValue(lockToken, out var delivery)) return false;
        if (Volatile.Read(ref delivery.State) != (int)DeliveryState.Pending) return false;
        expiresAt = DateTime.UtcNow + LockDuration;
        delivery.LockedUntil = expiresAt;
        delivery.LockTimer.Change(LockDuration, Timeout.InfiniteTimeSpan);
        return true;
    }

    private Delivery StartDelivery(Pending pending)
    {
        var deliveryId = Interlocked.Increment(ref _nextDeliveryId);
        SetWireDeliveryCount(pending.Message, pending.PriorDeliveries);
        var lockToken = Guid.NewGuid();
        var lockedUntil = DateTime.UtcNow + LockDuration;
        var timer = new Timer(_ => OnLockExpired(deliveryId), null, Timeout.Infinite, Timeout.Infinite);
        var delivery = new Delivery(deliveryId, pending.Message, pending.PriorDeliveries + 1, lockToken, lockedUntil, timer);
        _inFlight[deliveryId] = delivery;
        _byLockToken[lockToken] = delivery;
        timer.Change(LockDuration, Timeout.InfiniteTimeSpan);
        return delivery;
    }

    private void OnLockExpired(long deliveryId)
    {
        if (!_inFlight.TryGetValue(deliveryId, out var delivery)) return;
        if (Interlocked.CompareExchange(ref delivery.State, (int)DeliveryState.Expired, (int)DeliveryState.Pending) != (int)DeliveryState.Pending)
            return;
        Forget(delivery);
        _onLockExpired(delivery);
    }

    private void Forget(Delivery delivery)
    {
        _inFlight.TryRemove(delivery.Id, out _);
        _byLockToken.TryRemove(delivery.LockToken, out _);
        delivery.LockTimer.Dispose();
    }

    private static void SetWireDeliveryCount(Message message, int priorDeliveries)
    {
        message.Header ??= new Header();
        message.Header.DeliveryCount = (uint)priorDeliveries;
    }

    private readonly record struct Pending(Message Message, int PriorDeliveries);
}

sealed class Delivery(long id, Message message, int deliveryCount, Guid lockToken, DateTime lockedUntil, Timer lockTimer)
{
    public long Id { get; } = id;
    public Message Message { get; } = message;
    public int DeliveryCount { get; } = deliveryCount;
    public Guid LockToken { get; } = lockToken;
    public Timer LockTimer { get; } = lockTimer;
    public DateTime LockedUntil = lockedUntil;
    public int State;
}

enum DeliveryState
{
    Pending = 0,
    Settled = 1,
    Expired = 2,
}

readonly record struct DeadLetterInfo(string Reason, string Description = "")
{
    public static DeadLetterInfo DeadLetteredByReceiver { get; } = new("DeadLetteredByReceiver");
    public static DeadLetterInfo MaxDeliveryCountExceeded { get; } = new("MaxDeliveryCountExceeded");
}
