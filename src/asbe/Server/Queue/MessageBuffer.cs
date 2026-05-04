using System.Collections.Concurrent;
using System.Threading.Channels;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class MessageBuffer : IDisposable
{
    // Service Bus stamps each message with x-opt-sequence-number on first enqueue and
    // preserves it across redeliveries; the SDK reads it into ServiceBusReceivedMessage.SequenceNumber
    // and peek requests are anchored on it.
    public static readonly Symbol SequenceNumberAnnotation = "x-opt-sequence-number";

    public TimeSpan LockDuration { get; }

    private readonly Channel<Pending> _ready = Channel.CreateUnbounded<Pending>();
    private readonly ConcurrentDictionary<long, Delivery> _inFlight = new();
    private readonly ConcurrentDictionary<long, Delivery> _inFlightBySeq = new();
    private readonly ConcurrentDictionary<Guid, Delivery> _byLockToken = new();
    private readonly Action<Delivery> _onLockExpired;
    private readonly ILogger<MessageBuffer> _logger;
    private readonly object _trackedLock = new();
    private readonly SortedDictionary<long, Message> _tracked = new();
    private long _nextDeliveryId;
    private long _nextSequenceNumber;

    public MessageBuffer(TimeSpan lockDuration, Action<Delivery> onLockExpired, ILogger<MessageBuffer>? logger = null)
    {
        LockDuration = lockDuration;
        _onLockExpired = onLockExpired;
        _logger = logger ?? NullLogger<MessageBuffer>.Instance;
    }

    public long Enqueue(Message message)
    {
        var seq = AssignSequenceIfMissing(message);
        lock (_trackedLock) _tracked[seq] = message;
        _ready.Writer.TryWrite(new Pending(message, 0));
        return seq;
    }

    public void Requeue(Delivery delivery) => _ready.Writer.TryWrite(new Pending(delivery.Message, delivery.DeliveryCount));

    public void Drop(long sequenceNumber)
    {
        lock (_trackedLock) _tracked.Remove(sequenceNumber);
    }

    public bool HasPending
    {
        get { lock (_trackedLock) return _tracked.Count > 0; }
    }

    public IReadOnlyList<Message> Peek(long fromSequenceNumber, int maxCount)
    {
        if (maxCount <= 0) return [];
        var result = new List<Message>();
        lock (_trackedLock)
        {
            foreach (var (seq, message) in _tracked)
            {
                if (seq < fromSequenceNumber) continue;
                result.Add(message);
                if (result.Count >= maxCount) break;
            }
        }
        return result;
    }

    public async Task<Delivery> DequeueAsync(CancellationToken cancellation = default)
    {
        while (true)
        {
            var pending = await _ready.Reader.ReadAsync(cancellation);
            // Tombstone check: a TTL eviction may have removed the message from _tracked
            // while its Pending entry sits in the channel. Skip and pull the next one.
            var seq = ReadSequenceNumber(pending.Message);
            if (seq != 0)
            {
                lock (_trackedLock)
                {
                    if (!_tracked.ContainsKey(seq)) continue;
                }
            }
            return StartDelivery(pending);
        }
    }

    public bool TryRelease(long deliveryId, out Delivery delivery)
    {
        if (!_inFlight.TryGetValue(deliveryId, out delivery!)) return false;
        if (Interlocked.CompareExchange(ref delivery.State, (int)BufferedDeliveryState.Settled, (int)BufferedDeliveryState.Pending) != (int)BufferedDeliveryState.Pending)
            return false;
        Forget(delivery);
        return true;
    }

    public bool TryRenewLock(Guid lockToken, out DateTime expiresAt)
    {
        expiresAt = default;
        if (!_byLockToken.TryGetValue(lockToken, out var delivery)) return false;
        if (Volatile.Read(ref delivery.State) != (int)BufferedDeliveryState.Pending) return false;
        expiresAt = DateTime.UtcNow + LockDuration;
        delivery.LockedUntil = expiresAt;
        delivery.LockTimer.Change(LockDuration, Timeout.InfiniteTimeSpan);
        return true;
    }

    private Delivery StartDelivery(Pending pending)
    {
        var deliveryId = Interlocked.Increment(ref _nextDeliveryId);
        SetWireDeliveryCount(pending.Message, pending.PriorDeliveries);
        var sequenceNumber = ReadSequenceNumber(pending.Message);
        var lockToken = Guid.NewGuid();
        var lockedUntil = DateTime.UtcNow + LockDuration;
        var timer = new Timer(_ => OnLockExpired(deliveryId), null, Timeout.Infinite, Timeout.Infinite);
        var delivery = new Delivery(deliveryId, pending.Message, pending.PriorDeliveries + 1, sequenceNumber, lockToken, lockedUntil, timer);
        _inFlight[deliveryId] = delivery;
        _inFlightBySeq[sequenceNumber] = delivery;
        _byLockToken[lockToken] = delivery;
        timer.Change(LockDuration, Timeout.InfiniteTimeSpan);
        return delivery;
    }

    public long AssignSequenceNumber(Message message) => AssignSequenceIfMissing(message);

    private long AssignSequenceIfMissing(Message message)
    {
        message.MessageAnnotations ??= new MessageAnnotations();
        var existing = message.MessageAnnotations.Map[SequenceNumberAnnotation];
        if (existing is long seq) return seq;
        seq = Interlocked.Increment(ref _nextSequenceNumber);
        message.MessageAnnotations.Map[SequenceNumberAnnotation] = seq;
        return seq;
    }

    private static long ReadSequenceNumber(Message message) =>
        message.MessageAnnotations?.Map[SequenceNumberAnnotation] is long seq ? seq : 0;

    private void OnLockExpired(long deliveryId)
    {
        if (!_inFlight.TryGetValue(deliveryId, out var delivery)) return;
        if (Interlocked.CompareExchange(ref delivery.State, (int)BufferedDeliveryState.Expired, (int)BufferedDeliveryState.Pending) != (int)BufferedDeliveryState.Pending)
            return;
        Forget(delivery);
        _logger.LogTrace("Lock expired delivery={DeliveryId} seq={SequenceNumber} count={DeliveryCount}",
            delivery.Id, delivery.SequenceNumber, delivery.DeliveryCount);
        try
        {
            _onLockExpired(delivery);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Lock-expired callback failed for delivery={DeliveryId} seq={SequenceNumber}",
                delivery.Id, delivery.SequenceNumber);
        }
    }

    private void Forget(Delivery delivery)
    {
        _inFlight.TryRemove(delivery.Id, out _);
        _inFlightBySeq.TryRemove(delivery.SequenceNumber, out _);
        _byLockToken.TryRemove(delivery.LockToken, out _);
        delivery.LockTimer.Dispose();
    }

    // Removes a ready (not in-flight) message from tracking. The Pending entry left in
    // the channel is tombstoned: DequeueAsync sees the missing _tracked slot and skips.
    public bool TryEvictReady(long sequenceNumber, out Message message)
    {
        message = null!;
        if (_inFlightBySeq.ContainsKey(sequenceNumber)) return false;
        lock (_trackedLock)
        {
            if (!_tracked.TryGetValue(sequenceNumber, out var found)) return false;
            if (_inFlightBySeq.ContainsKey(sequenceNumber)) return false;
            _tracked.Remove(sequenceNumber);
            message = found;
            return true;
        }
    }

    public bool TryGetTracked(long sequenceNumber, out Message message)
    {
        lock (_trackedLock)
        {
            if (_tracked.TryGetValue(sequenceNumber, out var found))
            {
                message = found;
                return true;
            }
            message = null!;
            return false;
        }
    }

    private static void SetWireDeliveryCount(Message message, int priorDeliveries)
    {
        message.Header ??= new Header();
        message.Header.DeliveryCount = (uint)priorDeliveries;
    }

    public void Dispose()
    {
        // Lock timers root their callbacks (which capture `this`); without disposal
        // they keep the buffer alive until the timer fires.
        foreach (var delivery in _inFlight.Values) delivery.LockTimer.Dispose();
        _inFlight.Clear();
        _byLockToken.Clear();
        _ready.Writer.TryComplete();
    }

    private readonly record struct Pending(Message Message, int PriorDeliveries);
}

sealed class Delivery(long id, Message message, int deliveryCount, long sequenceNumber, Guid lockToken, DateTime lockedUntil, Timer lockTimer)
{
    public long Id { get; } = id;
    public Message Message { get; } = message;
    public int DeliveryCount { get; } = deliveryCount;
    public long SequenceNumber { get; } = sequenceNumber;
    public Guid LockToken { get; } = lockToken;
    public Timer LockTimer { get; } = lockTimer;
    public DateTime LockedUntil = lockedUntil;
    public int State;
}

enum BufferedDeliveryState
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
