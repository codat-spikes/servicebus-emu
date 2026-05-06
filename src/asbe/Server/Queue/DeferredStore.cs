using System.Collections.Concurrent;
using Amqp;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

// Holds messages that were deferred via the AMQP receive-link path
// (Modified{UndeliverableHere=true}). Deferred messages are pulled out of the ready
// buffer and are only retrievable by sequence number via the $management
// `com.microsoft:receive-by-sequence-number` op, which hands out fresh lock-tokens.
// Subsequent dispositions arrive via `com.microsoft:update-disposition`.
sealed class DeferredStore : IDisposable
{
    private readonly TimeSpan _lockDuration;
    private readonly ILogger<DeferredStore> _logger;
    private readonly ConcurrentDictionary<long, DeferredEntry> _bySeq = new();
    private readonly ConcurrentDictionary<Guid, DeferredEntry> _byLockToken = new();

    public DeferredStore(TimeSpan lockDuration, ILogger<DeferredStore>? logger = null)
    {
        _lockDuration = lockDuration;
        _logger = logger ?? NullLogger<DeferredStore>.Instance;
    }

    public int Count => _bySeq.Count;

    public void Add(long sequenceNumber, Message message)
    {
        _bySeq[sequenceNumber] = new DeferredEntry(sequenceNumber, message);
        _logger.LogTrace("Deferred seq={SequenceNumber}", sequenceNumber);
    }

    // Issues a fresh lock-token and starts an expiry timer. Lock expiry just clears the
    // lock — the message stays deferred and can be re-acquired with another receive
    // call (matching Service Bus semantics: deferred messages don't time out).
    public bool TryAcquire(long sequenceNumber, out Message message, out Guid lockToken, out DateTime lockedUntil)
    {
        message = null!;
        lockToken = default;
        lockedUntil = default;
        if (!_bySeq.TryGetValue(sequenceNumber, out var entry)) return false;

        lock (entry)
        {
            if (entry.Settled) return false;
            ReleaseLockLocked(entry);
            entry.LockToken = Guid.NewGuid();
            entry.LockedUntil = DateTime.UtcNow + _lockDuration;
            entry.LockTimer = new Timer(_ => ExpireLock(entry.LockToken), null, _lockDuration, Timeout.InfiniteTimeSpan);
            _byLockToken[entry.LockToken] = entry;
            message = entry.Message;
            lockToken = entry.LockToken;
            lockedUntil = entry.LockedUntil;
            return true;
        }
    }

    public bool TryComplete(Guid lockToken)
    {
        if (!_byLockToken.TryGetValue(lockToken, out var entry)) return false;
        lock (entry)
        {
            if (entry.Settled || entry.LockToken != lockToken) return false;
            entry.Settled = true;
            ReleaseLockLocked(entry);
        }
        _bySeq.TryRemove(entry.SequenceNumber, out _);
        return true;
    }

    public bool TryAbandon(Guid lockToken, out Message message)
    {
        message = null!;
        if (!_byLockToken.TryGetValue(lockToken, out var entry)) return false;
        lock (entry)
        {
            if (entry.Settled || entry.LockToken != lockToken) return false;
            ReleaseLockLocked(entry);
            message = entry.Message;
            return true;
        }
    }

    public bool TryReject(Guid lockToken, out Message message, out long sequenceNumber)
    {
        message = null!;
        sequenceNumber = 0;
        if (!_byLockToken.TryGetValue(lockToken, out var entry)) return false;
        lock (entry)
        {
            if (entry.Settled || entry.LockToken != lockToken) return false;
            entry.Settled = true;
            ReleaseLockLocked(entry);
            message = entry.Message;
            sequenceNumber = entry.SequenceNumber;
        }
        _bySeq.TryRemove(entry.SequenceNumber, out _);
        return true;
    }

    private void ExpireLock(Guid lockToken)
    {
        if (!_byLockToken.TryGetValue(lockToken, out var entry)) return;
        lock (entry)
        {
            if (entry.LockToken != lockToken) return;
            ReleaseLockLocked(entry);
        }
        _logger.LogTrace("Deferred lock expired seq={SequenceNumber}", entry.SequenceNumber);
    }

    private void ReleaseLockLocked(DeferredEntry entry)
    {
        if (entry.LockToken != default) _byLockToken.TryRemove(entry.LockToken, out _);
        entry.LockTimer?.Dispose();
        entry.LockTimer = null;
        entry.LockToken = default;
        entry.LockedUntil = default;
    }

    public void Dispose()
    {
        foreach (var entry in _bySeq.Values) entry.LockTimer?.Dispose();
        _bySeq.Clear();
        _byLockToken.Clear();
    }

    sealed class DeferredEntry(long sequenceNumber, Message message)
    {
        public long SequenceNumber { get; } = sequenceNumber;
        public Message Message { get; } = message;
        public Guid LockToken;
        public DateTime LockedUntil;
        public Timer? LockTimer;
        public bool Settled;
    }
}
