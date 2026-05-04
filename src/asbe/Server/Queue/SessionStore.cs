using System.Collections.Concurrent;
using Amqp;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

// Holds per-session state for a queue: a per-session MessageBuffer for FIFO delivery,
// a session lock (only one receiver may dequeue from a session at a time), and an opaque
// session-state blob that survives across receiver attaches.
//
// Session messages share their sequence-number space with the parent queue's primary
// buffer (they're stamped via primary.AssignSequenceNumber before being routed here),
// so PeekMessage / sequence-number ordering stays globally consistent.
sealed class SessionStore
{
    private readonly ConcurrentDictionary<string, Session> _sessions = new();
    private readonly TimeSpan _lockDuration;
    private readonly MessageBuffer _primary;
    private readonly Action<Delivery> _onMessageLockExpired;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<SessionStore> _logger;

    private readonly object _waiterGate = new();
    private readonly LinkedList<TaskCompletionSource<bool>> _waiters = new();

    public SessionStore(
        TimeSpan lockDuration,
        MessageBuffer primary,
        Action<Delivery> onMessageLockExpired,
        ILoggerFactory loggerFactory)
    {
        _lockDuration = lockDuration;
        _primary = primary;
        _onMessageLockExpired = onMessageLockExpired;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<SessionStore>();
    }

    public Session GetOrCreate(string sessionId) =>
        _sessions.GetOrAdd(sessionId, id => new Session(
            id,
            _lockDuration,
            new MessageBuffer(_lockDuration, _onMessageLockExpired, _loggerFactory.CreateLogger<MessageBuffer>()),
            _loggerFactory.CreateLogger<Session>()));

    public Session? Find(string sessionId) =>
        _sessions.TryGetValue(sessionId, out var s) ? s : null;

    // Routes an enqueued message into its session sub-buffer, stamping a global sequence
    // number from the primary buffer first so peek/ordering stay consistent across the queue.
    public void Enqueue(string sessionId, Message message)
    {
        _primary.AssignSequenceNumber(message);
        GetOrCreate(sessionId).Buffer.Enqueue(message);
        SignalWaiters();
    }

    // Picks any session that has pending messages and isn't currently locked. Used by
    // "next available session" — receivers attach with a null session-filter value.
    public Session? TryFindUnlockedWithMessages()
    {
        foreach (var session in _sessions.Values)
        {
            if (session.IsLocked) continue;
            if (!session.Buffer.HasPending) continue;
            return session;
        }
        return null;
    }

    // Async variant for "next available session" attach. Real Service Bus blocks the
    // attach until a session becomes available or the operation times out; we mirror that
    // by parking the attach on a TCS that gets pulsed whenever a session is enqueued or a
    // session lock is released. Returns null on timeout / cancellation.
    public async Task<Session?> WaitForUnlockedWithMessagesAsync(TimeSpan timeout, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var candidate = TryFindUnlockedWithMessages();
            if (candidate is not null) return candidate;

            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero) return null;

            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            LinkedListNode<TaskCompletionSource<bool>> node;
            lock (_waiterGate) node = _waiters.AddLast(tcs);
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(remaining);
                using var reg = cts.Token.Register(static s => ((TaskCompletionSource<bool>)s!).TrySetResult(false), tcs);
                await tcs.Task.ConfigureAwait(false);
                if (ct.IsCancellationRequested) return null;
            }
            finally
            {
                lock (_waiterGate) _waiters.Remove(node);
            }
        }
    }

    // Releases the session lock and pulses waiters parked on WaitForUnlockedWithMessagesAsync
    // so the next "accept next session" attach can pick it up.
    public void ReleaseLock(Session session, Guid token)
    {
        session.Release(token);
        if (session.Buffer.HasPending) SignalWaiters();
    }

    private void SignalWaiters()
    {
        lock (_waiterGate)
        {
            foreach (var w in _waiters) w.TrySetResult(true);
        }
    }

    public bool TryRenewMessageLock(Guid lockToken, out DateTime expiresAt)
    {
        foreach (var session in _sessions.Values)
        {
            if (session.Buffer.TryRenewLock(lockToken, out expiresAt)) return true;
        }
        expiresAt = default;
        return false;
    }
}

sealed class Session
{
    public string Id { get; }
    public MessageBuffer Buffer { get; }
    public byte[] State { get; set; } = [];

    private readonly TimeSpan _lockDuration;
    private readonly ILogger<Session> _logger;
    private readonly object _gate = new();
    private Guid _lockToken;
    private DateTime _lockedUntil;
    private bool _locked;

    public Session(string id, TimeSpan lockDuration, MessageBuffer buffer, ILogger<Session> logger)
    {
        Id = id;
        Buffer = buffer;
        _lockDuration = lockDuration;
        _logger = logger;
    }

    public bool IsLocked
    {
        get
        {
            lock (_gate) return _locked && DateTime.UtcNow < _lockedUntil;
        }
    }

    public bool TryAcquireLock(out Guid token, out DateTime expiresAt)
    {
        lock (_gate)
        {
            if (_locked && DateTime.UtcNow < _lockedUntil)
            {
                token = default;
                expiresAt = default;
                return false;
            }
            _lockToken = Guid.NewGuid();
            _lockedUntil = DateTime.UtcNow + _lockDuration;
            _locked = true;
            token = _lockToken;
            expiresAt = _lockedUntil;
            _logger.LogTrace("Acquired session lock id={SessionId} token={Token}", Id, token);
            return true;
        }
    }

    public bool TryRenewLock(out DateTime expiresAt)
    {
        lock (_gate)
        {
            if (!_locked || DateTime.UtcNow >= _lockedUntil)
            {
                expiresAt = default;
                return false;
            }
            _lockedUntil = DateTime.UtcNow + _lockDuration;
            expiresAt = _lockedUntil;
            return true;
        }
    }

    public bool TryRenewLock(Guid token, out DateTime expiresAt)
    {
        lock (_gate)
        {
            if (!_locked || _lockToken != token || DateTime.UtcNow >= _lockedUntil)
            {
                expiresAt = default;
                return false;
            }
            _lockedUntil = DateTime.UtcNow + _lockDuration;
            expiresAt = _lockedUntil;
            return true;
        }
    }

    public void Release(Guid token)
    {
        lock (_gate)
        {
            if (_lockToken != token) return;
            _locked = false;
            _logger.LogTrace("Released session lock id={SessionId} token={Token}", Id, token);
        }
    }
}
