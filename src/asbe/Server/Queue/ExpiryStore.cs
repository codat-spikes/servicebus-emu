using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

// Tracks per-message TTL expiries for an InMemoryQueue. One timer aimed at the
// soonest expiry; on fire we fan out the sequence numbers that are due and let
// the owning queue decide whether to evict (idle in primary) or defer (peek-locked).
sealed class ExpiryStore : IDisposable
{
    private readonly Action<long> _onExpired;
    private readonly ILogger<ExpiryStore> _logger;
    private readonly object _gate = new();
    private readonly Dictionary<long, DateTime> _bySeq = new();
    private readonly SortedDictionary<DateTime, List<long>> _byDue = new();
    private readonly Timer _timer;

    public ExpiryStore(Action<long> onExpired, ILogger<ExpiryStore>? logger = null)
    {
        _onExpired = onExpired;
        _logger = logger ?? NullLogger<ExpiryStore>.Instance;
        _timer = new Timer(_ => SafeFlush(), null, Timeout.Infinite, Timeout.Infinite);
    }

    public void Register(long sequenceNumber, DateTime expiresAtUtc)
    {
        lock (_gate)
        {
            if (_bySeq.TryGetValue(sequenceNumber, out var existing))
            {
                if (_byDue.TryGetValue(existing, out var oldList))
                {
                    oldList.Remove(sequenceNumber);
                    if (oldList.Count == 0) _byDue.Remove(existing);
                }
            }
            _bySeq[sequenceNumber] = expiresAtUtc;
            if (!_byDue.TryGetValue(expiresAtUtc, out var list))
                _byDue[expiresAtUtc] = list = new List<long>();
            list.Add(sequenceNumber);
            ArmTimer();
        }
    }

    public void Cancel(long sequenceNumber)
    {
        lock (_gate)
        {
            if (!_bySeq.Remove(sequenceNumber, out var due)) return;
            if (_byDue.TryGetValue(due, out var list))
            {
                list.Remove(sequenceNumber);
                if (list.Count == 0) _byDue.Remove(due);
            }
            ArmTimer();
        }
    }

    private void SafeFlush()
    {
        try { Flush(); }
        catch (Exception ex) { _logger.LogError(ex, "Expiry flush failed"); }
    }

    private void ArmTimer()
    {
        if (_byDue.Count == 0)
        {
            _timer.Change(Timeout.Infinite, Timeout.Infinite);
            return;
        }
        DateTime next = default;
        foreach (var key in _byDue.Keys) { next = key; break; }
        var delay = next - DateTime.UtcNow;
        if (delay < TimeSpan.Zero) delay = TimeSpan.Zero;
        _timer.Change(delay, Timeout.InfiniteTimeSpan);
    }

    private void Flush()
    {
        var due = new List<long>();
        lock (_gate)
        {
            var now = DateTime.UtcNow;
            while (_byDue.Count > 0)
            {
                DateTime first = default;
                List<long>? bucket = null;
                foreach (var kv in _byDue) { first = kv.Key; bucket = kv.Value; break; }
                if (first > now) break;
                _byDue.Remove(first);
                foreach (var seq in bucket!)
                {
                    if (_bySeq.Remove(seq)) due.Add(seq);
                }
            }
            ArmTimer();
        }
        foreach (var seq in due) _onExpired(seq);
    }

    public void Dispose() => _timer.Dispose();
}
