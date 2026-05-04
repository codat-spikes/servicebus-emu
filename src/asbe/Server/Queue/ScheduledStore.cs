using Amqp;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

// Holds messages scheduled for future enqueue. Service Bus assigns the sequence number
// at schedule time and the same number is what callers cancel by — so we lean on the
// primary buffer's sequence counter to keep schedule/peek/cancel in one namespace, and
// stamp it onto the message annotation so the active queue keeps it on flush.
sealed class ScheduledStore : IDisposable
{
    private readonly Func<Message, long> _assignSequence;
    private readonly Action<Message> _enqueue;
    private readonly ILogger<ScheduledStore> _logger;
    private readonly object _gate = new();
    private readonly Dictionary<long, ScheduledEntry> _bySeq = new();
    private readonly SortedDictionary<DateTime, List<long>> _byDue = new();
    private readonly Timer _timer;

    public ScheduledStore(Func<Message, long> assignSequence, Action<Message> enqueue, ILogger<ScheduledStore>? logger = null)
    {
        _assignSequence = assignSequence;
        _enqueue = enqueue;
        _logger = logger ?? NullLogger<ScheduledStore>.Instance;
        _timer = new Timer(_ => SafeFlush(), null, Timeout.Infinite, Timeout.Infinite);
    }

    public long Schedule(Message message, DateTime enqueueAtUtc)
    {
        var seq = _assignSequence(message);
        lock (_gate)
        {
            _bySeq[seq] = new ScheduledEntry(enqueueAtUtc, message);
            if (!_byDue.TryGetValue(enqueueAtUtc, out var list))
                _byDue[enqueueAtUtc] = list = new List<long>();
            list.Add(seq);
            ArmTimer();
        }
        _logger.LogTrace("Scheduled seq={SequenceNumber} enqueueAt={EnqueueAt:O}", seq, enqueueAtUtc);
        return seq;
    }

    public bool Cancel(long sequenceNumber)
    {
        lock (_gate)
        {
            if (!_bySeq.Remove(sequenceNumber, out var entry)) return false;
            if (_byDue.TryGetValue(entry.EnqueueAt, out var list))
            {
                list.Remove(sequenceNumber);
                if (list.Count == 0) _byDue.Remove(entry.EnqueueAt);
            }
            ArmTimer();
            _logger.LogTrace("Cancelled scheduled seq={SequenceNumber}", sequenceNumber);
            return true;
        }
    }

    private void SafeFlush()
    {
        try { Flush(); }
        catch (Exception ex) { _logger.LogError(ex, "Scheduled flush failed"); }
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
        var ready = new List<Message>();
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
                    if (_bySeq.Remove(seq, out var entry)) ready.Add(entry.Message);
                }
            }
            ArmTimer();
        }
        foreach (var message in ready) _enqueue(message);
        if (ready.Count > 0)
            _logger.LogTrace("Flushed {Count} scheduled messages", ready.Count);
    }

    public void Dispose() => _timer.Dispose();

    private readonly record struct ScheduledEntry(DateTime EnqueueAt, Message Message);
}
