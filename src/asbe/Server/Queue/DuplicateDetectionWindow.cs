// Sliding-window MessageId set. Each TryAdd evicts entries past their expiry, then
// either records and returns true (first sighting) or returns false (duplicate within
// the window). Eviction is lazy — no timer — which keeps test teardown simple and
// avoids leaks; the window is small enough that walking the FIFO on each add is fine.
sealed class DuplicateDetectionWindow
{
    private readonly TimeSpan _window;
    private readonly object _lock = new();
    private readonly Dictionary<string, DateTime> _seen = new(StringComparer.Ordinal);
    private readonly Queue<(string Id, DateTime ExpiresAt)> _order = new();

    public DuplicateDetectionWindow(TimeSpan window) => _window = window;

    public bool TryAdd(string messageId)
    {
        var now = DateTime.UtcNow;
        lock (_lock)
        {
            while (_order.Count > 0 && _order.Peek().ExpiresAt <= now)
            {
                var (id, exp) = _order.Dequeue();
                // Only drop the dictionary entry if it still matches the FIFO entry —
                // a re-add would have updated _seen[id] to a later expiry.
                if (_seen.TryGetValue(id, out var stored) && stored == exp)
                    _seen.Remove(id);
            }
            if (_seen.ContainsKey(messageId)) return false;
            var expiresAt = now + _window;
            _seen[messageId] = expiresAt;
            _order.Enqueue((messageId, expiresAt));
            return true;
        }
    }
}
