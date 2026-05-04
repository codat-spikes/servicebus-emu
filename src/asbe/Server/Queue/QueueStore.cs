using System.Collections.Concurrent;

sealed class QueueStore(IReadOnlyDictionary<string, QueueOptions> configured)
{
    private const string DeadLetterSuffix = "/$DeadLetterQueue";

    private readonly ConcurrentDictionary<string, InMemoryQueue> _queues = new();

    public InMemoryQueue Get(string address)
    {
        var trimmed = address.TrimStart('/');
        if (trimmed.EndsWith(DeadLetterSuffix, StringComparison.Ordinal))
        {
            var parent = trimmed[..^DeadLetterSuffix.Length];
            var queue = GetOrCreate(parent);
            return queue.DeadLetterQueue ?? throw new InvalidOperationException($"Queue '{parent}' has no dead-letter queue.");
        }
        return GetOrCreate(trimmed);
    }

    public void CreateQueue(string name, QueueOptions options)
    {
        if (!_queues.TryAdd(NormalizeName(name), new InMemoryQueue(options)))
            throw new InvalidOperationException($"Queue '{name}' already exists.");
    }

    public bool DeleteQueue(string name) => _queues.TryRemove(NormalizeName(name), out _);

    private static string NormalizeName(string name) => name.TrimStart('/');

    private InMemoryQueue GetOrCreate(string name) =>
        _queues.GetOrAdd(name, n => new InMemoryQueue(OptionsFor(n)));

    private QueueOptions OptionsFor(string name) =>
        configured.TryGetValue(name, out var opts) ? opts : QueueOptions.Default;
}
