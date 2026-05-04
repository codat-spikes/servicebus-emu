using System.Collections.Concurrent;

sealed class QueueStore(IReadOnlyDictionary<string, QueueOptions> configured, Action<string, InMemoryQueue> onQueueCreated)
{
    private const string DeadLetterSuffix = "/$DeadLetterQueue";
    private const string ManagementSuffix = "/$management";

    private readonly ConcurrentDictionary<string, InMemoryQueue> _queues = new();

    public string ManagementAddressFor(string name) => NormalizeName(name) + ManagementSuffix;

    public IQueueEndpoint Get(string address)
    {
        var trimmed = address.TrimStart('/');
        if (trimmed.EndsWith(DeadLetterSuffix, StringComparison.Ordinal))
            return GetOrCreate(trimmed[..^DeadLetterSuffix.Length]).DeadLetter;
        return GetOrCreate(trimmed);
    }

    public void CreateQueue(string name, QueueOptions options)
    {
        var key = NormalizeName(name);
        var queue = new InMemoryQueue(options);
        if (!_queues.TryAdd(key, queue))
            throw new InvalidOperationException($"Queue '{name}' already exists.");
        onQueueCreated(key, queue);
    }

    public bool DeleteQueue(string name) => _queues.TryRemove(NormalizeName(name), out _);

    private static string NormalizeName(string name) => name.TrimStart('/');

    private InMemoryQueue GetOrCreate(string name)
    {
        if (_queues.TryGetValue(name, out var existing)) return existing;
        var fresh = new InMemoryQueue(OptionsFor(name));
        var actual = _queues.GetOrAdd(name, fresh);
        if (ReferenceEquals(actual, fresh)) onQueueCreated(name, fresh);
        return actual;
    }

    private QueueOptions OptionsFor(string name) =>
        configured.TryGetValue(name, out var opts) ? opts : QueueOptions.Default;
}
