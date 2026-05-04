using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class QueueStore
{
    private const string DeadLetterSuffix = "/$DeadLetterQueue";
    private const string ManagementSuffix = "/$management";

    private readonly IReadOnlyDictionary<string, QueueOptions> _configured;
    private readonly Action<string, InMemoryQueue> _onQueueCreated;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<QueueStore> _logger;
    private readonly ConcurrentDictionary<string, InMemoryQueue> _queues = new();

    public QueueStore(
        IReadOnlyDictionary<string, QueueOptions> configured,
        Action<string, InMemoryQueue> onQueueCreated,
        ILoggerFactory? loggerFactory = null)
    {
        _configured = configured;
        _onQueueCreated = onQueueCreated;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<QueueStore>();
    }

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
        var queue = new InMemoryQueue(options, _loggerFactory);
        if (!_queues.TryAdd(key, queue))
            throw new InvalidOperationException($"Queue '{name}' already exists.");
        _onQueueCreated(key, queue);
        _logger.LogInformation("Created queue '{Queue}' lockDuration={LockDuration} maxDeliveryCount={MaxDeliveryCount}",
            key, options.LockDuration, options.MaxDeliveryCount);
    }

    public bool DeleteQueue(string name)
    {
        var key = NormalizeName(name);
        var removed = _queues.TryRemove(key, out _);
        if (removed) _logger.LogInformation("Deleted queue '{Queue}'", key);
        return removed;
    }

    private static string NormalizeName(string name) => name.TrimStart('/');

    private InMemoryQueue GetOrCreate(string name)
    {
        if (_queues.TryGetValue(name, out var existing)) return existing;
        var fresh = new InMemoryQueue(OptionsFor(name), _loggerFactory);
        var actual = _queues.GetOrAdd(name, fresh);
        if (ReferenceEquals(actual, fresh))
        {
            _onQueueCreated(name, fresh);
            _logger.LogInformation("Auto-created queue '{Queue}' from incoming link", name);
        }
        return actual;
    }

    private QueueOptions OptionsFor(string name) =>
        _configured.TryGetValue(name, out var opts) ? opts : QueueOptions.Default;
}
