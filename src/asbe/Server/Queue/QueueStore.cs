using System.Collections.Concurrent;
using Amqp;
using Amqp.Framing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

readonly record struct EndpointResolution(IQueueEndpoint? Queue, Topic? Topic, Error? Error)
{
    public static EndpointResolution OfQueue(IQueueEndpoint q) => new(q, null, null);
    public static EndpointResolution OfTopic(Topic t) => new(null, t, null);
    public static EndpointResolution OfError(Error e) => new(null, null, e);
}

sealed class QueueStore
{
    private const string DeadLetterSuffix = "/$DeadLetterQueue";
    private const string ManagementSuffix = "/$management";
    private const string SubscriptionsSegment = "/Subscriptions/";

    private readonly IReadOnlyDictionary<string, QueueOptions> _configured;
    private readonly Action<string, InMemoryQueue> _onQueueCreated;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<QueueStore> _logger;
    private readonly ConcurrentDictionary<string, InMemoryQueue> _queues = new();
    private readonly ConcurrentDictionary<string, Topic> _topics = new();

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

    public string QueueManagementAddressFor(string name) => NormalizeName(name) + ManagementSuffix;
    public string TopicManagementAddressFor(string name) => NormalizeName(name) + ManagementSuffix;
    public string SubscriptionManagementAddressFor(string topic, string subscription) =>
        NormalizeName(topic) + SubscriptionsSegment + subscription + ManagementSuffix;

    public EndpointResolution Get(string address)
    {
        var trimmed = address.TrimStart('/');

        var subIndex = trimmed.IndexOf(SubscriptionsSegment, StringComparison.Ordinal);
        if (subIndex >= 0)
        {
            var topicName = trimmed[..subIndex];
            var rest = trimmed[(subIndex + SubscriptionsSegment.Length)..];

            // rest is "<sub>" or "<sub>/$DeadLetterQueue" or "<sub>/$management"
            string subName;
            string suffix;
            var slash = rest.IndexOf('/');
            if (slash < 0) { subName = rest; suffix = ""; }
            else { subName = rest[..slash]; suffix = rest[slash..]; }

            if (!_topics.TryGetValue(topicName, out var topic))
                return EndpointResolution.OfError(new Error(ErrorCode.NotFound) { Description = $"Topic '{topicName}' does not exist." });
            if (!topic.TryGetSubscription(subName, out var subQueue))
                return EndpointResolution.OfError(new Error(ErrorCode.NotFound) { Description = $"Subscription '{subName}' does not exist on topic '{topicName}'." });

            return suffix switch
            {
                "" or ManagementSuffix => EndpointResolution.OfQueue(subQueue),
                DeadLetterSuffix => EndpointResolution.OfQueue(subQueue.DeadLetter),
                _ => EndpointResolution.OfError(new Error(ErrorCode.InvalidField) { Description = $"Unknown subscription sub-address '{suffix}'." }),
            };
        }

        if (_topics.TryGetValue(trimmed, out var topicAtRoot))
            return EndpointResolution.OfTopic(topicAtRoot);

        if (trimmed.EndsWith(DeadLetterSuffix, StringComparison.Ordinal))
            return EndpointResolution.OfQueue(GetOrCreateQueue(trimmed[..^DeadLetterSuffix.Length]).DeadLetter);
        return EndpointResolution.OfQueue(GetOrCreateQueue(trimmed));
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

    public Topic CreateTopic(string name, TopicOptions options)
    {
        var key = NormalizeName(name);
        var topic = new Topic(key, options, _loggerFactory);
        if (!_topics.TryAdd(key, topic))
            throw new InvalidOperationException($"Topic '{name}' already exists.");
        _logger.LogInformation("Created topic '{Topic}' subscriptions={Count}", key, topic.Subscriptions.Count);
        foreach (var (subName, subQueue) in topic.Subscriptions)
            _logger.LogInformation("  └─ subscription '{Subscription}' on topic '{Topic}'", subName, key);
        return topic;
    }

    public bool DeleteTopic(string name) => _topics.TryRemove(NormalizeName(name), out _);

    private static string NormalizeName(string name) => name.TrimStart('/');

    private InMemoryQueue GetOrCreateQueue(string name)
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
