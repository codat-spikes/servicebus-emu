using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed record TopicOptions(IReadOnlyDictionary<string, SubscriptionOptions> Subscriptions);

sealed class Topic
{
    private static readonly Symbol SequenceNumberAnnotation = MessageBuffer.SequenceNumberAnnotation;
    private static readonly Symbol ScheduledEnqueueTimeAnnotation = "x-opt-scheduled-enqueue-time";

    public string Name { get; }
    public IReadOnlyDictionary<string, InMemoryQueue> Subscriptions => _subscriptions;

    private readonly Dictionary<string, InMemoryQueue> _subscriptions;
    private readonly Dictionary<string, IReadOnlyList<RuleFilter>> _subscriptionRules;
    private readonly ILogger<Topic> _logger;
    private readonly ScheduledStore _scheduled;
    private long _nextSequenceNumber;

    public Topic(string name, TopicOptions options, ILoggerFactory? loggerFactory = null)
    {
        loggerFactory ??= NullLoggerFactory.Instance;
        Name = name;
        _logger = loggerFactory.CreateLogger<Topic>();
        _subscriptions = new Dictionary<string, InMemoryQueue>(options.Subscriptions.Count, StringComparer.Ordinal);
        _subscriptionRules = new Dictionary<string, IReadOnlyList<RuleFilter>>(options.Subscriptions.Count, StringComparer.Ordinal);
        foreach (var (subName, subOptions) in options.Subscriptions)
        {
            _subscriptions[subName] = new InMemoryQueue(subOptions.Queue, loggerFactory);
            _subscriptionRules[subName] = subOptions.Rules is { Count: > 0 } ? subOptions.Rules : [RuleFilter.MatchAll];
        }
        _scheduled = new ScheduledStore(AssignSequenceNumber, Enqueue, loggerFactory.CreateLogger<ScheduledStore>());
    }

    public long Schedule(Message message)
    {
        var enqueueAt = message.MessageAnnotations?.Map[ScheduledEnqueueTimeAnnotation] is DateTime t
            ? t
            : DateTime.UtcNow;
        return _scheduled.Schedule(message, enqueueAt);
    }

    public bool CancelScheduled(long sequenceNumber) => _scheduled.Cancel(sequenceNumber);

    private long AssignSequenceNumber(Message message)
    {
        message.MessageAnnotations ??= new MessageAnnotations();
        var existing = message.MessageAnnotations.Map[SequenceNumberAnnotation];
        if (existing is long seq) return seq;
        seq = Interlocked.Increment(ref _nextSequenceNumber);
        message.MessageAnnotations.Map[SequenceNumberAnnotation] = seq;
        return seq;
    }

    public bool TryGetSubscription(string name, out InMemoryQueue subscription) =>
        _subscriptions.TryGetValue(name, out subscription!);

    public void Enqueue(Message message)
    {
        // Each subscription's MessageBuffer stamps a sequence-number annotation onto the
        // message reference and stores the reference itself. Without a deep clone, every
        // subscription would share the same annotation map and the same Message instance,
        // so the second subscription's enqueue would reuse the first subscription's
        // sequence number and any later mutation (peek, lock state) would bleed across.
        var delivered = 0;
        foreach (var (subName, subscription) in _subscriptions)
        {
            if (!AnyRuleMatches(_subscriptionRules[subName], message)) continue;
            subscription.Enqueue(Clone(message));
            delivered++;
        }
        _logger.LogTrace("Topic '{Topic}' fanned message to {Delivered}/{Total} subscription(s)",
            Name, delivered, _subscriptions.Count);
    }

    private static bool AnyRuleMatches(IReadOnlyList<RuleFilter> rules, Message message)
    {
        for (int i = 0; i < rules.Count; i++)
            if (rules[i].Matches(message)) return true;
        return false;
    }

    private static Message Clone(Message message)
    {
        var encoded = message.Encode();
        var bytes = new byte[encoded.Length];
        Buffer.BlockCopy(encoded.Buffer, encoded.Offset, bytes, 0, encoded.Length);
        var clone = Message.Decode(new ByteBuffer(bytes, 0, bytes.Length, bytes.Length));
        // Each subscription manages its own sequence-number namespace. If a topic-level
        // sequence number is stamped on the original (as happens for scheduled messages),
        // strip it from the clone so the subscription's MessageBuffer assigns its own.
        clone.MessageAnnotations?.Map.Remove(SequenceNumberAnnotation);
        return clone;
    }
}
