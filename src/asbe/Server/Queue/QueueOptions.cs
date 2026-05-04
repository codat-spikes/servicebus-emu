sealed record QueueOptions(
    TimeSpan LockDuration,
    int MaxDeliveryCount,
    bool RequiresSession = false,
    TimeSpan? DefaultMessageTimeToLive = null,
    bool DeadLetteringOnMessageExpiration = false,
    bool RequiresDuplicateDetection = false,
    TimeSpan? DuplicateDetectionHistoryTimeWindow = null)
{
    public static readonly QueueOptions Default = new(TimeSpan.FromSeconds(30), 10);
}

// Per-subscription configuration: queue knobs (lock duration, delivery count, sessions)
// plus an optional set of rules. A subscription with no rules is implicitly match-all,
// matching Service Bus's `$Default` rule.
sealed record SubscriptionOptions(QueueOptions Queue, IReadOnlyList<RuleFilter>? Rules = null)
{
    public static SubscriptionOptions Default { get; } = new(QueueOptions.Default);
}
