// Config-binding shape for the asbe server. Lives behind the `Asbe` section
// (e.g. `Asbe:Queues:0:Name=orders`) so it round-trips cleanly via env vars
// when the AppHost integration flattens its registry into the container.
sealed class AsbeOptions
{
    public List<QueueEntry> Queues { get; set; } = new();
    public List<TopicEntry> Topics { get; set; } = new();
}

sealed class QueueEntry
{
    public string Name { get; set; } = "";
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxDeliveryCount { get; set; } = 10;
    public bool RequiresSession { get; set; }
    public TimeSpan? DefaultMessageTimeToLive { get; set; }
    public bool DeadLetteringOnMessageExpiration { get; set; }
    public bool RequiresDuplicateDetection { get; set; }
    public TimeSpan? DuplicateDetectionHistoryTimeWindow { get; set; }
    public string? ForwardTo { get; set; }
    public string? ForwardDeadLetteredMessagesTo { get; set; }

    public QueueOptions ToOptions() => new(
        LockDuration,
        MaxDeliveryCount,
        RequiresSession,
        DefaultMessageTimeToLive,
        DeadLetteringOnMessageExpiration,
        RequiresDuplicateDetection,
        DuplicateDetectionHistoryTimeWindow,
        ForwardTo,
        ForwardDeadLetteredMessagesTo);
}

sealed class TopicEntry
{
    public string Name { get; set; } = "";
    public bool RequiresDuplicateDetection { get; set; }
    public TimeSpan? DuplicateDetectionHistoryTimeWindow { get; set; }
    public List<SubscriptionEntry> Subscriptions { get; set; } = new();
}

sealed class SubscriptionEntry
{
    public string Name { get; set; } = "";
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxDeliveryCount { get; set; } = 10;
    public bool RequiresSession { get; set; }
    public TimeSpan? DefaultMessageTimeToLive { get; set; }
    public bool DeadLetteringOnMessageExpiration { get; set; }
    public string? ForwardTo { get; set; }
    public string? ForwardDeadLetteredMessagesTo { get; set; }
    public List<RuleEntry> Rules { get; set; } = new();

    public QueueOptions ToQueueOptions() => new(
        LockDuration,
        MaxDeliveryCount,
        RequiresSession,
        DefaultMessageTimeToLive,
        DeadLetteringOnMessageExpiration,
        RequiresDuplicateDetection: false,
        DuplicateDetectionHistoryTimeWindow: null,
        ForwardTo,
        ForwardDeadLetteredMessagesTo);
}

sealed class RuleEntry
{
    public string Name { get; set; } = "$Default";
    public string? SqlFilter { get; set; }
    public bool? TrueFilter { get; set; }

    public RuleFilter? ToFilter()
    {
        if (SqlFilter is { Length: > 0 } sql) return new SqlRuleFilter(sql);
        if (TrueFilter is true) return RuleFilter.MatchAll;
        if (TrueFilter is false) return new FalseRuleFilter();
        return null;
    }
}
