namespace Aspire.Hosting;

// Fluent builders. These wrap the underlying mutable record so callers can
// chain `.WithLockDuration(...)` etc. without us exposing setters that mutate
// after AppHost startup — the lifecycle hook reads the snapshot once.
public sealed class AsbeQueueBuilder
{
    internal AsbeQueue Queue { get; }
    internal AsbeQueueBuilder(AsbeQueue queue) { Queue = queue; }

    public AsbeQueueBuilder WithLockDuration(TimeSpan duration) { Queue.LockDuration = duration; return this; }
    public AsbeQueueBuilder WithMaxDeliveryCount(int count) { Queue.MaxDeliveryCount = count; return this; }
    public AsbeQueueBuilder RequiresSession() { Queue.RequiresSession = true; return this; }
    public AsbeQueueBuilder WithDefaultMessageTimeToLive(TimeSpan ttl) { Queue.DefaultMessageTimeToLive = ttl; return this; }
    public AsbeQueueBuilder WithDeadLetteringOnMessageExpiration() { Queue.DeadLetteringOnMessageExpiration = true; return this; }
    public AsbeQueueBuilder WithDuplicateDetection(TimeSpan? historyWindow = null)
    {
        Queue.RequiresDuplicateDetection = true;
        Queue.DuplicateDetectionHistoryTimeWindow = historyWindow;
        return this;
    }
    public AsbeQueueBuilder ForwardTo(string queueOrTopic) { Queue.ForwardTo = queueOrTopic; return this; }
    public AsbeQueueBuilder ForwardDeadLetteredMessagesTo(string queueOrTopic) { Queue.ForwardDeadLetteredMessagesTo = queueOrTopic; return this; }
}

public sealed class AsbeTopicBuilder
{
    internal AsbeTopic Topic { get; }
    internal AsbeTopicBuilder(AsbeTopic topic) { Topic = topic; }

    public AsbeTopicBuilder WithDuplicateDetection(TimeSpan? historyWindow = null)
    {
        Topic.RequiresDuplicateDetection = true;
        Topic.DuplicateDetectionHistoryTimeWindow = historyWindow;
        return this;
    }

    public AsbeSubscriptionBuilder AddSubscription(string name, Action<AsbeSubscriptionBuilder>? configure = null)
    {
        var existing = Topic.Subscriptions.Find(s => s.Name == name);
        var sub = existing ?? new AsbeSubscription(name);
        if (existing is null) Topic.Subscriptions.Add(sub);
        var builder = new AsbeSubscriptionBuilder(sub);
        configure?.Invoke(builder);
        return builder;
    }
}

public sealed class AsbeSubscriptionBuilder
{
    internal AsbeSubscription Subscription { get; }
    internal AsbeSubscriptionBuilder(AsbeSubscription subscription) { Subscription = subscription; }

    public AsbeSubscriptionBuilder WithLockDuration(TimeSpan duration) { Subscription.LockDuration = duration; return this; }
    public AsbeSubscriptionBuilder WithMaxDeliveryCount(int count) { Subscription.MaxDeliveryCount = count; return this; }
    public AsbeSubscriptionBuilder RequiresSession() { Subscription.RequiresSession = true; return this; }
    public AsbeSubscriptionBuilder WithDefaultMessageTimeToLive(TimeSpan ttl) { Subscription.DefaultMessageTimeToLive = ttl; return this; }
    public AsbeSubscriptionBuilder WithDeadLetteringOnMessageExpiration() { Subscription.DeadLetteringOnMessageExpiration = true; return this; }
    public AsbeSubscriptionBuilder ForwardTo(string queueOrTopic) { Subscription.ForwardTo = queueOrTopic; return this; }
    public AsbeSubscriptionBuilder ForwardDeadLetteredMessagesTo(string queueOrTopic) { Subscription.ForwardDeadLetteredMessagesTo = queueOrTopic; return this; }
    public AsbeSubscriptionBuilder WithSqlFilter(string expression, string ruleName = "$Default")
    {
        Subscription.Rules.Add(new AsbeRule { Name = ruleName, SqlFilter = expression });
        return this;
    }
    public AsbeSubscriptionBuilder WithTrueFilter(string ruleName = "$Default")
    {
        Subscription.Rules.Add(new AsbeRule { Name = ruleName, TrueFilter = true });
        return this;
    }
}
