// Immutable point-in-time runtime snapshots for entities in the broker. The HTTP
// management plane reads these to populate Service Bus QueueRuntimeProperties /
// TopicRuntimeProperties / SubscriptionRuntimeProperties responses without reaching
// across substore boundaries.
//
// Counts are read from the per-store synchronisation each owns (Channel / lock /
// ConcurrentDictionary). A snapshot is not a transactional view across stores —
// counts can drift while a snapshot is being assembled — but each individual count
// is consistent. That matches Azure's documented "approximate" semantics for
// MessageCountDetails.

readonly record struct EntityRuntimeSnapshot(
    long ActiveMessageCount,
    long DeadLetterMessageCount,
    long ScheduledMessageCount,
    long TransferMessageCount,
    long TransferDeadLetterMessageCount,
    long TotalMessageCount,
    long SizeInBytes,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    DateTimeOffset AccessedAt);

readonly record struct TopicRuntimeSnapshot(
    long ScheduledMessageCount,
    long SizeInBytes,
    int SubscriptionCount,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    DateTimeOffset AccessedAt);
