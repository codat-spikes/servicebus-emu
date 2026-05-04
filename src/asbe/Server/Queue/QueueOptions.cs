sealed record QueueOptions(TimeSpan LockDuration, int MaxDeliveryCount, bool RequiresSession = false)
{
    public static readonly QueueOptions Default = new(TimeSpan.FromSeconds(30), 10);
}
