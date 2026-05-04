sealed record QueueOptions(TimeSpan LockDuration, int MaxDeliveryCount)
{
    public static readonly QueueOptions Default = new(TimeSpan.FromSeconds(30), 10);
}
