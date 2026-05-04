using Amqp;

interface IQueueEndpoint
{
    void Enqueue(Message message);
    Task<Delivery> DequeueAsync(CancellationToken cancellation);
    void Complete(long deliveryId);
    void Abandon(long deliveryId);
    void Reject(long deliveryId, DeadLetterInfo info);
    bool TryRenewLock(Guid lockToken, out DateTime expiresAt);
}
