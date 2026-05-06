using Azure.Messaging.ServiceBus;
using Xunit;

namespace Queueing;

public sealed class TtlTests
{
    [Theory(Timeout = 60_000)]
    [Trait("Category", "Timing")]
    [Trait("Speed", "Slow")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task MessageTimeToLive_Expires_AndDeadLetters(Transport transport)
    {
        // Idle-expire: message sits in the ready buffer until TTL fires the eviction
        // timer, then DLQs. Azure has a periodic background scanner (~minutes) so the
        // wait would have to be much longer to be reliable on the real broker — the
        // peek-lock-then-TTL test below covers Azure DLQ parity via the fast path.
        Assert.SkipWhen(transport == Transport.Azure,
            "Azure's TTL sweep runs on its own cadence; PeekLockedMessage_PastTtl_DeadLettersOnLockExpiry covers parity.");

        var ct = TestContext.Current.CancellationToken;
        var options = new QueueOptions(TimeSpan.FromSeconds(30), 5, DeadLetteringOnMessageExpiration: true);
        await using var fx = await TestQueue.CreateAsync(transport, options, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("ttl-expire-me")
        {
            TimeToLive = TimeSpan.FromSeconds(3),
        }, ct);

        await using var dlq = fx.Client.CreateReceiver(fx.Name, new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
        });
        var dead = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(30), ct);
        Assert.NotNull(dead);
        Assert.Equal("ttl-expire-me", dead!.Body.ToString());
        Assert.Equal("TTLExpiredException", dead.DeadLetterReason);
        await dlq.CompleteMessageAsync(dead, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Timing")]
    [Trait("Speed", "Slow")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task DefaultMessageTimeToLive_AppliesToMessagesWithoutOwnTtl(Transport transport)
    {
        // Same idle-expire path as above; skip Azure for the same reason.
        Assert.SkipWhen(transport == Transport.Azure,
            "Azure's TTL sweep runs on its own cadence; PeekLockedMessage_PastTtl_DeadLettersOnLockExpiry covers parity.");

        var ct = TestContext.Current.CancellationToken;
        var options = new QueueOptions(
            LockDuration: TimeSpan.FromSeconds(30),
            MaxDeliveryCount: 5,
            DefaultMessageTimeToLive: TimeSpan.FromSeconds(3),
            DeadLetteringOnMessageExpiration: true);
        await using var fx = await TestQueue.CreateAsync(transport, options, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("default-ttl-expire-me"), ct);

        await using var dlq = fx.Client.CreateReceiver(fx.Name, new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
        });
        var dead = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(30), ct);
        Assert.NotNull(dead);
        Assert.Equal("default-ttl-expire-me", dead!.Body.ToString());
        Assert.Equal("TTLExpiredException", dead.DeadLetterReason);
        await dlq.CompleteMessageAsync(dead, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Timing")]
    [Trait("Speed", "Slow")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task PeekLockedMessage_PastTtl_DeadLettersOnLockExpiry(Transport transport)
    {
        // While peek-locked, TTL doesn't yank the message out from under the receiver.
        // Once the lock expires, an expired message goes to DLQ instead of being requeued.
        // Azure DLQs on the same lock-release path, so this case runs on both transports.
        var ct = TestContext.Current.CancellationToken;
        var options = new QueueOptions(
            LockDuration: TimeSpan.FromSeconds(5),
            MaxDeliveryCount: 5,
            DeadLetteringOnMessageExpiration: true);
        await using var fx = await TestQueue.CreateAsync(transport, options, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("locked-then-expired")
        {
            TimeToLive = TimeSpan.FromSeconds(4),
        }, ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var locked = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(locked);
        Assert.Equal("locked-then-expired", locked!.Body.ToString());

        await using var dlq = fx.Client.CreateReceiver(fx.Name, new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
        });
        var dead = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(30), ct);
        Assert.NotNull(dead);
        Assert.Equal("locked-then-expired", dead!.Body.ToString());
        Assert.Equal("TTLExpiredException", dead.DeadLetterReason);
        await dlq.CompleteMessageAsync(dead, ct);
    }
}
