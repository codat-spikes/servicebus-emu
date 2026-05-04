using Azure.Messaging.ServiceBus;
using Xunit;

namespace Queueing;

public sealed class TopicTests
{
    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Send_FansOutToAllSubscriptions(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub-a", TestData.DefaultOptions), ("sub-b", TestData.DefaultOptions)], ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        await sender.SendMessageAsync(new ServiceBusMessage("fan-out"), ct);

        foreach (var sub in fx.SubscriptionNames)
        {
            await using var receiver = fx.Client.CreateReceiver(fx.TopicName, sub);
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
            Assert.NotNull(msg);
            Assert.Equal("fan-out", msg!.Body.ToString());
            await receiver.CompleteMessageAsync(msg, ct);
        }
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Subscription_DeadLetter_ReceivesExplicitlyDeadLetteredMessages(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub", TestData.DefaultOptions)], ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        await sender.SendMessageAsync(new ServiceBusMessage("explicit-dlq"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.TopicName, "sub");
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(msg);
        await receiver.DeadLetterMessageAsync(msg!, "MyReason", "my description", ct);

        await using var dlq = fx.Client.CreateReceiver(fx.TopicName, "sub", new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
        });
        var dead = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(dead);
        Assert.Equal("explicit-dlq", dead!.Body.ToString());
        Assert.Equal("MyReason", dead.DeadLetterReason);
        Assert.Equal("my description", dead.DeadLetterErrorDescription);
        await dlq.CompleteMessageAsync(dead, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Subscriptions_HaveIndependentSequenceNumbers(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub-a", TestData.DefaultOptions), ("sub-b", TestData.DefaultOptions)], ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        await sender.SendMessageAsync(new ServiceBusMessage("m1"), ct);
        await sender.SendMessageAsync(new ServiceBusMessage("m2"), ct);

        // Consume from sub-a only — sub-b's copies must remain intact (not just unconsumed
        // — also unmutated, since the SDK reads sequence numbers/lock state from annotations).
        await using var ra = fx.Client.CreateReceiver(fx.TopicName, "sub-a");
        var a1 = await ra.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        var a2 = await ra.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(a1); Assert.NotNull(a2);
        await ra.CompleteMessageAsync(a1!, ct);
        await ra.CompleteMessageAsync(a2!, ct);

        await using var rb = fx.Client.CreateReceiver(fx.TopicName, "sub-b");
        var b1 = await rb.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        var b2 = await rb.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(b1); Assert.NotNull(b2);
        Assert.Equal(["m1", "m2"], new[] { b1!.Body.ToString(), b2!.Body.ToString() });
        await rb.CompleteMessageAsync(b1, ct);
        await rb.CompleteMessageAsync(b2, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Timing")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task ScheduledMessage_FansOutAtScheduledTime(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub-a", TestData.DefaultOptions), ("sub-b", TestData.DefaultOptions)], ct);

        var scheduledFor = DateTimeOffset.UtcNow.AddSeconds(3);
        await using var sender = fx.Client.CreateSender(fx.TopicName);
        await sender.ScheduleMessageAsync(new ServiceBusMessage("scheduled"), scheduledFor, ct);

        foreach (var sub in fx.SubscriptionNames)
        {
            await using var receiver = fx.Client.CreateReceiver(fx.TopicName, sub);
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
            Assert.NotNull(msg);
            Assert.Equal("scheduled", msg!.Body.ToString());
            Assert.True(DateTimeOffset.UtcNow >= scheduledFor.AddMilliseconds(-500),
                $"message arrived before scheduled time on '{sub}'");
            await receiver.CompleteMessageAsync(msg, ct);
        }
    }

    [Fact(Timeout = 30_000)]
    [Trait("Category", "Edge")]
    public async Task Receiver_AttachToBareTopic_IsRejected()
    {
        // Local-only — the SDK won't normally let you build a receiver on a bare topic
        // address; we want to prove the server rejects it cleanly if a raw AMQPNetLite
        // client tries.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(Transport.Local,
            [("sub", TestData.DefaultOptions)], ct);

        await using var receiver = fx.Client.CreateReceiver(fx.TopicName);
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct));
        Assert.Contains("topic", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
