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

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task CorrelationFilter_RoutesByCorrelationId(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [
                ("sub-a", TestData.DefaultOptions, (RuleFilter?)new CorrelationRuleFilter(CorrelationId: "for-a")),
                ("sub-b", TestData.DefaultOptions, (RuleFilter?)new CorrelationRuleFilter(CorrelationId: "for-b")),
            ], ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        await sender.SendMessageAsync(new ServiceBusMessage("msg-a") { CorrelationId = "for-a" }, ct);
        await sender.SendMessageAsync(new ServiceBusMessage("msg-b") { CorrelationId = "for-b" }, ct);
        await sender.SendMessageAsync(new ServiceBusMessage("msg-other") { CorrelationId = "for-nobody" }, ct);

        await using var ra = fx.Client.CreateReceiver(fx.TopicName, "sub-a");
        var a = await ra.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(a);
        Assert.Equal("msg-a", a!.Body.ToString());
        await ra.CompleteMessageAsync(a, ct);
        var aExtra = await ra.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(aExtra);

        await using var rb = fx.Client.CreateReceiver(fx.TopicName, "sub-b");
        var b = await rb.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(b);
        Assert.Equal("msg-b", b!.Body.ToString());
        await rb.CompleteMessageAsync(b, ct);
        var bExtra = await rb.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(bExtra);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task CorrelationFilter_MatchesOnApplicationProperty(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [
                ("sub", TestData.DefaultOptions,
                    (RuleFilter?)new CorrelationRuleFilter(
                        Properties: new Dictionary<string, object> { ["region"] = "eu" })),
            ], ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        var match = new ServiceBusMessage("eu-msg");
        match.ApplicationProperties["region"] = "eu";
        var miss = new ServiceBusMessage("us-msg");
        miss.ApplicationProperties["region"] = "us";
        var noProp = new ServiceBusMessage("no-prop");
        await sender.SendMessageAsync(match, ct);
        await sender.SendMessageAsync(miss, ct);
        await sender.SendMessageAsync(noProp, ct);

        await using var receiver = fx.Client.CreateReceiver(fx.TopicName, "sub");
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(received);
        Assert.Equal("eu-msg", received!.Body.ToString());
        await receiver.CompleteMessageAsync(received, ct);

        var extra = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(extra);
    }

    // Smoke test that SQL rule filters are wired through Topic.Enqueue. The full
    // grammar is covered by SqlFilterTests; this just proves end-to-end dispatch
    // (and parity with Azure when the connection string is configured).
    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task SqlFilter_RoutesByPredicate(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [
                ("sub", TestData.DefaultOptions,
                    (RuleFilter?)new SqlRuleFilter("region IN ('eu', 'uk') AND tier = 'gold'")),
            ], ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        var match = new ServiceBusMessage("match");
        match.ApplicationProperties["region"] = "uk";
        match.ApplicationProperties["tier"] = "gold";
        var miss = new ServiceBusMessage("miss");
        miss.ApplicationProperties["region"] = "us";
        miss.ApplicationProperties["tier"] = "gold";
        await sender.SendMessageAsync(match, ct);
        await sender.SendMessageAsync(miss, ct);

        await using var receiver = fx.Client.CreateReceiver(fx.TopicName, "sub");
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(received);
        Assert.Equal("match", received!.Body.ToString());
        await receiver.CompleteMessageAsync(received, ct);

        var extra = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(extra);
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
