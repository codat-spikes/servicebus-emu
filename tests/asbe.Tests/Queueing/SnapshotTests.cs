using Azure.Messaging.ServiceBus;
using Xunit;

namespace Queueing;

// Local-only tests for the EntityRuntimeSnapshot / TopicRuntimeSnapshot scaffolding
// that the upcoming HTTP management plane will project into Atom-XML. Once the HTTP
// listener is in, parity will be re-asserted via ServiceBusAdministrationClient
// against both Local and Azure transports.
public sealed class SnapshotTests
{
    [Fact(Timeout = 30_000)]
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public async Task QueueSnapshot_ReflectsActiveDeadLetterAndScheduled()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(Transport.Local, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("active-1"), ct);
        await sender.SendMessageAsync(new ServiceBusMessage("active-2"), ct);
        await sender.ScheduleMessageAsync(
            new ServiceBusMessage("scheduled"),
            DateTimeOffset.UtcNow.AddMinutes(10),
            ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var toDeadLetter = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5), ct);
        Assert.NotNull(toDeadLetter);
        await receiver.DeadLetterMessageAsync(toDeadLetter!, cancellationToken: ct);

        Assert.True(LocalServer.Server.TryGetQueueSnapshot(fx.Name, out var snapshot));
        Assert.Equal(1, snapshot.ActiveMessageCount);
        Assert.Equal(1, snapshot.DeadLetterMessageCount);
        Assert.Equal(1, snapshot.ScheduledMessageCount);
        Assert.Equal(3, snapshot.TotalMessageCount);
        Assert.True(snapshot.CreatedAt <= snapshot.AccessedAt);
    }

    [Fact(Timeout = 30_000)]
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public async Task TopicSnapshot_ReportsSubscriptionCountAndScheduled()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(
            Transport.Local,
            [("sub-a", TestData.DefaultOptions), ("sub-b", TestData.DefaultOptions)],
            ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        await sender.ScheduleMessageAsync(
            new ServiceBusMessage("future"),
            DateTimeOffset.UtcNow.AddMinutes(10),
            ct);

        Assert.True(LocalServer.Server.TryGetTopicSnapshot(fx.TopicName, out var snapshot));
        Assert.Equal(2, snapshot.SubscriptionCount);
        Assert.Equal(1, snapshot.ScheduledMessageCount);
    }

    [Fact(Timeout = 30_000)]
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public void StrictLookups_ReturnFalseForUnknownEntity()
    {
        LocalServer.EnsureStarted();
        Assert.False(LocalServer.Server.TryGetQueueSnapshot("not-a-queue-" + Guid.NewGuid().ToString("N"), out _));
        Assert.False(LocalServer.Server.TryGetTopicSnapshot("not-a-topic-" + Guid.NewGuid().ToString("N"), out _));
    }
}
