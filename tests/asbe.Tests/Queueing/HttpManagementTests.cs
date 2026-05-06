using Azure.Messaging.ServiceBus;
using Xunit;

namespace Queueing;

// Parity tests for the HTTP Atom-XML management plane. These exercise
// ServiceBusAdministrationClient against the local listener via LocalAdmin's custom
// transport. The Azure transport leg isn't included for these — the connection-string
// flow already covers Azure, and replicating the local URI-rewrite shim against an
// Azure namespace would gain nothing.
public sealed class HttpManagementTests
{
    [Fact(Timeout = 30_000)]
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public async Task GetQueueRuntimeProperties_ReflectsCounts()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(Transport.Local, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("a"), ct);
        await sender.SendMessageAsync(new ServiceBusMessage("b"), ct);
        await sender.ScheduleMessageAsync(new ServiceBusMessage("scheduled"), DateTimeOffset.UtcNow.AddMinutes(10), ct);

        var admin = LocalAdmin.CreateClient();
        var props = await admin.GetQueueRuntimePropertiesAsync(fx.Name, ct);

        Assert.Equal(fx.Name, props.Value.Name);
        Assert.Equal(2, props.Value.ActiveMessageCount);
        Assert.Equal(1, props.Value.ScheduledMessageCount);
        Assert.Equal(0, props.Value.DeadLetterMessageCount);
        Assert.Equal(3, props.Value.TotalMessageCount);
    }

    [Fact(Timeout = 30_000)]
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public async Task GetQueueAsync_ReturnsConfiguredOptions()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(Transport.Local, TestData.FastOptions, ct);

        var admin = LocalAdmin.CreateClient();
        var queue = await admin.GetQueueAsync(fx.Name, ct);

        Assert.Equal(fx.Name, queue.Value.Name);
        Assert.Equal(TestData.FastOptions.LockDuration, queue.Value.LockDuration);
        Assert.Equal(TestData.FastOptions.MaxDeliveryCount, queue.Value.MaxDeliveryCount);
    }

    [Fact(Timeout = 30_000)]
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public async Task GetQueueRuntimeProperties_UnknownQueue_Throws404()
    {
        var ct = TestContext.Current.CancellationToken;
        LocalServer.EnsureStarted();
        var admin = LocalAdmin.CreateClient();

        var ex = await Assert.ThrowsAsync<ServiceBusException>(async () =>
            await admin.GetQueueRuntimePropertiesAsync("not-a-queue-" + Guid.NewGuid().ToString("N"), ct));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, ex.Reason);
    }

    [Fact(Timeout = 30_000)]
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public async Task GetQueuesAsync_ListsAllQueues()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx1 = await TestQueue.CreateAsync(Transport.Local, TestData.DefaultOptions, ct);
        await using var fx2 = await TestQueue.CreateAsync(Transport.Local, TestData.DefaultOptions, ct);

        var admin = LocalAdmin.CreateClient();
        var names = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var q in admin.GetQueuesAsync(ct))
            names.Add(q.Name);

        Assert.Contains(fx1.Name, names);
        Assert.Contains(fx2.Name, names);
    }

    [Fact(Timeout = 30_000)]
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public async Task GetTopicRuntimeProperties_ReportsSubscriptionCount()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(
            Transport.Local,
            [("sub-a", TestData.DefaultOptions), ("sub-b", TestData.DefaultOptions)],
            ct);

        var admin = LocalAdmin.CreateClient();
        var props = await admin.GetTopicRuntimePropertiesAsync(fx.TopicName, ct);

        Assert.Equal(fx.TopicName, props.Value.Name);
        Assert.Equal(2, props.Value.SubscriptionCount);
    }

    [Fact(Timeout = 30_000)]
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public async Task GetSubscriptionRuntimeProperties_ReflectsCounts()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(
            Transport.Local,
            [("sub", TestData.DefaultOptions)],
            ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        await sender.SendMessageAsync(new ServiceBusMessage("topic-msg"), ct);

        var admin = LocalAdmin.CreateClient();
        var props = await admin.GetSubscriptionRuntimePropertiesAsync(fx.TopicName, "sub", ct);

        Assert.Equal("sub", props.Value.SubscriptionName);
        Assert.Equal(fx.TopicName, props.Value.TopicName);
        Assert.Equal(1, props.Value.ActiveMessageCount);
    }
}
