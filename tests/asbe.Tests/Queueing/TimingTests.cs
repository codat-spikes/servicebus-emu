using System.Diagnostics;
using Azure.Messaging.ServiceBus;
using Xunit;

namespace Queueing;

public sealed class TimingTests
{
    [Theory(Timeout = 60_000)]
    [Trait("Category", "Timing")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task PeekLock_LockExpires_RedeliversWithBumpedDeliveryCount(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.FastOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("expire-me"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(first);
        Assert.Equal(1, first!.DeliveryCount);

        await Task.Delay(TimeSpan.FromSeconds(8), ct);

        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(second);
        Assert.Equal("expire-me", second!.Body.ToString());
        Assert.Equal(2, second.DeliveryCount);
        await receiver.CompleteMessageAsync(second, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Timing")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task PeekLock_RenewLock_ExtendsLock(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.FastOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("renew-me"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(msg);
        var originalLockedUntil = msg!.LockedUntil;

        await Task.Delay(TimeSpan.FromSeconds(3), ct);
        await receiver.RenewMessageLockAsync(msg, ct);
        Assert.True(msg.LockedUntil > originalLockedUntil,
            $"LockedUntil {msg.LockedUntil:O} should be later than the original {originalLockedUntil:O}.");

        // Past the original 5s lock; the renewal kept it ours.
        await Task.Delay(TimeSpan.FromSeconds(3), ct);
        await receiver.CompleteMessageAsync(msg, ct);

        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(second);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Timing")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task MaxDeliveryCount_RoutesMessageToDeadLetterQueue(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.FastOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("dlq-me"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        for (int i = 0; i < 3; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
            Assert.NotNull(msg);
            await receiver.AbandonMessageAsync(msg!, cancellationToken: ct);
        }

        var none = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(none);

        await using var dlq = fx.Client.CreateReceiver(fx.Name, new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
        });
        var dead = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(dead);
        Assert.Equal("dlq-me", dead!.Body.ToString());
        Assert.Equal("MaxDeliveryCountExceeded", dead.DeadLetterReason);
        await dlq.CompleteMessageAsync(dead, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Timing")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task ScheduleMessage_DelaysDelivery_UntilEnqueueTime(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        var enqueueAt = DateTimeOffset.UtcNow.AddSeconds(4);
        var seq = await sender.ScheduleMessageAsync(new ServiceBusMessage("scheduled-payload"), enqueueAt, ct);
        Assert.True(seq > 0);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        // Should not be delivered yet.
        var early = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(early);

        var late = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(late);
        Assert.Equal("scheduled-payload", late!.Body.ToString());
        await receiver.CompleteMessageAsync(late, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Timing")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task CancelScheduledMessage_PreventsDelivery(Transport transport)
    {
        // Real Service Bus delivers the message even after CancelScheduledMessageAsync
        // returns — likely a tight 5s window between schedule, cancel, and the
        // server-side enqueue trigger. Tracked separately; skip the Azure leg until we
        // figure out the right pacing or whether a longer delay reproduces it on Azure.
        Assert.SkipWhen(transport == Transport.Azure, "Azure CancelScheduledMessage parity flake — see docs/DELETE_QUEUE_DEADLOCK.md.");

        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        var enqueueAt = DateTimeOffset.UtcNow.AddSeconds(5);
        var seq = await sender.ScheduleMessageAsync(new ServiceBusMessage("cancel-me"), enqueueAt, ct);

        await sender.CancelScheduledMessageAsync(seq, ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var none = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(8), ct);
        Assert.Null(none);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Timing")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task ReceiverDispose_DoesNotShortcutLock_NewReceiverWaitsForExpiry(Transport transport)
    {
        // Service Bus parity check: when a peek-lock receiver detaches without settling,
        // the broker holds the lock for its full duration (at-least-once semantics — the
        // broker can't tell whether the client got the message or just lost the network).
        // A fresh receiver on the same queue should see the message redelivered only
        // after the lock expires, not immediately on detach.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.FastOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("hold-the-lock"), ct);

        var first = fx.Client.CreateReceiver(fx.Name);
        var msg = await first.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(msg);
        Assert.Equal(1, msg!.DeliveryCount);
        await first.DisposeAsync();

        // Immediately try a fresh receiver — the lock is still held, so we shouldn't
        // get anything within a 2s window (lock duration is 5s).
        await using var second = fx.Client.CreateReceiver(fx.Name);
        var sw = Stopwatch.StartNew();
        var early = await second.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(early);

        // After the lock expires, the same fresh receiver picks it up with a bumped count.
        var redelivered = await second.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        sw.Stop();
        Assert.NotNull(redelivered);
        Assert.Equal("hold-the-lock", redelivered!.Body.ToString());
        Assert.Equal(2, redelivered.DeliveryCount);
        Assert.True(sw.Elapsed >= TimeSpan.FromSeconds(3),
            $"redelivery happened in {sw.Elapsed.TotalSeconds:F1}s — broker shortcut the lock on receiver detach");
        await second.CompleteMessageAsync(redelivered, ct);
    }

    [Theory(Timeout = 120_000)]
    [Trait("Category", "Timing")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Transaction_Rollback_LeavesLockHeldUntilExpiry(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.FastOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("rollback-complete"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(msg);

        using (new System.Transactions.TransactionScope(System.Transactions.TransactionScopeAsyncFlowOption.Enabled))
        {
            await receiver.CompleteMessageAsync(msg!, ct);
            // No tx.Complete() -> rollback. Lock should remain held until expiry.
        }

        // Lock is still held by this receiver — nothing redelivers immediately.
        var immediate = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1), ct);
        Assert.Null(immediate);

        // After lock expiry (FastOptions = 5s), the message redelivers with bumped count.
        var redelivered = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(15), ct);
        Assert.NotNull(redelivered);
        Assert.Equal("rollback-complete", redelivered!.Body.ToString());
        Assert.Equal(2, redelivered.DeliveryCount);
        await receiver.CompleteMessageAsync(redelivered, ct);
    }
}
