using Azure.Messaging.ServiceBus;
using Xunit;

namespace Queueing;

public sealed class EdgeTests
{
    // Regression guard for the AmqpServer.DeleteQueue / connection-close deadlock.
    // Before the fix, one iteration of `schedule + cancel + drain receive + DeleteQueue`
    // reliably hung on the synchronous UnregisterRequestProcessor call. See
    // docs/DELETE_QUEUE_DEADLOCK.md for the full story.
    [Fact(Timeout = 30_000)]
    [Trait("Category", "Edge")]
    public async Task ScheduleCancelDrainDelete_DoesNotDeadlock()
    {
        var ct = TestContext.Current.CancellationToken;
        LocalServer.EnsureStarted();

        for (var i = 0; i < 5; i++)
        {
            var name = $"deadlock-repro-{Guid.NewGuid():N}";
            LocalServer.Server.CreateQueue(name, QueueOptions.Default);
            await using (var client = new ServiceBusClient(AmqpServer.LocalConnectionString))
            await using (var sender = client.CreateSender(name))
            await using (var receiver = client.CreateReceiver(name))
            {
                var seq = await sender.ScheduleMessageAsync(new ServiceBusMessage("x"), DateTimeOffset.UtcNow.AddSeconds(30), ct);
                await sender.CancelScheduledMessageAsync(seq, ct);
                var none = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1), ct);
                Assert.Null(none);
            }
            LocalServer.Server.DeleteQueue(name);
        }
    }

    // The broker advertises MaxMessageSize on attach. The SDK's ServiceBusMessageBatch
    // reads that ceiling and refuses TryAddMessage once the accumulated payload would
    // exceed it. If we forget to advertise (or advertise an absurdly large value), the
    // SDK silently accepts oversized batches and the parity with Azure breaks.
    [Theory(Timeout = 60_000)]
    [Trait("Category", "Edge")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task SendBatch_TryAddMessage_RefusesOnceMaxBatchSizeReached(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        using var batch = await sender.CreateMessageBatchAsync(ct);

        // Each message ~64KB body. Service Bus Basic/Standard caps a batch at 256KB,
        // so after a handful of these TryAddMessage must return false rather than
        // letting us submit an oversized batch.
        var payload = new byte[64 * 1024];
        var added = 0;
        while (batch.TryAddMessage(new ServiceBusMessage(payload)))
        {
            added++;
            if (added > 64) break; // safety net; real cap is much lower
        }

        Assert.InRange(added, 1, 16);
        Assert.True(batch.SizeInBytes <= 1024 * 1024, $"batch ballooned to {batch.SizeInBytes} bytes — broker may not be advertising MaxMessageSize");
        await sender.SendMessagesAsync(batch, ct);
    }
}
