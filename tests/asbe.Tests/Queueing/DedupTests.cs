using Azure.Messaging.ServiceBus;
using Xunit;

namespace Queueing;

public sealed class DedupTests
{
    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task DuplicateMessageId_WithinWindow_IsDroppedSilently(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        var options = QueueOptions.Default with
        {
            RequiresDuplicateDetection = true,
            // Azure's minimum is well above the test runtime, so any duplicate sent
            // back-to-back stays inside the window.
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(1),
        };
        await using var fx = await TestQueue.CreateAsync(transport, options, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        var id = Guid.NewGuid().ToString("N");
        await sender.SendMessageAsync(new ServiceBusMessage("first") { MessageId = id }, ct);
        // Second send with the same MessageId — broker accepts the transfer but drops
        // the message. Sender sees no error.
        await sender.SendMessageAsync(new ServiceBusMessage("second") { MessageId = id }, ct);
        // A different MessageId should still go through.
        await sender.SendMessageAsync(new ServiceBusMessage("other") { MessageId = Guid.NewGuid().ToString("N") }, ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
        });
        var bodies = new List<string>();
        for (var i = 0; i < 2; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
            Assert.NotNull(msg);
            bodies.Add(msg!.Body.ToString());
        }
        // The duplicate should never have been enqueued; a third receive must time out.
        var extra = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(extra);

        Assert.Contains("first", bodies);
        Assert.Contains("other", bodies);
        Assert.DoesNotContain("second", bodies);
    }
}
