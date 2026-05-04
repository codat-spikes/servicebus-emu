using Azure.Messaging.ServiceBus;
using Xunit;

// Regression guard for the AmqpServer.DeleteQueue / connection-close deadlock.
// Before the fix, one iteration of `schedule + cancel + drain receive + DeleteQueue`
// reliably hung on the synchronous UnregisterRequestProcessor call. See
// docs/DELETE_QUEUE_DEADLOCK.md for the full story.
public sealed class DeleteQueueDeadlockTests
{
    [Fact(Timeout = 30_000)]
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
}
