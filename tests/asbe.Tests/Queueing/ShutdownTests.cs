using Xunit;

namespace Queueing;

// Local-only — the in-process server's lifecycle is what we care about here, so
// these don't need an Azure parity arm.
public sealed class ShutdownTests
{
    // Use a port distinct from the LocalServer singleton (5672) so these tests
    // can stand up and tear down their own AmqpServer without contention.
    private const int TestPort = 5673;

    [Fact(Timeout = 30_000)]
    [Trait("Category", "Edge")]
    public async Task DisposeAsync_FreesListenerPort()
    {
        await using (var server = new AmqpServer(TestPort))
            server.Start();

        // If the listener weren't released, the second Start would throw "address in use".
        await using var second = new AmqpServer(TestPort);
        second.Start();
    }

    [Fact(Timeout = 30_000)]
    [Trait("Category", "Edge")]
    public async Task DisposeAsync_IsIdempotent()
    {
        var server = new AmqpServer(TestPort);
        server.Start();
        await server.DisposeAsync();
        await server.DisposeAsync();
    }

    [Fact(Timeout = 30_000)]
    [Trait("Category", "Edge")]
    public async Task DisposeAsync_BeforeStart_DoesNotThrow()
    {
        var server = new AmqpServer(TestPort);
        await server.DisposeAsync();
    }

    [Fact(Timeout = 30_000)]
    [Trait("Category", "Edge")]
    public async Task DisposeAsync_WithQueuesAndTopics_DoesNotThrow()
    {
        await using var server = new AmqpServer(TestPort);
        server.Start();
        server.CreateQueue("q1", QueueOptions.Default);
        server.CreateQueue("q2", QueueOptions.Default);
        server.CreateTopic("t1", new TopicOptions(new Dictionary<string, SubscriptionOptions>
        {
            ["sub-a"] = new SubscriptionOptions(QueueOptions.Default),
            ["sub-b"] = new SubscriptionOptions(QueueOptions.Default),
        }));
    }

    [Fact(Timeout = 30_000)]
    [Trait("Category", "Edge")]
    public async Task Server_CanBeStartedAndDisposed_Repeatedly_OnSamePort()
    {
        for (int i = 0; i < 3; i++)
        {
            await using var server = new AmqpServer(TestPort);
            server.Start();
            server.CreateQueue($"q-{i}", QueueOptions.Default);
        }
    }
}
