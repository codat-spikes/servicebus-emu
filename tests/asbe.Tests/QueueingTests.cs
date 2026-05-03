using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Xunit;

public sealed class QueueingTests
{
    public enum Transport { Local, Azure }

    public static TheoryData<Transport> Transports => new() { Transport.Local, Transport.Azure };

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task SendAndReceive_RoundTripsMessageBody(Transport transport)
    {
        var (client, queue) = await Setup(transport);

        await using var sender = client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("Hello world"));

        await using var receiver = client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
        });
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));

        Assert.Equal("Hello world", msg?.Body.ToString());
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task PeekLock_Complete_RemovesMessage(Transport transport)
    {
        var (client, queue) = await Setup(transport);

        await using var sender = client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("complete-me"));

        await using var receiver = client.CreateReceiver(queue);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(msg);
        Assert.Equal("complete-me", msg!.Body.ToString());
        await receiver.CompleteMessageAsync(msg);

        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(second);
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task PeekLock_Abandon_Redelivers(Transport transport)
    {
        var (client, queue) = await Setup(transport);

        await using var sender = client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("abandon-me"));

        await using var receiver = client.CreateReceiver(queue);
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(first);
        await receiver.AbandonMessageAsync(first!);

        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(second);
        Assert.Equal("abandon-me", second!.Body.ToString());
        await receiver.CompleteMessageAsync(second);
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task PeekLock_DeadLetter_RemovesFromMainQueue(Transport transport)
    {
        var (client, queue) = await Setup(transport);

        await using var sender = client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("dead-letter-me"));

        await using var receiver = client.CreateReceiver(queue);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(msg);
        await receiver.DeadLetterMessageAsync(msg!);

        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(second);
    }

    private static async Task<(ServiceBusClient Client, string Queue)> Setup(Transport transport)
    {
        switch (transport)
        {
            case Transport.Local:
                LocalServer.EnsureStarted();
                return (new ServiceBusClient(AmqpServer.LocalConnectionString), $"test-queue-{Guid.NewGuid():N}");

            case Transport.Azure:
                var conn = TestConfig.Value["ServiceBus:ConnectionString"];
                Assert.SkipWhen(string.IsNullOrWhiteSpace(conn), "ServiceBus:ConnectionString not set; run `task sb:up` and put the connection string in tests/asbe.Tests/appsettings.test.json (see appsettings.test.example.json).");
                var queue = TestConfig.Value["ServiceBus:Queue"] ?? "test-queue";
                var client = new ServiceBusClient(conn!);
                await Drain(client, queue);
                return (client, queue);

            default:
                throw new ArgumentOutOfRangeException(nameof(transport));
        }
    }

    private static async Task Drain(ServiceBusClient client, string queue)
    {
        await using var receiver = client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
        });
        while (await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(200)) is not null) { }
    }
}

internal static class TestConfig
{
    public static readonly IConfiguration Value = new ConfigurationBuilder()
        .AddJsonFile("appsettings.test.json", optional: true, reloadOnChange: false)
        .Build();
}

internal static class LocalServer
{
    private static readonly Lock _gate = new();
    private static AmqpServer? _server;

    public static void EnsureStarted()
    {
        lock (_gate)
        {
            if (_server is not null) return;
            _server = new AmqpServer();
            _server.Start();
        }
    }
}
