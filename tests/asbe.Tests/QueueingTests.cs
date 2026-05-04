using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Configuration;
using Xunit;

public sealed class QueueingTests
{
    public enum Transport { Local, Azure }

    public static TheoryData<Transport> Transports => new() { Transport.Local, Transport.Azure };

    private static readonly QueueOptions DefaultOptions = QueueOptions.Default;
    private static readonly QueueOptions FastOptions = new(TimeSpan.FromSeconds(5), 3);

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task SendAndReceive_RoundTripsMessageBody(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("Hello world"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
        });
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);

        Assert.Equal("Hello world", msg?.Body.ToString());
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task PeekLock_Complete_RemovesMessage(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("complete-me"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(msg);
        Assert.Equal("complete-me", msg!.Body.ToString());
        await receiver.CompleteMessageAsync(msg, ct);

        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(second);
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task PeekLock_Abandon_BumpsDeliveryCount(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("count-me"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(first);
        Assert.Equal(1, first!.DeliveryCount);
        await receiver.AbandonMessageAsync(first, cancellationToken: ct);

        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(second);
        Assert.Equal("count-me", second!.Body.ToString());
        Assert.Equal(2, second.DeliveryCount);
        await receiver.CompleteMessageAsync(second, ct);
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task PeekLock_LockExpires_RedeliversWithBumpedDeliveryCount(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, FastOptions, ct);

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

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task PeekLock_RenewLock_ExtendsLock(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, FastOptions, ct);

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

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task MaxDeliveryCount_RoutesMessageToDeadLetterQueue(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, FastOptions, ct);

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

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task DeadLetterQueue_ReceivesExplicitlyDeadLetteredMessages(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("explicit-dlq"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(msg);
        await receiver.DeadLetterMessageAsync(msg!, "MyReason", "my description", ct);

        await using var dlq = fx.Client.CreateReceiver(fx.Name, new ServiceBusReceiverOptions
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

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task PeekLock_DeadLetter_RemovesFromMainQueue(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("dead-letter-me"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(msg);
        await receiver.DeadLetterMessageAsync(msg!, cancellationToken: ct);

        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(second);
    }
}

internal sealed class TestQueue : IAsyncDisposable
{
    public required ServiceBusClient Client { get; init; }
    public required string Name { get; init; }
    public required Func<ValueTask> Cleanup { private get; init; }

    public static async Task<TestQueue> CreateAsync(QueueingTests.Transport transport, QueueOptions options, CancellationToken ct)
    {
        var name = $"test-{Guid.NewGuid():N}";
        switch (transport)
        {
            case QueueingTests.Transport.Local:
                LocalServer.EnsureStarted();
                LocalServer.Server.CreateQueue(name, options);
                return new TestQueue
                {
                    Client = new ServiceBusClient(AmqpServer.LocalConnectionString),
                    Name = name,
                    Cleanup = () => { LocalServer.Server.DeleteQueue(name); return ValueTask.CompletedTask; },
                };

            case QueueingTests.Transport.Azure:
                var conn = TestConfig.Value["ServiceBus:ConnectionString"];
                Assert.SkipWhen(string.IsNullOrWhiteSpace(conn), "ServiceBus:ConnectionString not set; run `task sb:up` and put the connection string in tests/asbe.Tests/appsettings.test.json (see appsettings.test.example.json).");
                var admin = new ServiceBusAdministrationClient(conn);
                await admin.CreateQueueAsync(new CreateQueueOptions(name)
                {
                    LockDuration = options.LockDuration,
                    MaxDeliveryCount = options.MaxDeliveryCount,
                }, ct);
                return new TestQueue
                {
                    Client = new ServiceBusClient(conn!),
                    Name = name,
                    Cleanup = async () => { await admin.DeleteQueueAsync(name, CancellationToken.None); },
                };

            default:
                throw new ArgumentOutOfRangeException(nameof(transport));
        }
    }

    public async ValueTask DisposeAsync()
    {
        await Client.DisposeAsync();
        await Cleanup();
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

    public static AmqpServer Server => _server ?? throw new InvalidOperationException("LocalServer not started.");

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
