using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Configuration;
using Xunit;

public enum Transport { Local, Azure }

internal static class TestData
{
    public static TheoryData<Transport> Transports => new() { Transport.Local, Transport.Azure };

    public static readonly QueueOptions DefaultOptions = QueueOptions.Default;
    public static readonly QueueOptions FastOptions = new(TimeSpan.FromSeconds(5), 3);
}

internal sealed class TestQueue : IAsyncDisposable
{
    public required ServiceBusClient Client { get; init; }
    public required string Name { get; init; }
    public required Func<ValueTask> Cleanup { private get; init; }

    public static async Task<TestQueue> CreateAsync(Transport transport, QueueOptions options, CancellationToken ct)
    {
        var name = $"test-{Guid.NewGuid():N}";
        switch (transport)
        {
            case Transport.Local:
                LocalServer.EnsureStarted();
                LocalServer.Server.CreateQueue(name, options);
                return new TestQueue
                {
                    Client = new ServiceBusClient(AmqpServer.LocalConnectionString),
                    Name = name,
                    Cleanup = () => { LocalServer.Server.DeleteQueue(name); return ValueTask.CompletedTask; },
                };

            case Transport.Azure:
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
