using Azure.Messaging.ServiceBus;
using Xunit;

namespace Queueing;

public sealed class CoreTests
{
    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task SendAndReceive_RoundTripsMessageBody(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("Hello world"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
        });
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);

        Assert.Equal("Hello world", msg?.Body.ToString());
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task PeekLock_Complete_RemovesMessage(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

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

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task PeekLock_Abandon_BumpsDeliveryCount(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

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

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task DeadLetterQueue_ReceivesExplicitlyDeadLetteredMessages(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

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

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Peek_ReturnsMessagesWithSequenceNumbersWithoutConsuming(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("peek-1"), ct);
        await sender.SendMessageAsync(new ServiceBusMessage("peek-2"), ct);
        await sender.SendMessageAsync(new ServiceBusMessage("peek-3"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var peeked = await receiver.PeekMessagesAsync(maxMessages: 10, fromSequenceNumber: 0, cancellationToken: ct);

        Assert.Equal(3, peeked.Count);
        Assert.Equal(["peek-1", "peek-2", "peek-3"], peeked.Select(m => m.Body.ToString()));
        Assert.True(peeked[0].SequenceNumber < peeked[1].SequenceNumber);
        Assert.True(peeked[1].SequenceNumber < peeked[2].SequenceNumber);

        // Peek must not consume — a real receive should still see all three.
        var received = new List<string>();
        for (int i = 0; i < 3; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
            Assert.NotNull(msg);
            received.Add(msg!.Body.ToString());
            await receiver.CompleteMessageAsync(msg, ct);
        }
        Assert.Equal(["peek-1", "peek-2", "peek-3"], received);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Peek_FromSequenceNumber_SkipsEarlierMessages(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("a"), ct);
        await sender.SendMessageAsync(new ServiceBusMessage("b"), ct);
        await sender.SendMessageAsync(new ServiceBusMessage("c"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var first = await receiver.PeekMessageAsync(fromSequenceNumber: 0, cancellationToken: ct);
        Assert.NotNull(first);
        Assert.Equal("a", first!.Body.ToString());

        var rest = await receiver.PeekMessagesAsync(maxMessages: 10, fromSequenceNumber: first.SequenceNumber + 1, cancellationToken: ct);
        Assert.Equal(["b", "c"], rest.Select(m => m.Body.ToString()));
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task PeekLock_DeadLetter_RemovesFromMainQueue(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

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
