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
    public async Task SendMessages_Batch_DeliversAllInOrder(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        var bodies = Enumerable.Range(0, 10).Select(i => $"batch-{i}").ToArray();
        await sender.SendMessagesAsync(bodies.Select(b => new ServiceBusMessage(b)), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
        });
        var received = new List<string>();
        for (var i = 0; i < bodies.Length; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
            Assert.NotNull(msg);
            received.Add(msg!.Body.ToString());
        }
        Assert.Equal(bodies, received);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task SendMessages_BatchViaCreateMessageBatch_DeliversAll(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        using var batch = await sender.CreateMessageBatchAsync(ct);
        var bodies = Enumerable.Range(0, 5).Select(i => $"mb-{i}").ToArray();
        foreach (var b in bodies)
        {
            Assert.True(batch.TryAddMessage(new ServiceBusMessage(b)));
        }
        await sender.SendMessagesAsync(batch, ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
        });
        var received = new List<string>();
        for (var i = 0; i < bodies.Length; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
            Assert.NotNull(msg);
            received.Add(msg!.Body.ToString());
        }
        Assert.Equal(bodies, received);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Peek_AfterComplete_SkipsCompletedMessages(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessagesAsync(new[]
        {
            new ServiceBusMessage("p-1"),
            new ServiceBusMessage("p-2"),
            new ServiceBusMessage("p-3"),
        }, ct);

        // Receive + complete the middle message.
        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.Equal("p-1", first!.Body.ToString());
        Assert.Equal("p-2", second!.Body.ToString());
        await receiver.CompleteMessageAsync(second!, ct);
        await receiver.AbandonMessageAsync(first!, cancellationToken: ct);

        await using var peeker = fx.Client.CreateReceiver(fx.Name);
        var peeked = await peeker.PeekMessagesAsync(maxMessages: 10, fromSequenceNumber: 0, cancellationToken: ct);
        Assert.Equal(["p-1", "p-3"], peeked.Select(m => m.Body.ToString()));
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Peek_FromSequenceNumberPastTail_ReturnsEmpty(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("only-one"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var first = await receiver.PeekMessageAsync(fromSequenceNumber: 0, cancellationToken: ct);
        Assert.NotNull(first);

        var beyond = await receiver.PeekMessagesAsync(maxMessages: 10, fromSequenceNumber: first!.SequenceNumber + 1000, cancellationToken: ct);
        Assert.Empty(beyond);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Peek_MaxMessages_RespectsLimit(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessagesAsync(Enumerable.Range(0, 5).Select(i => new ServiceBusMessage($"m-{i}")), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var peeked = await receiver.PeekMessagesAsync(maxMessages: 2, fromSequenceNumber: 0, cancellationToken: ct);
        Assert.Equal(2, peeked.Count);
        Assert.Equal(["m-0", "m-1"], peeked.Select(m => m.Body.ToString()));
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Peek_OnDeadLetterSubqueue_ReturnsDeadLetteredMessages(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessagesAsync(new[]
        {
            new ServiceBusMessage("dlq-a"),
            new ServiceBusMessage("dlq-b"),
        }, ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        for (var i = 0; i < 2; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
            Assert.NotNull(msg);
            await receiver.DeadLetterMessageAsync(msg!, cancellationToken: ct);
        }

        await using var dlqPeeker = fx.Client.CreateReceiver(fx.Name, new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
        });
        var peeked = await dlqPeeker.PeekMessagesAsync(maxMessages: 10, fromSequenceNumber: 0, cancellationToken: ct);
        Assert.Equal(2, peeked.Count);
        Assert.Equal(new HashSet<string> { "dlq-a", "dlq-b" }, peeked.Select(m => m.Body.ToString()).ToHashSet());
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

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Transaction_Commit_FlushesSends(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        using (var tx = new System.Transactions.TransactionScope(System.Transactions.TransactionScopeAsyncFlowOption.Enabled))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("tx-1"), ct);
            await sender.SendMessageAsync(new ServiceBusMessage("tx-2"), ct);
            tx.Complete();
        }

        await using var receiver = fx.Client.CreateReceiver(fx.Name, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
        });
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.Equal(new[] { "tx-1", "tx-2" }, new[] { first!.Body.ToString(), second!.Body.ToString() });
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Transaction_Rollback_DropsSends(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        using (new System.Transactions.TransactionScope(System.Transactions.TransactionScopeAsyncFlowOption.Enabled))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("rollback-me"), ct);
            // Intentionally do not Complete -> txn rolls back on dispose.
        }

        await using var receiver = fx.Client.CreateReceiver(fx.Name, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
        });
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(msg);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Transaction_Commit_AppliesComplete(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("complete-in-tx"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(msg);

        using (var tx = new System.Transactions.TransactionScope(System.Transactions.TransactionScopeAsyncFlowOption.Enabled))
        {
            await receiver.CompleteMessageAsync(msg!, ct);
            tx.Complete();
        }

        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(second);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Defer_RemovesFromReadyQueue_AndReceiveBySequenceNumber_ReturnsIt(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("defer-me"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(msg);
        var seq = msg!.SequenceNumber;
        await receiver.DeferMessageAsync(msg, cancellationToken: ct);

        var none = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(none);

        var deferred = await receiver.ReceiveDeferredMessageAsync(seq, ct);
        Assert.NotNull(deferred);
        Assert.Equal("defer-me", deferred!.Body.ToString());
        Assert.Equal(seq, deferred.SequenceNumber);

        await receiver.CompleteMessageAsync(deferred, ct);
        var stillNone = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(stillNone);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Defer_DeadLetter_MovesToDlq(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("defer-then-dlq"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(msg);
        var seq = msg!.SequenceNumber;
        await receiver.DeferMessageAsync(msg, cancellationToken: ct);

        var deferred = await receiver.ReceiveDeferredMessageAsync(seq, ct);
        Assert.NotNull(deferred);
        await receiver.DeadLetterMessageAsync(deferred!, "DeferredDeadLetter", "from deferred", ct);

        await using var dlq = fx.Client.CreateReceiver(fx.Name, new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
        });
        var dead = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(dead);
        Assert.Equal("defer-then-dlq", dead!.Body.ToString());
        Assert.Equal("DeferredDeadLetter", dead.DeadLetterReason);
        Assert.Equal("from deferred", dead.DeadLetterErrorDescription);
        await dlq.CompleteMessageAsync(dead, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task Defer_ReceiveMultipleBySequenceNumber_ReturnsAll(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessagesAsync(new[]
        {
            new ServiceBusMessage("a"),
            new ServiceBusMessage("b"),
            new ServiceBusMessage("c"),
        }, ct);

        await using var receiver = fx.Client.CreateReceiver(fx.Name);
        var seqs = new List<long>();
        for (var i = 0; i < 3; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
            Assert.NotNull(msg);
            seqs.Add(msg!.SequenceNumber);
            await receiver.DeferMessageAsync(msg, cancellationToken: ct);
        }

        var deferred = await receiver.ReceiveDeferredMessagesAsync(seqs, ct);
        Assert.Equal(3, deferred.Count);
        Assert.Equal(new HashSet<string> { "a", "b", "c" }, deferred.Select(m => m.Body.ToString()).ToHashSet());

        foreach (var d in deferred) await receiver.CompleteMessageAsync(d, ct);
    }
}
