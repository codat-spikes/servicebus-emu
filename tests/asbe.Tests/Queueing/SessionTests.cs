using Azure.Messaging.ServiceBus;
using Xunit;

namespace Queueing;

public sealed class SessionTests
{
    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task SessionReceiver_OnlySeesItsOwnSessionMessages(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.SessionOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("a-1") { SessionId = "A" }, ct);
        await sender.SendMessageAsync(new ServiceBusMessage("b-1") { SessionId = "B" }, ct);
        await sender.SendMessageAsync(new ServiceBusMessage("a-2") { SessionId = "A" }, ct);

        await using var receiverA = await fx.Client.AcceptSessionAsync(fx.Name, "A", cancellationToken: ct);
        var a1 = await receiverA.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        var a2 = await receiverA.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(a1);
        Assert.NotNull(a2);
        Assert.Equal("a-1", a1!.Body.ToString());
        Assert.Equal("a-2", a2!.Body.ToString());
        Assert.Equal("A", a1.SessionId);
        Assert.Equal("A", a2.SessionId);
        await receiverA.CompleteMessageAsync(a1, ct);
        await receiverA.CompleteMessageAsync(a2, ct);

        await using var receiverB = await fx.Client.AcceptSessionAsync(fx.Name, "B", cancellationToken: ct);
        var b1 = await receiverB.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(b1);
        Assert.Equal("b-1", b1!.Body.ToString());
        Assert.Equal("B", b1.SessionId);
        await receiverB.CompleteMessageAsync(b1, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task SessionState_RoundTrips(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.SessionOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("hello") { SessionId = "S1" }, ct);

        await using var receiver = await fx.Client.AcceptSessionAsync(fx.Name, "S1", cancellationToken: ct);
        var initial = await receiver.GetSessionStateAsync(ct);
        Assert.True(initial is null || initial.ToArray().Length == 0);

        await receiver.SetSessionStateAsync(BinaryData.FromBytes([1, 2, 3, 4]), ct);
        var loaded = await receiver.GetSessionStateAsync(ct);
        Assert.Equal([1, 2, 3, 4], loaded!.ToArray());
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Timing")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task SessionLock_CanBeRenewed(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.FastSessionOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("renew") { SessionId = "R" }, ct);

        await using var receiver = await fx.Client.AcceptSessionAsync(fx.Name, "R", cancellationToken: ct);
        var before = receiver.SessionLockedUntil;
        await Task.Delay(TimeSpan.FromSeconds(2), ct);
        await receiver.RenewSessionLockAsync(ct);
        Assert.True(receiver.SessionLockedUntil > before);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Edge")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task SessionLock_HeldByOneClient_BlocksAcquireFromAnotherClient(Transport transport)
    {
        // Multi-connection parity: two clients race for the same session id. The first
        // wins; the second's accept must fail (Service Bus surfaces this as a
        // ServiceBusException with Reason=SessionCannotBeLocked).
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.SessionOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("contended") { SessionId = "S" }, ct);

        await using var clientA = fx.NewClient();
        await using var clientB = fx.NewClient();

        await using var holder = await clientA.AcceptSessionAsync(fx.Name, "S", cancellationToken: ct);
        Assert.Equal("S", holder.SessionId);

        var ex = await Assert.ThrowsAsync<ServiceBusException>(async () =>
            await clientB.AcceptSessionAsync(fx.Name, "S", cancellationToken: ct));
        Assert.Equal(ServiceBusFailureReason.SessionCannotBeLocked, ex.Reason);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Edge")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task SessionLock_AfterFirstClientReleases_SecondClientCanAcquire(Transport transport)
    {
        // Multi-connection parity: once the first client detaches, the session lock is
        // released and a second client on a different connection can pick it up.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.SessionOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("hand-off") { SessionId = "S" }, ct);

        await using var clientA = fx.NewClient();
        var first = await clientA.AcceptSessionAsync(fx.Name, "S", cancellationToken: ct);
        await first.DisposeAsync();

        await using var clientB = fx.NewClient();
        await using var second = await clientB.AcceptSessionAsync(fx.Name, "S", cancellationToken: ct);
        Assert.Equal("S", second.SessionId);
        var msg = await second.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(msg);
        Assert.Equal("hand-off", msg!.Body.ToString());
        await second.CompleteMessageAsync(msg, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Edge")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task NextAvailableSession_PicksAnUnlockedSession(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestQueue.CreateAsync(transport, TestData.SessionOptions, ct);

        await using var sender = fx.Client.CreateSender(fx.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("only-msg") { SessionId = "ONLY" }, ct);

        await using var receiver = await fx.Client.AcceptNextSessionAsync(fx.Name, cancellationToken: ct);
        Assert.Equal("ONLY", receiver.SessionId);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(msg);
        Assert.Equal("only-msg", msg!.Body.ToString());
        await receiver.CompleteMessageAsync(msg, ct);
    }
}
