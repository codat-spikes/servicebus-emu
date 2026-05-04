using Azure.Messaging.ServiceBus;
using Xunit;

namespace Queueing;

public sealed class ForwardingTests
{
    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task ForwardTo_RoutesMessagesToTargetQueue(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var target = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);
        var sourceOpts = TestData.DefaultOptions with { ForwardTo = target.Name };
        await using var source = await TestQueue.CreateAsync(transport, sourceOpts, ct);

        await using var sender = source.Client.CreateSender(source.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("forward-me"), ct);

        await using var receiver = target.Client.CreateReceiver(target.Name);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(msg);
        Assert.Equal("forward-me", msg!.Body.ToString());
        await receiver.CompleteMessageAsync(msg, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task ForwardDeadLetteredMessagesTo_RoutesDeadLettersToTargetQueue(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var target = await TestQueue.CreateAsync(transport, TestData.DefaultOptions, ct);
        var sourceOpts = TestData.DefaultOptions with { ForwardDeadLetteredMessagesTo = target.Name };
        await using var source = await TestQueue.CreateAsync(transport, sourceOpts, ct);

        await using var sender = source.Client.CreateSender(source.Name);
        await sender.SendMessageAsync(new ServiceBusMessage("dead-letter-forward"), ct);

        await using var receiver = source.Client.CreateReceiver(source.Name);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(msg);
        await receiver.DeadLetterMessageAsync(msg!, "Reason", "description", ct);

        await using var targetReceiver = target.Client.CreateReceiver(target.Name);
        var forwarded = await targetReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(forwarded);
        Assert.Equal("dead-letter-forward", forwarded!.Body.ToString());
        Assert.Equal("Reason", forwarded.DeadLetterReason);
        Assert.Equal("description", forwarded.DeadLetterErrorDescription);
        await targetReceiver.CompleteMessageAsync(forwarded, ct);
    }
}
