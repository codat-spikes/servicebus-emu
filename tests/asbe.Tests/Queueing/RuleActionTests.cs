using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Xunit;
using AzureSqlFilter = Azure.Messaging.ServiceBus.Administration.SqlRuleFilter;
using AzureSqlAction = Azure.Messaging.ServiceBus.Administration.SqlRuleAction;

namespace Queueing;

// SQL rule actions — SET <prop> = <expr> and REMOVE <prop> mutate the matched message
// before it lands on the subscription buffer. Parameterised across Local/Azure transport
// to verify wire and semantics parity.
public sealed class RuleActionTests
{
    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task SetAppProperty_OverwritesExisting(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub", TestData.DefaultOptions)], ct);

        await using var rules = fx.Client.CreateRuleManager(fx.TopicName, "sub");
        await rules.DeleteRuleAsync("$Default", ct);
        await rules.CreateRuleAsync(new CreateRuleOptions("tag-uk", new AzureSqlFilter("region = 'uk'"))
        {
            Action = new AzureSqlAction("SET tier = 'gold'"),
        }, ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        var msg = new ServiceBusMessage("hi");
        msg.ApplicationProperties["region"] = "uk";
        msg.ApplicationProperties["tier"] = "bronze";
        await sender.SendMessageAsync(msg, ct);

        await using var receiver = fx.Client.CreateReceiver(fx.TopicName, "sub");
        var got = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(20), ct);
        Assert.NotNull(got);
        Assert.Equal("gold", got!.ApplicationProperties["tier"]);
        Assert.Equal("uk", got.ApplicationProperties["region"]);
        await receiver.CompleteMessageAsync(got, ct);
    }

    [Fact(Timeout = 60_000)]
    [Trait("Category", "Core")]
    public void MultiStatementAction_LocalParser_Applies()
    {
        var stmts = SqlActionParser.Parse("SET tier = 'gold', SET stamped = 'yes', REMOVE drop_me");
        Assert.Equal(3, stmts.Count);
        var msg = new Amqp.Message { ApplicationProperties = new Amqp.Framing.ApplicationProperties() };
        msg.ApplicationProperties["drop_me"] = "doomed";
        for (int i = 0; i < stmts.Count; i++) stmts[i].Apply(msg);
        Assert.Equal("gold", msg.ApplicationProperties["tier"]);
        Assert.Equal("yes", msg.ApplicationProperties["stamped"]);
        Assert.False(msg.ApplicationProperties.Map.ContainsKey("drop_me"));
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task SetSystemProperty_UpdatesSubject(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub", TestData.DefaultOptions)], ct);

        await using var rules = fx.Client.CreateRuleManager(fx.TopicName, "sub");
        await rules.DeleteRuleAsync("$Default", ct);
        await rules.CreateRuleAsync(new CreateRuleOptions("relabel", new AzureSqlFilter("1=1"))
        {
            Action = new AzureSqlAction("SET sys.Label = 'rebranded'"),
        }, ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        var msg = new ServiceBusMessage("body") { Subject = "original" };
        await sender.SendMessageAsync(msg, ct);

        await using var receiver = fx.Client.CreateReceiver(fx.TopicName, "sub");
        var got = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(20), ct);
        Assert.NotNull(got);
        Assert.Equal("rebranded", got!.Subject);
        await receiver.CompleteMessageAsync(got, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task RemoveAppProperty_DropsKey(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub", TestData.DefaultOptions)], ct);

        await using var rules = fx.Client.CreateRuleManager(fx.TopicName, "sub");
        await rules.DeleteRuleAsync("$Default", ct);
        await rules.CreateRuleAsync(new CreateRuleOptions("strip", new AzureSqlFilter("1=1"))
        {
            Action = new AzureSqlAction("REMOVE secret"),
        }, ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        var msg = new ServiceBusMessage("body");
        msg.ApplicationProperties["secret"] = "hush";
        msg.ApplicationProperties["keep"] = "this";
        await sender.SendMessageAsync(msg, ct);

        await using var receiver = fx.Client.CreateReceiver(fx.TopicName, "sub");
        var got = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(20), ct);
        Assert.NotNull(got);
        Assert.False(got!.ApplicationProperties.ContainsKey("secret"));
        Assert.Equal("this", got.ApplicationProperties["keep"]);
        await receiver.CompleteMessageAsync(got, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task SetSystemProperty_UpdatesContentType(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub", TestData.DefaultOptions)], ct);

        await using var rules = fx.Client.CreateRuleManager(fx.TopicName, "sub");
        await rules.DeleteRuleAsync("$Default", ct);
        await rules.CreateRuleAsync(new CreateRuleOptions("retype", new AzureSqlFilter("1=1"))
        {
            Action = new AzureSqlAction("SET sys.ContentType = 'application/x-rewritten'"),
        }, ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        var msg = new ServiceBusMessage("body") { ContentType = "application/json" };
        await sender.SendMessageAsync(msg, ct);

        await using var receiver = fx.Client.CreateReceiver(fx.TopicName, "sub");
        var got = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(20), ct);
        Assert.NotNull(got);
        Assert.Equal("application/x-rewritten", got!.ContentType);
        await receiver.CompleteMessageAsync(got, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task RuleWithoutAction_StillRoutes(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub", TestData.DefaultOptions)], ct);

        await using var rules = fx.Client.CreateRuleManager(fx.TopicName, "sub");
        await rules.DeleteRuleAsync("$Default", ct);
        await rules.CreateRuleAsync(new CreateRuleOptions("uk-only", new AzureSqlFilter("region = 'uk'")), ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        var msg = new ServiceBusMessage("body");
        msg.ApplicationProperties["region"] = "uk";
        msg.ApplicationProperties["tier"] = "bronze";
        await sender.SendMessageAsync(msg, ct);

        await using var receiver = fx.Client.CreateReceiver(fx.TopicName, "sub");
        var got = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(20), ct);
        Assert.NotNull(got);
        Assert.Equal("bronze", got!.ApplicationProperties["tier"]);
        await receiver.CompleteMessageAsync(got, ct);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task SqlRuleAction_RoundTripsThroughGetRules(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub", TestData.DefaultOptions)], ct);

        await using var rules = fx.Client.CreateRuleManager(fx.TopicName, "sub");
        await rules.CreateRuleAsync(new CreateRuleOptions("tag", new AzureSqlFilter("1=1"))
        {
            Action = new AzureSqlAction("SET tier = 'gold'"),
        }, ct);

        var enumerated = new List<RuleProperties>();
        await foreach (var r in rules.GetRulesAsync(ct)) enumerated.Add(r);

        var added = enumerated.Single(r => r.Name == "tag");
        var action = Assert.IsType<AzureSqlAction>(added.Action);
        Assert.Equal("SET tier = 'gold'", action.SqlExpression);
    }
}
