using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Xunit;
using AzureSql = Azure.Messaging.ServiceBus.Administration.SqlRuleFilter;
using AzureCorrelation = Azure.Messaging.ServiceBus.Administration.CorrelationRuleFilter;
using AzureTrue = Azure.Messaging.ServiceBus.Administration.TrueRuleFilter;
using LocalSql = SqlRuleFilter;
using LocalCorrelation = CorrelationRuleFilter;

namespace Queueing;

// Runtime subscription rule management — AddRuleAsync / RemoveRuleAsync / GetRulesAsync
// over the subscription's $management link.
public sealed class RuntimeRulesTests
{
    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task DefaultRuleIsMatchAll_SeededOnSubscriptionCreate(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub", TestData.DefaultOptions)], ct);

        await using var rules = fx.Client.CreateRuleManager(fx.TopicName, "sub");
        var enumerated = new List<RuleProperties>();
        await foreach (var r in rules.GetRulesAsync(ct)) enumerated.Add(r);

        Assert.Single(enumerated);
        Assert.Equal("$Default", enumerated[0].Name);
        Assert.IsType<AzureTrue>(enumerated[0].Filter);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task AddRule_RoutesMatchingMessages(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        // Seed the subscription with no filter so $Default = TrueFilter; remove it then
        // add a SQL filter at runtime, and prove it routes.
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub", TestData.DefaultOptions)], ct);

        await using var rules = fx.Client.CreateRuleManager(fx.TopicName, "sub");
        await rules.DeleteRuleAsync("$Default", ct);
        await rules.CreateRuleAsync(
            new CreateRuleOptions("gold-uk", new AzureSql("region = 'uk' AND tier = 'gold'")),
            ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        var match = new ServiceBusMessage("match");
        match.ApplicationProperties["region"] = "uk";
        match.ApplicationProperties["tier"] = "gold";
        var miss = new ServiceBusMessage("miss");
        miss.ApplicationProperties["region"] = "us";
        miss.ApplicationProperties["tier"] = "gold";
        await sender.SendMessageAsync(match, ct);
        await sender.SendMessageAsync(miss, ct);

        await using var receiver = fx.Client.CreateReceiver(fx.TopicName, "sub");
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(received);
        Assert.Equal("match", received!.Body.ToString());
        await receiver.CompleteMessageAsync(received, ct);

        var extra = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(extra);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task AddCorrelationRule_RoundTripsThroughGetRules(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub", TestData.DefaultOptions)], ct);

        await using var rules = fx.Client.CreateRuleManager(fx.TopicName, "sub");
        var corr = new AzureCorrelation
        {
            CorrelationId = "for-eu",
            Subject = "orders",
        };
        corr.ApplicationProperties["region"] = "eu";
        await rules.CreateRuleAsync(new CreateRuleOptions("orders-eu", corr), ct);

        var enumerated = new List<RuleProperties>();
        await foreach (var r in rules.GetRulesAsync(ct)) enumerated.Add(r);

        var added = enumerated.Single(r => r.Name == "orders-eu");
        var azureCorr = Assert.IsType<AzureCorrelation>(added.Filter);
        Assert.Equal("for-eu", azureCorr.CorrelationId);
        Assert.Equal("orders", azureCorr.Subject);
        Assert.Equal("eu", azureCorr.ApplicationProperties["region"]);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task RemoveRule_StopsRouting(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub", TestData.DefaultOptions)], ct);

        await using var rules = fx.Client.CreateRuleManager(fx.TopicName, "sub");
        // Removing $Default leaves zero rules; nothing should route.
        await rules.DeleteRuleAsync("$Default", ct);

        await using var sender = fx.Client.CreateSender(fx.TopicName);
        await sender.SendMessageAsync(new ServiceBusMessage("dropped"), ct);

        await using var receiver = fx.Client.CreateReceiver(fx.TopicName, "sub");
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2), ct);
        Assert.Null(received);
    }

    [Theory(Timeout = 60_000)]
    [Trait("Category", "Core")]
    [MemberData(nameof(TestData.Transports), MemberType = typeof(TestData))]
    public async Task AddRule_DuplicateName_Throws(Transport transport)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await TestTopic.CreateAsync(transport,
            [("sub", TestData.DefaultOptions)], ct);

        await using var rules = fx.Client.CreateRuleManager(fx.TopicName, "sub");
        await Assert.ThrowsAsync<ServiceBusException>(async () =>
            await rules.CreateRuleAsync(new CreateRuleOptions("$Default", new AzureSql("1=1")), ct));
    }
}
