using Aspire.Hosting;
using Microsoft.Extensions.Configuration;
using Xunit;

// Smoke tests for the Aspire.Hosting.Asbe package. These cover the bits with
// real risk of regression: registry dedup, env-var flattening, and connection
// string shape. They don't exercise the full DistributedApplication orchestrator
// — that's covered manually via `samples/AppHost`.
public class AspireHostingTests
{
    [Fact]
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public void AddQueue_WithSameName_DedupsAndAppliesLastConfiguration()
    {
        var builder = DistributedApplication.CreateBuilder([]);
        var asbe = builder.AddAsbe("sb");

        asbe.AddQueue("orders", q => q.WithMaxDeliveryCount(5));
        asbe.AddQueue("orders", q => q.WithMaxDeliveryCount(20));

        var queues = asbe.Resource.Queues;
        Assert.Single(queues);
        Assert.Equal(20, queues[0].MaxDeliveryCount);
    }

    [Fact]
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public void AddTopic_AccumulatesSubscriptionsFromMultipleCallers()
    {
        var builder = DistributedApplication.CreateBuilder([]);
        var asbe = builder.AddAsbe("sb");

        asbe.AddTopic("events").AddSubscription("billing", s => s.WithSqlFilter("Type = 'Invoice'"));
        asbe.AddTopic("events").AddSubscription("audit");

        var topics = asbe.Resource.Topics;
        Assert.Single(topics);
        Assert.Equal(2, topics[0].Subscriptions.Count);
        Assert.Contains(topics[0].Subscriptions, s => s.Name == "billing" && s.Rules.Count == 1);
        Assert.Contains(topics[0].Subscriptions, s => s.Name == "audit");
    }

    [Fact]
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public void Flatten_ProducesEnvVarsConsumedByAsbeOptionsBinder()
    {
        var builder = DistributedApplication.CreateBuilder([]);
        var asbe = builder.AddAsbe("sb");
        asbe.AddQueue("orders", q => q
            .WithLockDuration(TimeSpan.FromSeconds(45))
            .WithMaxDeliveryCount(7)
            .WithDuplicateDetection(TimeSpan.FromMinutes(10)));
        asbe.AddTopic("events").AddSubscription("billing", s => s.WithSqlFilter("Type = 'Invoice'"));

        var env = AsbeBuilderExtensions.Flatten(asbe.Resource).ToDictionary(kv => kv.Key, kv => kv.Value);

        // Round-trip the env vars through the same config binder asbe uses at startup
        // so this test fails if the env-var keys ever drift from AsbeOptions's shape.
        var config = new Microsoft.Extensions.Configuration.ConfigurationBuilder()
            .AddInMemoryCollection(env.Select(kv => new KeyValuePair<string, string?>(kv.Key.Replace("__", ":"), kv.Value)))
            .Build();
        var bound = new AsbeOptions();
        config.GetSection("Asbe").Bind(bound);

        Assert.Single(bound.Queues);
        Assert.Equal("orders", bound.Queues[0].Name);
        Assert.Equal(TimeSpan.FromSeconds(45), bound.Queues[0].LockDuration);
        Assert.Equal(7, bound.Queues[0].MaxDeliveryCount);
        Assert.True(bound.Queues[0].RequiresDuplicateDetection);
        Assert.Equal(TimeSpan.FromMinutes(10), bound.Queues[0].DuplicateDetectionHistoryTimeWindow);

        Assert.Single(bound.Topics);
        Assert.Equal("events", bound.Topics[0].Name);
        Assert.Single(bound.Topics[0].Subscriptions);
        Assert.Equal("billing", bound.Topics[0].Subscriptions[0].Name);
        Assert.Single(bound.Topics[0].Subscriptions[0].Rules);
        Assert.Equal("Type = 'Invoice'", bound.Topics[0].Subscriptions[0].Rules[0].SqlFilter);
    }

    [Fact]
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public void ConnectionStringExpression_UsesDevelopmentEmulatorFormat()
    {
        var builder = DistributedApplication.CreateBuilder([]);
        var asbe = builder.AddAsbe("sb");

        // The expression contains placeholder references resolved at runtime; we only
        // need to confirm the literal `UseDevelopmentEmulator=true` marker is present
        // so consumers wired via WithReference get a connection string the Azure SDK
        // recognises as the dev emulator.
        var format = asbe.Resource.ConnectionStringExpression.Format;
        Assert.Contains("UseDevelopmentEmulator=true", format, StringComparison.Ordinal);
        Assert.Contains("SharedAccessKeyName=dev", format, StringComparison.Ordinal);
    }
}
