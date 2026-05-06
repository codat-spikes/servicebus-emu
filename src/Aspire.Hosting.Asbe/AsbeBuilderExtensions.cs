using System.Globalization;
using Aspire.Hosting.ApplicationModel;

namespace Aspire.Hosting;

public static class AsbeBuilderExtensions
{
    // Image coords. Pinned per package version so AppHosts get a reproducible
    // build. Override via `.WithImageTag("latest")` if you're iterating locally
    // on the asbe image alongside the AppHost.
    public const string ImageName = "asbe";
    public const string ImageTag = "latest";
    public const int InternalPort = 5672;

    /// <summary>
    /// Adds an asbe (Azure Service Bus emulator) container resource to the AppHost.
    /// </summary>
    public static IResourceBuilder<AsbeResource> AddAsbe(this IDistributedApplicationBuilder builder, string name)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(name);

        var resource = new AsbeResource(name);

        // Subscribe BeforeStart once per resource. The handler reads the resource's
        // (possibly post-AddAsbe-mutated) Queues/Topics lists and projects them
        // into env-var annotations on the container so asbe boots with the topology.
        builder.Eventing.Subscribe<Aspire.Hosting.ApplicationModel.BeforeStartEvent>((@event, ct) =>
        {
            foreach (var r in @event.Model.Resources.OfType<AsbeResource>())
            {
                var envVars = Flatten(r).ToList();
                if (envVars.Count == 0) continue;
                r.Annotations.Add(new EnvironmentCallbackAnnotation(ctx =>
                {
                    foreach (var (k, v) in envVars) ctx.EnvironmentVariables[k] = v;
                }));
            }
            return Task.CompletedTask;
        });

        return builder.AddResource(resource)
            .WithImage(ImageName, ImageTag)
            .WithEndpoint(targetPort: InternalPort, port: null, scheme: "tcp", name: AsbeResource.PrimaryEndpointName);
    }

    /// <summary>
    /// Pins the container image tag (default: "latest").
    /// </summary>
    public static IResourceBuilder<AsbeResource> WithImageTag(this IResourceBuilder<AsbeResource> builder, string tag)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithAnnotation(new ContainerImageAnnotation { Image = ImageName, Tag = tag }, ResourceAnnotationMutationBehavior.Replace);
    }

    /// <summary>
    /// Declares a queue on the asbe resource. Subsequent calls with the same name return
    /// the existing queue so multiple consumers can independently contribute their needs.
    /// </summary>
    public static AsbeQueueBuilder AddQueue(this IResourceBuilder<AsbeResource> builder, string name, Action<AsbeQueueBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(name);

        var existing = builder.Resource.Queues.Find(q => q.Name == name);
        var queue = existing ?? new AsbeQueue(name);
        if (existing is null) builder.Resource.Queues.Add(queue);
        var qb = new AsbeQueueBuilder(queue);
        configure?.Invoke(qb);
        return qb;
    }

    /// <summary>
    /// Declares a topic. Subsequent calls with the same name return the existing topic
    /// so subscriptions from multiple consumers accumulate.
    /// </summary>
    public static AsbeTopicBuilder AddTopic(this IResourceBuilder<AsbeResource> builder, string name, Action<AsbeTopicBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(name);

        var existing = builder.Resource.Topics.Find(t => t.Name == name);
        var topic = existing ?? new AsbeTopic(name);
        if (existing is null) builder.Resource.Topics.Add(topic);
        var tb = new AsbeTopicBuilder(topic);
        configure?.Invoke(tb);
        return tb;
    }

    /// <summary>
    /// Per-consumer contribution: declares that the referencing project requires the
    /// named queue. Equivalent to calling <c>AddQueue</c> on the resource directly,
    /// but reads more naturally on the consumer side.
    /// </summary>
    public static IResourceBuilder<TProject> RequiresQueue<TProject>(this IResourceBuilder<TProject> builder, IResourceBuilder<AsbeResource> asbe, string queueName, Action<AsbeQueueBuilder>? configure = null)
        where TProject : IResourceWithEnvironment
    {
        asbe.AddQueue(queueName, configure);
        return builder;
    }

    /// <summary>
    /// Per-consumer contribution: declares that the referencing project requires the
    /// named topic.
    /// </summary>
    public static IResourceBuilder<TProject> RequiresTopic<TProject>(this IResourceBuilder<TProject> builder, IResourceBuilder<AsbeResource> asbe, string topicName, Action<AsbeTopicBuilder>? configure = null)
        where TProject : IResourceWithEnvironment
    {
        asbe.AddTopic(topicName, configure);
        return builder;
    }

    internal static IEnumerable<KeyValuePair<string, string>> Flatten(AsbeResource r)
    {
        for (var i = 0; i < r.Queues.Count; i++)
        {
            var q = r.Queues[i];
            var prefix = $"Asbe__Queues__{i}__";
            yield return new(prefix + "Name", q.Name);
            yield return new(prefix + "LockDuration", FormatTimeSpan(q.LockDuration));
            yield return new(prefix + "MaxDeliveryCount", q.MaxDeliveryCount.ToString(CultureInfo.InvariantCulture));
            if (q.RequiresSession) yield return new(prefix + "RequiresSession", "true");
            if (q.DefaultMessageTimeToLive is { } ttl) yield return new(prefix + "DefaultMessageTimeToLive", FormatTimeSpan(ttl));
            if (q.DeadLetteringOnMessageExpiration) yield return new(prefix + "DeadLetteringOnMessageExpiration", "true");
            if (q.RequiresDuplicateDetection) yield return new(prefix + "RequiresDuplicateDetection", "true");
            if (q.DuplicateDetectionHistoryTimeWindow is { } w) yield return new(prefix + "DuplicateDetectionHistoryTimeWindow", FormatTimeSpan(w));
            if (q.ForwardTo is { Length: > 0 } f) yield return new(prefix + "ForwardTo", f);
            if (q.ForwardDeadLetteredMessagesTo is { Length: > 0 } fd) yield return new(prefix + "ForwardDeadLetteredMessagesTo", fd);
        }

        for (var i = 0; i < r.Topics.Count; i++)
        {
            var t = r.Topics[i];
            var prefix = $"Asbe__Topics__{i}__";
            yield return new(prefix + "Name", t.Name);
            if (t.RequiresDuplicateDetection) yield return new(prefix + "RequiresDuplicateDetection", "true");
            if (t.DuplicateDetectionHistoryTimeWindow is { } w) yield return new(prefix + "DuplicateDetectionHistoryTimeWindow", FormatTimeSpan(w));

            for (var j = 0; j < t.Subscriptions.Count; j++)
            {
                var s = t.Subscriptions[j];
                var sp = prefix + $"Subscriptions__{j}__";
                yield return new(sp + "Name", s.Name);
                yield return new(sp + "LockDuration", FormatTimeSpan(s.LockDuration));
                yield return new(sp + "MaxDeliveryCount", s.MaxDeliveryCount.ToString(CultureInfo.InvariantCulture));
                if (s.RequiresSession) yield return new(sp + "RequiresSession", "true");
                if (s.DefaultMessageTimeToLive is { } ttl) yield return new(sp + "DefaultMessageTimeToLive", FormatTimeSpan(ttl));
                if (s.DeadLetteringOnMessageExpiration) yield return new(sp + "DeadLetteringOnMessageExpiration", "true");
                if (s.ForwardTo is { Length: > 0 } f) yield return new(sp + "ForwardTo", f);
                if (s.ForwardDeadLetteredMessagesTo is { Length: > 0 } fd) yield return new(sp + "ForwardDeadLetteredMessagesTo", fd);

                for (var k = 0; k < s.Rules.Count; k++)
                {
                    var rule = s.Rules[k];
                    var rp = sp + $"Rules__{k}__";
                    yield return new(rp + "Name", rule.Name);
                    if (rule.SqlFilter is { Length: > 0 } sql) yield return new(rp + "SqlFilter", sql);
                    if (rule.TrueFilter is { } tf) yield return new(rp + "TrueFilter", tf ? "true" : "false");
                }
            }
        }
    }

    // .NET config binder reads TimeSpan via TimeSpan.Parse, which accepts the
    // standard "[d.]hh:mm:ss[.fffffff]" format produced by ToString("c").
    private static string FormatTimeSpan(TimeSpan ts) => ts.ToString("c", CultureInfo.InvariantCulture);
}
