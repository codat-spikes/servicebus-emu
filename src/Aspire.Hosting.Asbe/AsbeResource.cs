using Aspire.Hosting.ApplicationModel;

namespace Aspire.Hosting;

// The asbe container resource. Mirrors the Service Bus dev-emulator wire format,
// so consumers wire it up with stock `Azure.Messaging.ServiceBus` and a
// `UseDevelopmentEmulator=true` connection string — no asbe-specific code in the
// consuming app.
public sealed class AsbeResource : ContainerResource, IResourceWithConnectionString
{
    public const string PrimaryEndpointName = "amqp";

    internal List<AsbeQueue> Queues { get; } = new();
    internal List<AsbeTopic> Topics { get; } = new();

    public AsbeResource(string name) : base(name) { }

    public EndpointReference PrimaryEndpoint => new(this, PrimaryEndpointName);

    // The dev-emulator connection string format Azure.Messaging.ServiceBus
    // recognises — keys are accepted but unverified, and `UseDevelopmentEmulator=true`
    // tells the SDK to talk plain AMQP (no TLS) and skip SAS token CBS handshake quirks.
    public ReferenceExpression ConnectionStringExpression =>
        ReferenceExpression.Create(
            $"Endpoint=sb://{PrimaryEndpoint.Property(EndpointProperty.Host)}:{PrimaryEndpoint.Property(EndpointProperty.Port)};SharedAccessKeyName=dev;SharedAccessKey=dev;UseDevelopmentEmulator=true;");
}

public sealed class AsbeQueue
{
    public string Name { get; }
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxDeliveryCount { get; set; } = 10;
    public bool RequiresSession { get; set; }
    public TimeSpan? DefaultMessageTimeToLive { get; set; }
    public bool DeadLetteringOnMessageExpiration { get; set; }
    public bool RequiresDuplicateDetection { get; set; }
    public TimeSpan? DuplicateDetectionHistoryTimeWindow { get; set; }
    public string? ForwardTo { get; set; }
    public string? ForwardDeadLetteredMessagesTo { get; set; }

    internal AsbeQueue(string name) { Name = name; }
}

public sealed class AsbeTopic
{
    public string Name { get; }
    public bool RequiresDuplicateDetection { get; set; }
    public TimeSpan? DuplicateDetectionHistoryTimeWindow { get; set; }
    internal List<AsbeSubscription> Subscriptions { get; } = new();

    internal AsbeTopic(string name) { Name = name; }
}

public sealed class AsbeSubscription
{
    public string Name { get; }
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxDeliveryCount { get; set; } = 10;
    public bool RequiresSession { get; set; }
    public TimeSpan? DefaultMessageTimeToLive { get; set; }
    public bool DeadLetteringOnMessageExpiration { get; set; }
    public string? ForwardTo { get; set; }
    public string? ForwardDeadLetteredMessagesTo { get; set; }
    internal List<AsbeRule> Rules { get; } = new();

    internal AsbeSubscription(string name) { Name = name; }
}

public sealed class AsbeRule
{
    public string Name { get; set; } = "$Default";
    public string? SqlFilter { get; set; }
    public bool? TrueFilter { get; set; }
}
