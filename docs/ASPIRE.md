# Aspire integration

asbe ships with `Aspire.Hosting.Asbe`, a hosting integration for .NET Aspire 13.x. Add it to an AppHost and the asbe container resource is wired up with topology declared in C# — no JSON config file to mount.

## Quick start

```csharp
var builder = DistributedApplication.CreateBuilder(args);

var asbe = builder.AddAsbe("sb");

asbe.AddQueue("orders", q => q
    .WithLockDuration(TimeSpan.FromSeconds(30))
    .WithMaxDeliveryCount(10)
    .WithDuplicateDetection(TimeSpan.FromMinutes(10)));

asbe.AddTopic("events")
    .AddSubscription("billing", s => s.WithSqlFilter("Type = 'Invoice'"));

builder.AddProject<Projects.AppA>("app-a").WithReference(asbe);
builder.AddProject<Projects.AppB>("app-b").WithReference(asbe).RequiresQueue(asbe, "orders");

builder.Build().Run();
```

Consuming projects use stock `Azure.Messaging.ServiceBus` — `WithReference(asbe)` injects a `ConnectionStrings__sb` env var with `UseDevelopmentEmulator=true` so the SDK speaks plain AMQP to the asbe container.

## How it works

- `AddAsbe(name)` registers a `ContainerResource` pointing at the `asbe:latest` image (built locally via `task image:build`) with a TCP endpoint named `amqp` on internal port 5672.
- `AddQueue` / `AddTopic` / `AddSubscription` mutate a registry on the `AsbeResource`. Calls with the same entity name are deduped — last writer wins for options, subscriptions accumulate. This is what makes per-consumer contribution (`RequiresQueue` from inside a project's `.WithReference(...)` chain) compose without coordination.
- A `BeforeStartEvent` handler flattens the registry into env vars (`Asbe__Queues__N__*`, `Asbe__Topics__N__Subscriptions__M__*`) on the container before the orchestrator starts it. asbe's `Program.cs` binds these via the standard .NET config binder into `AsbeOptions` and creates the entities at boot.
- Tracing is automatic: asbe's `Asbe.Server` `ActivitySource` emits OTel spans, and Aspire's auto-injected `OTEL_EXPORTER_OTLP_ENDPOINT` env var routes them into the dashboard.

## Multiple consumers, one bus

The pattern that motivated this design — App A and App B both need Service Bus, and App A is itself consumed by App B's tests:

```csharp
var asbe = builder.AddAsbe("sb");

builder.AddProject<Projects.AppA>("app-a")
    .WithReference(asbe)
    .RequiresQueue(asbe, "orders")
    .RequiresTopic(asbe, "events");

builder.AddProject<Projects.AppB>("app-b")
    .WithReference(asbe)
    .RequiresQueue(asbe, "orders")              // dedupes with App A's declaration
    .RequiresQueue(asbe, "invoices");           // App B-specific
```

Both apps see the same broker and the same topology. Where to declare each entity is a style call — the dedup means it doesn't matter for correctness.

## Follow-ups

The following are deliberately out of scope for the initial integration. None block the headline flow; capture issues as we hit them.

### Aspire transport in the parity test suite

`tests/asbe.Tests` is parameterised over `Local` and `Azure`. A third `Aspire` transport — spinning up a `DistributedApplication` via `Aspire.Hosting.Testing` and running the existing assertions against the containerised image — would catch wire-level regressions the in-proc `Local` path can't see (TCP framing, connection pooling, container env-var handling).

Blocker: the test fixture creates queues at runtime (`LocalServer.Server.CreateQueue(name, options)` per test), but the Aspire integration declares topology at AppHost build time. To support runtime queue creation over AMQP we'd need to handle Service Bus's management `CreateEntity` operation in `ManagementRequestProcessor` — currently we only handle the runtime ops (peek, lock renewal, scheduled, sessions, deferred, rule management).

Two ways to unblock:
1. Add `CreateEntity` / `DeleteEntity` to the management surface so tests can stand up unique queues over the wire.
2. Start a long-lived AppHost once per test session with a stable shared queue, and have tests use unique message IDs / session IDs to isolate from each other. Cheaper to land but loses the per-test cleanup guarantee.

### Publishing the container image to a registry

`AddAsbe` defaults to `asbe:latest` and assumes a local `docker images asbe` (built via `task image:build`). For consumers outside this repo we should:

- Publish to `ghcr.io/aevv/asbe:<version>` from CI, tag-aligned with the `Aspire.Hosting.Asbe` package version.
- Default `WithImage` to the registry-qualified name and let consumers override via `.WithImageTag(...)` for local iteration.

Until then, anyone trying the AppHost outside this repo has to clone + `task image:build` first.

### Client integration package

A separate `Aspire.Asbe` client integration (`builder.AddAsbeClient("sb")`) that wraps `Azure.Messaging.ServiceBus` registration with health checks + OTel sources is *not* planned. Microsoft's `Aspire.Azure.Messaging.ServiceBus` already does this and works against asbe unchanged because we speak the dev-emulator wire format. Use that.

### Topology hot-reload

Mutating queues/topics after AppHost startup currently requires either a restart or going through the runtime rule-management API (subscriptions only). If this becomes painful we can extend the management surface (above) and wire a `BeforeUpdateEvent` handler that diffs the registry and POSTs the delta — but `dotnet watch` on the AppHost is good enough for now.

### OTel coverage

Today only the producer-side `queue.enqueue` span exists. Useful additions:

- Consumer-side spans (link attach + delivery dispatch) with `messaging.operation.type=receive`.
- Settlement spans (`complete`, `abandon`, `dead-letter`) so latency to settle shows up in the dashboard.
- Management-op spans (peek, schedule, rule add/remove).
- Metrics: queue depth, lock-expiry rate, dedup drop rate, DLQ rate.
