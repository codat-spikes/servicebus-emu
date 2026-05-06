# asbe

.NET 10 solution exploring an in-process AMQP server that mimics Azure Service Bus, with parity tests against a real Service Bus namespace.

## Layout

- `src/asbe/` — the app: `AmqpServer` (AMQPNetLite listener) + `AmqpClient` (Azure.Messaging.ServiceBus). Configuration binds from the `Asbe:{Queues,Topics}` config section (or env vars like `Asbe__Queues__0__Name`); see `AsbeOptions.cs`. Container image built via `task image:build` (uses .NET SDK container support).
- `src/Aspire.Hosting.Asbe/` — Aspire hosting integration NuGet package. Exposes `builder.AddAsbe("sb")` + `AddQueue`/`AddTopic`/`AddSubscription` fluent API. Multiple consumers can independently call `AddQueue("name")` and the registry dedupes by name; the `BeforeStartEvent` handler flattens the registry into env-var annotations on the container.
- `samples/AppHost/` — minimal Aspire AppHost demonstrating the integration end-to-end. Run with `task sample` (after `task image:build`).
- `tests/asbe.Tests/` — xUnit v3 + Microsoft Testing Platform. Tests are parameterized over transport (`Local`, `Azure`) so the same assertions run against the in-proc server and a real Service Bus. `AspireHostingTests.cs` covers the hosting integration's dedup + env-var flattening + connection string shape.

## Taskfile

Use [Task](https://taskfile.dev) for all build/run/test/infra commands instead of invoking `dotnet` or `az` directly.

- `task` — list available tasks
- `task build` — build the solution
- `task run` — run the asbe app
- `task test` — run the test suite (skips Azure cases unless `tests/asbe.Tests/appsettings.test.json` provides a connection string)
- `task sb:up` — create Azure Service Bus (Basic SKU) namespace `asbe-testing` + queue `test-queue` in resource group `asbe-testing-rg` using the current `az` subscription. Prints the connection string at the end.
- `task sb:down` — delete the resource group (async).

When adding new common workflows, add them as tasks in `Taskfile.yml` rather than documenting raw shell commands. Keep `desc` fields short and accurate so `task --list` stays useful.

## Testing rules

- **Every new server feature must ship with a test** in `tests/asbe.Tests/`. Put it on the parameterized transport plumbing so it runs against both `Local` and `Azure` — that's the whole point of the test project, validating our server behaves like real Service Bus.
- Use `Assert.SkipWhen` for Azure-only preconditions (e.g. missing connection string). Don't gate the local case on Azure config.
- Tests are split by category and live in separate files: `CoreTests.cs` (Core), `TimingTests.cs` (Timing), `EdgeTests.cs` (Edge). Shared plumbing (transport enum, `TestQueue` fixture, `LocalServer`) is in `TestInfrastructure.cs`. Tag every new test with `[Trait("Category", "Core" | "Timing" | "Edge")]` and put it in the matching file. Use **Core** for fast round-trip parity, **Timing** for anything that waits on lock duration / scheduled delivery / redelivery, **Edge** for regression repros of specific bugs.
- Tests are *also* tagged with a `Speed` trait (`Fast` / `Slow`) cutting across categories — anything whose measured wall-clock is ≥4s on either transport is `Slow`. Tag every new test with both `Category` and `Speed`. If you don't know the speed, run it once and pick based on the duration.
- `task test:fast` is the recommended dev loop (Speed=Fast subset; ~74% of tests in ~2 min vs ~7 min full). `task test:core` / `test:timing` / `test:edge` filter by category; `task test:slow` runs only the slow subset; `task test` runs everything.
- To run the full parity suite: `task sb:up`, copy `tests/asbe.Tests/appsettings.test.example.json` to `appsettings.test.json` (gitignored) and paste in the printed connection string, `task test`, then `task sb:down` when finished.
