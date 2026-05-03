# asbe

.NET 10 console app demonstrating an AMQP server/client with simple queueing (AMQPNetLite + Azure.Messaging.ServiceBus).

## Taskfile

Use [Task](https://taskfile.dev) for all build/run commands instead of invoking `dotnet` directly. This keeps commands consistent and discoverable.

- `task` — list available tasks (default)
- `task build` — build the project
- `task run` — run the project

When adding new common workflows (test, format, publish, clean, etc.), add them as tasks to `Taskfile.yml` rather than documenting raw shell commands. Keep task `desc` fields short and accurate so `task --list` stays useful.
