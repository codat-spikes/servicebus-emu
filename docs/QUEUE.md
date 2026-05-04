# Queues

## What works

- SASL `MSSBCBS` + CBS token auth (`$cbs` request/response link pair).
- Send to a queue. The broker advertises `MaxMessageSize` on attach so the SDK's batch APIs work.
- Receive from a queue. Backed by an in-memory store with async-blocking dequeue (TCS waiters, no spin).
- Multiple queues. Any address auto-creates an `InMemoryQueue` via `QueueStore`.
- PeekLock receive with `CompleteMessageAsync` / `AbandonMessageAsync` / `DeadLetterMessageAsync` (including reason/description).
- Lock expiry timer: peek-locked messages held past `LockDuration` are re-enqueued with bumped delivery count.
- `MaxDeliveryCount` enforcement → automatic DLQ routing on exceeded count.
- `$DeadLetterQueue` sub-address routing for explicit and automatic dead-lettering.
- `$management` link, registered per-queue at `<name>/$management`, with `com.microsoft:renew-lock` (`RenewMessageLockAsync`), `com.microsoft:peek-message` (`PeekMessageAsync` / `PeekMessagesAsync`), `com.microsoft:schedule-message` (`ScheduleMessageAsync`), `com.microsoft:cancel-scheduled-message` (`CancelScheduledMessageAsync`), `com.microsoft:renew-session-lock` (`RenewSessionLockAsync`), `com.microsoft:get-session-state` (`GetSessionStateAsync`), and `com.microsoft:set-session-state` (`SetSessionStateAsync`).
- Stable per-message sequence numbers (`x-opt-sequence-number`) assigned on first enqueue and preserved across redeliveries; primary and DLQ buffers each maintain their own sequence space.
- Sessions. Messages with `Properties.GroupId` set route into per-session sub-buffers that share the queue's sequence-number space. Receivers attaching with the `com.microsoft:session-filter` filter acquire a session lock (only one receiver per session at a time); the response attach echoes the session-id and stamps `com.microsoft:locked-until-utc` so the SDK knows when to renew. A null filter value picks the next available unlocked session that has pending messages. The session lock is released when the receiver detaches. All queues support sessions implicitly — `RequiresSession` exists in `QueueOptions` only for parity with Azure's `CreateQueueOptions`.

## Known limitations

- No persistence — queue contents are lost on process exit.
- No auth check beyond accepting the CBS `put-token` shape.
- No topics or subscriptions.
- "Next available session" attach fails fast if no session is available; real Service Bus blocks until one becomes available (or times out).
- Session parity tests against Azure require a Standard-tier (or higher) namespace; the default `task sb:up` provisions Basic, so `SessionTests` skip the Azure transport unless the namespace is upgraded.

## Next steps (rough priority order)

1. **Topics + subscriptions.** Addressing convention `topic/Subscriptions/sub` plus a fan-out store.
2. **Persistence.** SQLite or a simple write-ahead log.
3. **Connection lifecycle.** Proper shutdown, multi-connection handling.
