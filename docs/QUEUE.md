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
- `$management` link, registered per-queue at `<name>/$management`, with `com.microsoft:renew-lock` (`RenewMessageLockAsync`).

## Known limitations

- No persistence — queue contents are lost on process exit.
- `$management` only implements renew-lock; peek / schedule / session ops still fail.
- No auth check beyond accepting the CBS `put-token` shape.
- No topics, subscriptions, or sessions.

## Next steps (rough priority order)

1. **More `$management` ops.** Peek (`com.microsoft:peek-message`), schedule, session ops. Same processor shape as renew-lock.
2. **Topics + subscriptions.** Addressing convention `topic/Subscriptions/sub` plus a fan-out store.
3. **Sessions.** `x-opt-session-id` filter on receivers, session locks.
4. **Persistence.** SQLite or a simple write-ahead log.
5. **Connection lifecycle.** Proper shutdown, multi-connection handling.
