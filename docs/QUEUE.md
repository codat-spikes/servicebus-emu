# Queues

## What works

- SASL `MSSBCBS` + CBS token auth (`$cbs` request/response link pair).
- Send to a queue. The broker advertises `MaxMessageSize` on attach so the SDK's batch APIs work.
- Receive from a queue. Backed by an in-memory store with async-blocking dequeue (TCS waiters, no spin).
- Multiple queues. Any address auto-creates an `InMemoryQueue` via `QueueStore`.
- PeekLock receive with `CompleteMessageAsync` / `AbandonMessageAsync` / `DeadLetterMessageAsync`.

## Known limitations

- No lock expiry / redelivery timer — abandoned messages are requeued, but a peek-locked message held by a crashed receiver is leaked until process exit.
- No delivery-count tracking on redelivered messages.
- Dead-lettered messages go to an in-memory side queue with no `$DeadLetterQueue` sub-address yet.
- No persistence — queue contents are lost on process exit.
- No `$management` link, so peek / schedule / session / renew-lock SDK calls will fail.
- No auth check beyond accepting the CBS `put-token` shape.
- No topics, subscriptions, or sessions.

## Next steps (rough priority order)

1. **Lock expiry + delivery count.** Per-message timer that re-enqueues on expiry; bump `header.delivery-count` on redelivery. `$DeadLetterQueue` sub-address routing.
2. **`$management` link.** Same `IRequestProcessor` shape as `$cbs`. Needed for peek, schedule, session ops, renew-lock.
3. **Topics + subscriptions.** Addressing convention `topic/Subscriptions/sub` plus a fan-out store.
4. **Sessions.** `x-opt-session-id` filter on receivers, session locks.
5. **Persistence.** SQLite or a simple write-ahead log.
6. **Connection lifecycle.** Proper shutdown, multi-connection handling.
