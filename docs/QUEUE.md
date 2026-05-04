# Queues

## What works

- SASL `MSSBCBS` + CBS token auth (`$cbs` request/response link pair).
- Send to a queue. The broker advertises `MaxMessageSize` on attach so the SDK's batch APIs work. Both `SendMessagesAsync(IEnumerable<>)` and `ServiceBusMessageBatch` (which the SDK packs into a single AMQP transfer with `message-format=0x80013700` and a body of multiple `Data` sections) are unwrapped server-side and fanned out into individual enqueues — applies to queue sends and topic sends both.
- Receive from a queue. Backed by an in-memory store with async-blocking dequeue (TCS waiters, no spin).
- Multiple queues. Any address auto-creates an `InMemoryQueue` via `QueueStore`.
- PeekLock receive with `CompleteMessageAsync` / `AbandonMessageAsync` / `DeadLetterMessageAsync` (including reason/description).
- Lock expiry timer: peek-locked messages held past `LockDuration` are re-enqueued with bumped delivery count.
- `MaxDeliveryCount` enforcement → automatic DLQ routing on exceeded count.
- `$DeadLetterQueue` sub-address routing for explicit and automatic dead-lettering.
- `$management` link, registered per-queue at `<name>/$management` and per-DLQ at `<name>/$DeadLetterQueue/$management` (DLQ management only handles `peek-message` and `renew-lock`; schedule/session ops return 501 there), with `com.microsoft:renew-lock` (`RenewMessageLockAsync`), `com.microsoft:peek-message` (`PeekMessageAsync` / `PeekMessagesAsync`), `com.microsoft:schedule-message` (`ScheduleMessageAsync`), `com.microsoft:cancel-scheduled-message` (`CancelScheduledMessageAsync`), `com.microsoft:renew-session-lock` (`RenewSessionLockAsync`), `com.microsoft:get-session-state` (`GetSessionStateAsync`), and `com.microsoft:set-session-state` (`SetSessionStateAsync`).
- Stable per-message sequence numbers (`x-opt-sequence-number`) assigned on first enqueue and preserved across redeliveries; primary and DLQ buffers each maintain their own sequence space.
- Topics + subscriptions. `CreateTopic(name, TopicOptions)` declares a topic with a fixed set of subscriptions; senders attach to the topic root and each delivery is fanned out (deep-cloned via AMQP encode/decode) into every subscription's `InMemoryQueue`. Receivers attach to `topic/Subscriptions/sub` and get the full queue surface — peek-lock, abandon, dead-letter, lock renewal, scheduled delivery, sessions, and `topic/Subscriptions/sub/$management`. Receiver attach to a bare topic address is rejected with `amqp:not-allowed`. Topics and subscriptions must be pre-declared (no auto-create) — matches Azure semantics.
- Subscription rules — correlation filters only. `SubscriptionOptions` accepts a list of `RuleFilter`s; `CorrelationRuleFilter` matches on system properties (`CorrelationId`, `MessageId`, `To`, `ReplyTo`, `Label`/Subject, `SessionId`/GroupId, `ReplyToSessionId`/ReplyToGroupId, `ContentType`) and arbitrary `ApplicationProperties` entries (AND across all set fields). Multiple rules on a subscription are OR-combined. A subscription with no rules is implicitly match-all. `SqlRuleFilter` is a placeholder that throws `NotSupportedException` until the SQL grammar lands.
- Sessions. Messages with `Properties.GroupId` set route into per-session sub-buffers that share the queue's sequence-number space. Receivers attaching with the `com.microsoft:session-filter` filter acquire a session lock (only one receiver per session at a time); the response attach echoes the session-id and stamps `com.microsoft:locked-until-utc` so the SDK knows when to renew. A null filter value picks the next available unlocked session that has pending messages. A contended attach is rejected with `com.microsoft:session-cannot-be-locked` so the SDK surfaces it as `ServiceBusFailureReason.SessionCannotBeLocked`. The session lock is released when the receiver detaches, and a different connection can immediately re-acquire. All queues support sessions implicitly — `RequiresSession` exists in `QueueOptions` only for parity with Azure's `CreateQueueOptions`.
- Receiver-detach lock semantics match Azure. AMQP `released` disposition (which AMQPNetLite emits for unsettled deliveries when a link closes) is treated as "lock held until expiry" rather than as an immediate abandon — matching Service Bus's at-least-once contract. Explicit `AbandonMessageAsync` (which the SDK sends as `modified` with `delivery-failed=true`) still requeues immediately as before.

## Known limitations

- No auth check beyond accepting the CBS `put-token` shape.
- SQL rule filters not implemented — only correlation filters and match-all. `SqlRuleFilter` exists as a placeholder but throws on evaluation.
- "Next available session" attach fails fast if no session is available; real Service Bus blocks until one becomes available (or times out).
- Session parity tests against Azure require a Standard-tier (or higher) namespace; the default `task sb:up` provisions Basic, so `SessionTests` skip the Azure transport unless the namespace is upgraded.
- Topic parity tests against Azure also require Standard-tier; `TopicTests` skip the Azure transport on Basic namespaces.

## Next steps (rough priority order)

1. **SQL rule filters.** Expression grammar + evaluator. The `SqlRuleFilter` placeholder is wired through TestInfrastructure already; just needs the parser/evaluator behind `Matches`.
2. **Transactions.** AMQP `txn:declare` / `txn:discharge` aren't supported at all — the SDK's `ServiceBusTransaction` would currently fail.
Deferred: server shutdown / `DisposeAsync`. No consumer needs it (the test harness keeps `LocalServer` as a singleton on purpose), and AMQPNetLite's `ContainerHost` close path has lock-ordering hazards (see the `DeleteQueue` deadlock comment in `AmqpServer.cs`). Revisit when a real consumer wants to start/stop the server in-process.
