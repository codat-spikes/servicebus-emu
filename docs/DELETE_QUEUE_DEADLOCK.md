# DeleteQueue / connection-close deadlock

## Symptom

Running the parity test suite with Azure enabled hangs. Most Local tests time out after 60s with `Test execution timed out after 60000 milliseconds`. The first hung test varies between runs (`CancelScheduledMessage_PreventsDelivery(Local)`, `PeekLock_RenewLock_ExtendsLock(Local)`, etc.); once one hangs, the static `LocalServer` is wedged for every subsequent Local test in the run.

Local-only run (no Azure config) finished green — the wedge needed enough activity to expose a race, and the slower mixed-transport run reliably tripped it.

## Root cause

A lock-ordering deadlock inside AmqpNetLite, triggered when our `AmqpServer.DeleteQueue` ran during the same window as a client closing its connection.

### Threads involved

**Thread A — test thread, fixture cleanup:**

```
TestQueue.DisposeAsync
  → AmqpServer.DeleteQueue
    → ContainerHost.UnregisterRequestProcessor
      → RemoveProcessor (locks requestProcessors)
        → RequestProcessor.Dispose
          → lock(requestLinks)
            → for each link: link.CloseInternal(0, error)
              → wants link's own monitor (lock(this))
```

**Thread B — server connection-close handler, fired when the SDK closes its connection:**

```
ListenerConnection close
  → for each link: link.CloseInternal
    → lock(this)            ← link's own monitor
      → NotifyClosed
        → fires Closed event
          → RequestProcessor.OnLinkClosed
            → wants lock(requestLinks)
```

A holds `requestLinks` and waits for the link's monitor. B holds the link's monitor and waits for `requestLinks`. Classic deadlock.

It only triggers when client disposal and `DeleteQueue` race against each other. A 500ms `Task.Delay` between the two makes it disappear, which was the smoking gun.

## Fix

`src/asbe/Server/AmqpServer.cs` — `DeleteQueue` no longer calls `_host.UnregisterRequestProcessor`. We just remove the queue from `QueueStore`. The management endpoint registration leaks for the lifetime of the server, but:

- Test queue names are unique GUIDs, so re-registration can't collide.
- The leaked entry is one dictionary slot plus a `ManagementRequestProcessor` instance — trivial.
- Production callers don't churn queues at a rate where the leak matters.

The deadlock disappears because Thread A no longer takes `requestLinks` at all.

## Reproduction

`tests/asbe.Tests/ConcurrencyReproTests.cs::RepeatedScheduleCancelDrainDelete_DoesNotHang` — Local-only, single test, no Azure config required. One iteration of `schedule + cancel + drain receive(1s) + DeleteQueue` reliably hung before the fix and passes after. Worth keeping as a regression guard.

## Related but separate

- `CancelScheduledMessage_PreventsDelivery(transport: Azure)` fails its assertion (real Service Bus delivers the message despite `CancelScheduledMessageAsync`). Likely a tight 5-second window in the test rather than a server-emulator bug. Unaffected by this fix.
- The earlier `[Theory(Timeout = 60_000)]` plumbing on `QueueingTests` was added during diagnosis so that hangs surface as test failures instead of a stuck process. Worth keeping.

## Investigation dead-ends (so the next person doesn't repeat them)

- **AmqpNetLite's `CompleteDrain` sends a flow with `drain=false`.** The AMQP spec says the drain ack should have `drain=true`. Looked like a smoking gun, but instrumenting `OnFlow` showed CompleteDrain always ran in <2ms and the SDK still hung. The wedge was downstream — at `DeleteQueue` — not at drain ack. We added a reflection-based corrective-flow workaround (`SendDrainTrueAck` in `AmqpNetLiteCompat.cs`) that turned out to be unnecessary; safe to revert if we want a smaller diff.
- **Static `LocalServer` cross-test pollution.** Suspected, but the deadlock is per-pair-of-threads, not cumulative state.
- **Test parallelism.** Setting `parallelizeTestCollections: false` in `xunit.runner.json` did not change the symptom — confirming the race wasn't between concurrent tests but within a single test's teardown.
