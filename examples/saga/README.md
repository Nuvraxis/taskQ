# saga examples

Runnable programs showing taskQ's [`saga`](../../saga/) package — durable
multi-step workflows with automatic, reverse-order compensation.

Both use [`membroker`](../membroker/), so they need **no setup** — a saga has
no broker-specific code, so the same `Saga[S]` runs unmodified over
redisbroker or pgbroker.

| Example | What it shows |
| --- | --- |
| [`basic`](./basic) | Happy path — an order workflow (reserve inventory → charge payment → ship) where every step succeeds and the run completes. |
| [`compensate`](./compensate) | The final step fails permanently, so the steps that already succeeded are rolled back in reverse order (refund, then release), and `WithOnFailed` reports the step that actually failed. |

## Running

From the repository root:

```sh
go run ./examples/saga/basic
go run ./examples/saga/compensate
go run ./examples/saga/idempotent   # a Compensate retries without refunding twice
```

`compensate`'s output shows the pivot from forward to rollback:

```
[order-2002] reserved inventory (reservation resv-order-2002)
[order-2002] charged payment (charge charge-order-2002)
[order-2002] attempting to ship...
[order-2002] rolling back: refunding charge charge-order-2002
[order-2002] rolling back: releasing reservation resv-order-2002
saga … failed at "ship-order": carrier API unavailable — every prior step has been rolled back
```

## How it works

- **A saga is just a queue enqueuing to itself.** Each step is one hop:
  `Start` enqueues the first `Envelope[S]`, and every `Handler()` call runs one
  step, then enqueues the next hop (forward) or the previous step's
  compensation (rolling back). Nothing loops through the whole workflow
  in-process — so a crashed worker just resumes from the queue.
- **You drive it with an ordinary `Pool`.** `saga.New(...).Handler()` is a
  plain `taskq.Handler[Envelope[S]]`; both examples run it through
  `taskq.NewPool(broker, "orders", sg.Handler())`, exactly like any other
  queue.
- **`Compensate` may be nil.** The final `ship-order` step has none — a step
  with nothing to undo is skipped as an automatic no-op during rollback.
- **Per-hop retries.** `compensate` passes `WithMaxRetry(0)` to give up on the
  first failed attempt and keep the demo short; the default is 3.
- **Compensate must be safe to run twice.** A hop is retried as a whole and
    can be redelivered after a crash, so a refund or release that isn't
    naturally idempotent needs a dedup key. `saga.HopFromContext(ctx)` returns
    the saga ID, step index and direction of the hop currently executing;
    `Hop.Key()` is stable across retries and redeliveries, so hand it to your
    provider as the idempotency key or use it as a unique column in your own
    "already reversed" table. State is requeued verbatim on retry, so a flag
    written into `S` mid-attempt is not a substitute.

See the root [README](../../README.md#sagas) for the full walkthrough.
