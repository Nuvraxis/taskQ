# membroker examples

Runnable programs showing how to use taskQ with the in-memory broker
([`membroker`](../../membroker/)). Because `membroker` needs no external
services, every example runs with a plain `go run` and no setup.

| Example | What it shows |
| --- | --- |
| [`basic`](./basic) | Enqueue typed jobs, fan them out to a pool of worker goroutines, and shut down cleanly when the queue drains. |
| [`retry`](./retry) | Retry semantics: a failed handler is requeued with `Nack` up to `MaxRetry` times, then given up on (dead-lettered). |
| [`pool`](./pool) | A **hand-written** worker `Pool[T]` — N concurrent workers, retries with exponential backoff + jitter, settlement callbacks, context-driven shutdown. Shows the mechanics the built-in `taskq.Pool` now handles for you. |
| [`middleware`](./middleware) | The **built-in** `taskq.Pool` plus `taskq.Chain` middleware: `LoggingMiddleware`, a hand-written recorder, and `RecoveryMiddleware` (panics become retryable errors). |

## Running

From the repository root:

```sh
go run ./examples/membroker/basic
go run ./examples/membroker/retry
go run ./examples/membroker/pool
go run ./examples/membroker/middleware
```

`basic`, `retry`, and `pool` spell out the consume-side glue by hand — dequeue
a raw `Message`, decode it into a typed `Task[T]`, invoke a `Handler[T]`, then
`Ack`/`Nack`. That's useful for understanding what a consumer does, but taskQ
now ships that loop as `taskq.Pool`; `middleware` uses the real thing. Reach
for `taskq.NewPool` in your own code and keep the hand-written `pool` example
as a reference for the mechanics underneath it.

> **Note on backoff:** `membroker` has no delayed-delivery / visibility
> timeout, so `pool` implements backoff by having the worker wait before it
> requeues (`Nack` puts the task at the back of the queue). A production
> broker would instead redeliver the task with a *not-before* timestamp, so a
> worker never has to sit idle sleeping.
