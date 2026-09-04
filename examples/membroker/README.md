# membroker examples

Runnable programs showing how to use taskQ with the in-memory broker
([`membroker`](../../membroker/)). Because `membroker` needs no external
services, every example runs with a plain `go run` and no setup.

| Example | What it shows |
| --- | --- |
| [`basic`](./basic) | Enqueue typed jobs, fan them out to a pool of worker goroutines, and shut down cleanly when the queue drains. |
| [`retry`](./retry) | Retry semantics: a failed handler is requeued with `Nack` up to `MaxRetry` times, then given up on (dead-lettered). |
| [`pool`](./pool) | A reusable worker `Pool[T]` combining both: N concurrent workers, retries with **exponential backoff + jitter**, per-task settlement callbacks, and context-driven graceful shutdown. |

## Running

From the repository root:

```sh
go run ./examples/membroker/basic
go run ./examples/membroker/retry
go run ./examples/membroker/pool
```

Each example includes the consume-side glue — dequeue a raw `Message`, decode
it into a typed `Task[T]`, invoke a `Handler[T]`, then `Ack`/`Nack` — as a
plain loop (`basic`, `retry`) or wrapped in a small `Pool[T]` type (`pool`).
That glue is intentional: taskQ's broker layer is byte-level on purpose, and a
built-in worker pool is still on the roadmap. Until it lands, `pool` is the
pattern to copy; the others show the mechanics underneath it.

> **Note on backoff:** `membroker` has no delayed-delivery / visibility
> timeout, so `pool` implements backoff by having the worker wait before it
> requeues (`Nack` puts the task at the back of the queue). A production
> broker would instead redeliver the task with a *not-before* timestamp, so a
> worker never has to sit idle sleeping.
