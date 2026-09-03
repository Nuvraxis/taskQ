# membroker examples

Runnable programs showing how to use taskQ with the in-memory broker
([`membroker`](../../membroker/)). Because `membroker` needs no external
services, every example runs with a plain `go run` and no setup.

| Example | What it shows |
| --- | --- |
| [`basic`](./basic) | Enqueue typed jobs, fan them out to a pool of worker goroutines, and shut down cleanly when the queue drains. |
| [`retry`](./retry) | Retry semantics: a failed handler is requeued with `Nack` up to `MaxRetry` times, then given up on (dead-lettered). |

## Running

From the repository root:

```sh
go run ./examples/membroker/basic
go run ./examples/membroker/retry
```

Each example includes a small `work` consume loop that dequeues a raw
`Message`, decodes it into a typed `Task[T]`, and invokes a `Handler[T]`.
That glue is intentional: taskQ's broker layer is byte-level on purpose, and
a built-in worker pool is still on the roadmap. Until it lands, these loops
show exactly what a consumer needs to do.
