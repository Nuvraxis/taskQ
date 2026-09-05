# redisbroker examples

Runnable programs showing how to use taskQ with the Redis Streams broker
([`redisbroker`](../../redisbroker/)).

Unlike [`membroker`](../membroker/), these need a **running Redis**. Start one
with either:

```sh
docker run --rm -p 6379:6379 redis:7   # throwaway instance
make deps-up                           # redis + postgres via compose
```

The examples connect to `localhost:6379` by default; override with the
`TASKQ_REDIS_ADDR` environment variable. If Redis isn't reachable, each
example prints how to start one and exits cleanly instead of crashing.

| Example | What it shows |
| --- | --- |
| [`basic`](./basic) | Connect, enqueue typed jobs, read them back through a consumer group, and `Ack` each by its `ReceiptHandle`. |
| [`retry`](./retry) | Single-worker retry: on failure, `Nack` writes a **new** stream entry at the back (Redis Streams redelivery), up to `MaxRetry`, then dead-letters. |
| [`pool`](./pool) | A reusable `Pool[T]`: N workers sharing one consumer group, retries with **exponential backoff + jitter**, settlement callbacks, and context-driven shutdown. |

## Running

From the repository root:

```sh
go run ./examples/redisbroker/basic
go run ./examples/redisbroker/retry
go run ./examples/redisbroker/pool
```

## How these differ from the membroker examples

They use the same `taskq.Queue[T]` / `Handler[T]` / `Task[T]` API — only the
broker setup and shutdown differ, because `redisbroker` is a networked,
persistent backend:

- **You own the client.** `redisbroker.New(client, …)` takes a `*redis.Client`
  it does not manage; the example creates it and `defer client.Close()`s it.
- **No `Close()` / `ErrQueueClosed`.** A consumer stops when its **context is
  cancelled**, not when the broker is closed. The examples track completion
  with a `sync.WaitGroup` and then `cancel()`.
- **Ack/Nack target a `ReceiptHandle`.** `Dequeue` records the Redis Stream
  entry ID on the message; `Ack` (XACK + XDEL) and `Nack` (requeue as a new
  entry, then XACK/XDEL the old one) use it.
- **Crash recovery is built in.** An entry a worker Dequeued but never Acked
  stays in the consumer group's Pending Entries List; `Dequeue` sweeps for one
  such stale entry (idle past `WithClaimMinIdle`, default 30s) via `XAUTOCLAIM`
  before reading new entries, so a crashed consumer's work is redelivered
  automatically — no separate reaper or ticker, the same shape as pgbroker's
  lease expiry.

The `pool` example's `Pool[T]` is deliberately near-identical to the
[membroker `pool`](../membroker/pool/) one: that's the point of the shared
`Broker` interface — the same consumer code runs over either backend.
