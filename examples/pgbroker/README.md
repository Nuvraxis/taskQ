# pgbroker examples

Runnable programs showing how to use taskQ with the Postgres broker
([`pgbroker`](../../pgbroker/)).

These need a **running Postgres**. Start one with:

```sh
docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16
```

The examples connect using the DSN in `TASKQ_POSTGRES_DSN`, defaulting to
`postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable`. If
Postgres isn't reachable, each example prints how to start one and exits
cleanly instead of crashing.

You don't need to create the table by hand: the shared
[`internal/pgdemo`](./internal/pgdemo) helper applies `pgbroker/schema.sql`
(all `CREATE ... IF NOT EXISTS`) on connect. In a real app you'd run
`schema.sql` once yourself — pgbroker has no migration tooling.

| Example | What it shows |
| --- | --- |
| [`basic`](./basic) | Connect, apply schema, enqueue, consume (`SELECT ... FOR UPDATE SKIP LOCKED`), `Ack` by `ReceiptHandle`, and read `QueueDepth` via the admin methods. |
| [`pool`](./pool) | The built-in `taskq.Pool` + `taskq.Chain` middleware: 4 workers competing for one queue (SKIP LOCKED), retries with backoff, `RecoveryMiddleware`, and context-driven shutdown. |

## Running

From the repository root:

```sh
go run ./examples/pgbroker/basic
go run ./examples/pgbroker/pool
```

## How these differ from the other backends

Same `taskq.Queue[T]` / `Pool[T]` / `Handler[T]` API — only the broker setup
differs, because pgbroker is a durable, lease-based backend:

- **You own the pool.** `pgbroker.New(pool, …)` takes a caller-owned
  `*pgxpool.Pool`; the example creates it and `defer pool.Close()`s it. The
  broker has no `Close()`.
- **The table must exist.** Run `pgbroker/schema.sql` once (the examples do it
  for you via `pgdemo`). No migrations yet.
- **Lease = visibility timeout.** A dequeued row is invisible to other workers
  until it's Acked or its lease (`WithLeaseDuration`, default 30s) expires — at
  which point it's redelivered. That's real crash recovery, with no separate
  reaper: the same query that finds new rows finds expired-lease ones.
- **Context-only shutdown.** No `ErrQueueClosed`; `Pool.Run` / `Dequeue` stop
  when the context is cancelled.
- **Admin methods.** `QueueDepth`, `QueueDepthAvailable`, `PeekMessages`,
  `ReapExpiredLease`, `PurgeQueue`, `ListQueues`, `PurgeAll` — for
  monitoring/cleanup, outside the `Broker` interface.

See the root [README](../../README.md#postgres-broker) for the full config
reference.
