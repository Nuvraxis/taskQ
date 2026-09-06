# taskQ

A small, type-safe task queue for Go, built around a deliberately tiny broker
interface. Producers enqueue strongly-typed payloads; a pluggable broker
handles delivery, acknowledgement, and requeue. The core has no dependencies
beyond [`google/uuid`](https://github.com/google/uuid).

```
  producer                            consumer
Queue[T] ─Enqueue─▶ Broker ─Dequeue─▶ Pool[T] ─▶ Chain(mw…) ─▶ Handler[T]
 (T→JSON)          (transport)      (N workers,   (logging,      (your code)
                        ▲            backoff)      recovery,…)
                        └──────────── Ack / Nack ──────────┘
```

![taskQ package architecture](taskQ%20package%20architecture.png)

## Status

taskQ is early and evolving. What's shipping today:

- ✅ Core types and the `Broker` contract
- ✅ [`membroker`](membroker/) — an in-memory broker for local dev and tests
- ✅ [`redisbroker`](redisbroker/) — a Redis Streams broker (consumer groups,
  `ReceiptHandle`-based Ack/Nack, `XAUTOCLAIM` crash recovery); live-Redis
  tests are gated behind `-short`
- ✅ [`pgbroker`](pgbroker/) — a Postgres broker (`SELECT … FOR UPDATE SKIP
  LOCKED`, lease-based visibility timeout with crash recovery, LISTEN/NOTIFY);
  live-Postgres tests are gated behind `-short`
- ✅ `Pool[T]` — a concurrent worker pool (configurable concurrency + backoff)
  and `Middleware[T]` composition (`Chain`, `LoggingMiddleware`,
  `RecoveryMiddleware`, plus OpenTelemetry tracing in [`otelmw`](otelmw/))
- ✅ [`taskqtest`](taskqtest/) — one shared conformance suite that all three
  brokers pass, so backends stay interchangeable
- ✅ [`saga`](saga/) — durable multi-step workflows with automatic
  compensation; needs no Broker/Pool changes, works over every backend
- 🚧 Remaining polish: richer Pool Ack/Nack observability — see the code's
  Phase-5 notes.

## Install

```sh
go get github.com/Nuvraxis/taskQ
```

Requires Go 1.26+.

## Concepts

taskQ separates the **typed producer side** from the **byte-level transport
side**, so a broker never needs to know about your payload types.

| Type | Role |
| --- | --- |
| `Broker` | The backend contract: `Enqueue`, `Dequeue`, `Ack`, `Nack`. Deliberately minimal — backend-specific behavior (delays, priority, …) lives in constructor options, not the interface. |
| `Message` | The broker-level envelope: an ID, queue name, JSON `Payload`, attempt counters, and timestamps. Brokers only ever see this. |
| `Queue[T]` | Binds a broker to a queue name and a payload type `T`. `Enqueue` marshals `T` to JSON and hands a `Message` to the broker. |
| `Task[T]` | A `Message` with its `Payload` decoded back into `T` — what a `Handler[T]` receives. |
| `Handler[T]` | `func(ctx, Task[T]) error`. Returning a non-nil error signals the task should be retried (subject to `MaxRetry`). |
| `Pool[T]` | The consumer counterpart to `Queue[T]`: runs a `Handler[T]` against dequeued tasks with N workers, Acking successes and scheduling backoff `Nack`s for failures. Configure with `WithConcurrency` / `WithBackoffStrategy`. |
| `Middleware[T]` | `func(Handler[T]) Handler[T]` — wraps a handler with cross-cutting behavior. Compose with `Chain`; built-ins: `LoggingMiddleware`, `RecoveryMiddleware`, and `otelmw.Middleware` for tracing. |
| `Saga[S]` | A durable, multi-step workflow: `[]Step[S]` run in order, with automatic reverse-order compensation if a step fails permanently. Built on `Queue[Envelope[S]]` + `Pool[Envelope[S]]` — no Broker changes required. |

### Acknowledgement model

- **`Ack`** means *done — remove it*, whether the task succeeded or the caller
  gave up after exhausting retries.
- **`Nack`** means *requeue for redelivery*. It does **not** decide retry
  policy; the consumer does, by comparing `Message.Attempts` against
  `Message.MaxRetry` and passing the count it wants persisted on redelivery.

### Retry budget

`MaxRetry` is the number of redeliveries allowed after the first attempt, so a
task is delivered at most `MaxRetry + 1` times. Set a default per queue with
`WithDefaultMaxRetry`, or override it per call with `WithMaxRetry`.

## Quick start

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/membroker"
)

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func main() {
	ctx := context.Background()

	broker := membroker.New()
	defer broker.Close()

	// Producer: a typed queue over the broker.
	emails := taskq.NewQueue[EmailJob](broker, "emails")
	if err := emails.Enqueue(ctx, EmailJob{To: "a@example.com", Subject: "Hi"}); err != nil {
		log.Fatal(err)
	}

	// Consumer: dequeue a Message, decode it, run your handler, then Ack.
	msg, err := broker.Dequeue(ctx, "emails")
	if err != nil {
		log.Fatal(err)
	}

	var job EmailJob
	if err := json.Unmarshal(msg.Payload, &job); err != nil {
		log.Fatal(err)
	}
	log.Printf("sending email to %s: %q", job.To, job.Subject)

	if err := broker.Ack(ctx, *msg); err != nil && !errors.Is(err, taskq.ErrQueueClosed) {
		log.Fatal(err)
	}
}
```

## Consuming with `Pool` and middleware

The quick start dequeues by hand to show the mechanics; in practice you hand a
`Handler[T]` to a `Pool`, which runs it across N workers and turns failures
into backoff-spaced `Nack`s for you. Wrap the handler with `Chain` to add
cross-cutting behavior:

```go
handler := taskq.Chain[EmailJob](
	func(ctx context.Context, task taskq.Task[EmailJob]) error {
		// ... your work ...
		return nil
	},
	taskq.LoggingMiddleware[EmailJob](nil), // nil → slog.Default(); logs outcome + duration
	taskq.RecoveryMiddleware[EmailJob](),   // innermost: a panic becomes a retryable error
)

pool := taskq.NewPool(broker, "emails", handler,
	taskq.WithConcurrency(8),
	taskq.WithBackoffStrategy(taskq.ExponentialBackoff{
		Base: time.Second, Max: 30 * time.Second, Factor: 2, Jitter: true,
	}),
)

// Run blocks until ctx is cancelled or the broker is closed, then waits for
// in-flight handlers to finish.
if err := pool.Run(ctx); err != nil {
	log.Fatal(err)
}
```

**Order matters.** `Chain(base, A, B, C)` runs as `A(B(C(base)))` — the first
middleware listed is outermost. Put `RecoveryMiddleware` **last** (innermost,
directly wrapping the handler) so the `*PanicError` it produces is visible to
the logging/tracing middleware wrapped around it. For distributed tracing, add
[`otelmw.Middleware`](otelmw/) between logging and recovery.

**Toggling tracing.** `otelmw` honors the standard OpenTelemetry
`OTEL_SDK_DISABLED` environment variable (set it to `true` to turn tracing
off). Use the env-gated constructors so you don't need an `if` at the call
site — `otelmw.MiddlewareFromEnv[T](tracer)` and
`otelmw.NewTracingBrokerFromEnv(broker, tracer)` become no-op passthroughs
when tracing is disabled:

```go
handler := taskq.Chain[EmailJob](
	base,
	taskq.LoggingMiddleware[EmailJob](nil),
	otelmw.MiddlewareFromEnv[EmailJob](tracer), // no-op if OTEL_SDK_DISABLED=true
	taskq.RecoveryMiddleware[EmailJob](),
)
```

See [`examples/`](examples/) for complete, runnable programs — the worker pool,
middleware, and exponential-backoff retries. The in-memory set needs no setup;
the Redis and Postgres sets need a running server (see each backend's README):

```sh
# in-memory (zero setup)
go run ./examples/membroker/basic
go run ./examples/membroker/retry
go run ./examples/membroker/pool
go run ./examples/membroker/middleware

# Redis Streams (needs Redis on localhost:6379, or set TASKQ_REDIS_ADDR)
go run ./examples/redisbroker/basic
go run ./examples/redisbroker/retry
go run ./examples/redisbroker/pool

# Postgres (needs Postgres on localhost:5432, or set TASKQ_POSTGRES_DSN)
go run ./examples/pgbroker/basic
go run ./examples/pgbroker/pool

# Sagas (zero setup — uses membroker)
go run ./examples/saga/basic        # happy path: reserve → charge → ship
go run ./examples/saga/compensate   # a step fails, watch it roll back
```

## In-memory broker

`membroker` is a zero-config `Broker` for tests and local development:

```go
broker := membroker.New()
defer broker.Close()
```

Characteristics:

- **FIFO** per queue; `Nack` requeues at the back.
- **Blocking `Dequeue`** — it waits until a message is available, the context
  is cancelled, or the broker is closed.
- **No persistence and no visibility timeout** — `Dequeue` removes a message
  immediately, so a crash between `Dequeue` and `Ack` loses it. Fine for dev
  and tests; not intended for production.
- **`Close`** unblocks pending `Dequeue` calls (they return
  `taskq.ErrQueueClosed`) but lets already-buffered messages drain first.

## Redis broker

`redisbroker` maps each queue to a Redis Stream and reads through a consumer
group, so `Ack`/`Nack` target the exact entry via `ReceiptHandle`. You pass a
`*redis.Client` (which the broker never closes) and a running Redis:

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
defer client.Close()

broker := redisbroker.New(client, redisbroker.WithConsumerGroup("workers"))
```

| Option | Default | Purpose |
| --- | --- | --- |
| `WithKeyPrefix(p)` | `"taskq:"` | Prefix for stream keys — namespacing on a shared Redis. |
| `WithConsumerGroup(name)` | `"taskq-workers"` | Consumer group; brokers sharing it compete for a stream's entries. |
| `WithConsumerName(name)` | random UUID | This consumer's identity within the group; also which consumer an `XAUTOCLAIM`-reclaimed entry is reassigned to. |
| `WithBlockTimeout(d)` | `5s` | How long one `XREADGROUP` blocks before `Dequeue` re-checks the context. |
| `WithClaimMinIdle(d)` | `30s` | Crash recovery threshold — how long an entry must sit unacknowledged in another consumer's Pending Entries List before `Dequeue` reclaims it via `XAUTOCLAIM`. |

There's no `Close()`/`ErrQueueClosed`: cancel the context to stop a consumer.
**Crash recovery is built in** — `Dequeue` opportunistically sweeps for one
stale pending entry (idle past `WithClaimMinIdle`) via `XAUTOCLAIM` before
falling back to reading new entries, so a crashed consumer's stranded work
is redelivered automatically; no separate reaper process or ticker needed.

## Postgres broker

`pgbroker` stores messages in one `taskq_messages` table and dequeues with
`SELECT … FOR UPDATE SKIP LOCKED`, so many workers pull concurrently without
stepping on each other. A claimed row is hidden by a **lease** (visibility
timeout) until it's Acked or the lease expires — which is how it recovers a
crashed worker's message, with no separate reaper. `Dequeue` blocks on
`LISTEN/NOTIFY`, falling back to polling.

### Setup

pgbroker takes a caller-owned `*pgxpool.Pool` (it never closes it) and needs
its table to exist — run [`pgbroker/schema.sql`](pgbroker/schema.sql) once
(all `CREATE … IF NOT EXISTS`); there's no migration tooling yet:

```go
pool, err := pgxpool.New(ctx, os.Getenv("TASKQ_POSTGRES_DSN"))
if err != nil { /* ... */ }
defer pool.Close()

// once per database, e.g. `psql -f pgbroker/schema.sql`
broker := pgbroker.New(pool, pgbroker.WithLeaseDuration(60*time.Second))
```

Start a throwaway Postgres with:

```sh
docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16
```

### Config

| Option | Default | Purpose |
| --- | --- | --- |
| `WithLeaseDuration(d)` | `30s` | Visibility timeout — how long a claimed message stays invisible before its lease expires and it's redelivered. Set it comfortably above your handler's worst-case runtime, or an in-flight message gets redelivered to a second worker. |
| `WithPollInterval(d)` | `200ms` | Ceiling on `Dequeue` wake-up latency — the fallback when a `NOTIFY` is missed or never sent (e.g. a lease expiring with no new Enqueue to trigger one). |
| `WithNotifyChannel(name)` | `"taskq_new_message"` | Postgres `NOTIFY` channel. It's cluster-wide, not table-scoped, so set a distinct name if multiple taskQ deployments share one database. |
| `WithQueuePrefix(prefix)` | `""` | Namespaces queue names at the storage layer (every row carries `prefix+queue`). Mainly for tests sharing one database; the `Message.Queue` returned to callers is always the unprefixed name. |

### Notes

- **No `Close()`/`ErrQueueClosed`** — cancel the context to stop a consumer or `Pool.Run` (same as redisbroker).
- **Crash recovery is built in** — an expired lease makes the row claimable again on the next `Dequeue`; no reaper process required.
- **PgBouncer** in transaction-pooling mode doesn't forward `LISTEN/NOTIFY`, so every wake-up degrades to the poll interval (correctness is unaffected — only latency).
- **Admin methods** beyond the `Broker` interface: `QueueDepth`, `QueueDepthAvailable`, `PeekMessages`, `ReapExpiredLease`, `PurgeQueue`, `ListQueues`, `PurgeAll` — for monitoring and cleanup.

## Sagas

A [`saga`](saga/) is a durable multi-step workflow: steps run in order, and if
one fails permanently after earlier steps already succeeded, those earlier
steps are rolled back — their compensations run in reverse order. It's built
entirely on the primitives above. A saga run is just a `Queue[Envelope[S]]`
enqueuing to itself, one hop per step, so it needs no `Broker` or `Pool`
changes and runs unmodified over membroker, redisbroker, or pgbroker.

`Saga[S].Handler()` returns an ordinary `Handler[Envelope[S]]` that you run
through an ordinary `Pool`. Each call processes exactly one hop — one step, in
one direction — then returns; the next hop always lives in the queue, never in
a goroutine's stack. That's what makes a saga survive a worker crash: a
restarted worker simply dequeues wherever the run left off.

Define the steps, hand `Handler()` to a `Pool`, and `Start` a run:

```go
type OrderState struct {
	OrderID       string
	ReservationID string // set by reserve, read by its Compensate
	ChargeID      string // set by charge, read by its Compensate
}

steps := []saga.Step[OrderState]{
	{
		Name:       "reserve-inventory",
		Do:         func(ctx context.Context, s *OrderState) error { s.ReservationID = "resv-" + s.OrderID; return nil },
		Compensate: func(ctx context.Context, s *OrderState) error { /* release s.ReservationID */ return nil },
	},
	{
		Name:       "charge-payment",
		Do:         func(ctx context.Context, s *OrderState) error { s.ChargeID = "charge-" + s.OrderID; return nil },
		Compensate: func(ctx context.Context, s *OrderState) error { /* refund s.ChargeID */ return nil },
	},
	{
		Name: "ship-order",
		Do:   func(ctx context.Context, s *OrderState) error { return ship(s) },
		// No Compensate: the last step has nothing to undo. A nil Compensate
		// is an automatic no-op if rollback ever reaches it.
	},
}

sg := saga.New(broker, "orders", steps,
	saga.WithOnComplete(func(ctx context.Context, id string, s OrderState) { /* finished */ }),
	saga.WithOnFailed(func(ctx context.Context, id string, s OrderState, failedStep string, cause error) {
		// failedStep/cause describe the step whose Do originally failed —
		// even after rollback has unwound all the way back to step 0.
	}),
)

// Drive it with an ordinary Pool — nothing saga-specific here.
pool := taskq.NewPool(broker, "orders", sg.Handler())
go pool.Run(ctx)

sagaID, err := sg.Start(ctx, OrderState{OrderID: "order-1001"})
```

Semantics worth knowing:

- **Steps run in order; compensation runs in reverse.** If step 2 of 3 fails
  permanently, only the steps that actually succeeded (0 and 1) are
  compensated, in reverse (1 then 0) — the failed step has nothing of its own
  to undo. A step whose `Compensate` is `nil` is skipped as a no-op.
- **The retry budget is per hop.** `WithMaxRetry(n)` (default 3) applies to
  each individual `Do` or `Compensate` call — same `MaxRetry + 1`-attempts
  semantics as the rest of taskQ — not to the run as a whole.
- **Hooks report the real outcome.** `WithOnComplete` fires once every step's
  `Do` succeeds. `WithOnFailed` fires when a run doesn't complete, always
  reporting the step whose `Do` *originally* failed (not whichever
  `Compensate` finished last). `WithOnCompensationFailed` fires when a
  `Compensate` itself exhausts its retries — the one outcome a saga can't
  resolve on its own, leaving the run partly rolled back. Treat it as
  page-a-human, not log-and-continue.

See [`examples/saga`](examples/saga/) for runnable happy-path and rollback
demos.

## Development

Common tasks are wrapped in the [`Makefile`](Makefile) (POSIX-shell first; on
Windows use Git Bash or WSL2):

```sh
make build     # go build ./...
make test      # go test ./...
make test-race # go test -race ./...   (needs a C toolchain)
make vet       # go vet ./...
make fmt       # gofmt -l -w .
make lint      # golangci-lint run
```

Run the examples with `go run` — or `make run EX=<path>`. The `membroker/*` and
`saga/*` examples need no setup; `redisbroker/*` needs a running Redis and
`pgbroker/*` a running Postgres (`make deps-up`, or the `docker run` commands
in each backend's README). See [`examples/`](examples/):

```sh
go run ./examples/membroker/pool       # or: make run EX=membroker/pool
go run ./examples/redisbroker/pool     # or: make run EX=redisbroker/pool
go run ./examples/pgbroker/pool        # or: make run EX=pgbroker/pool
go run ./examples/saga/compensate      # or: make run EX=saga/compensate
```

CI runs build/vet/tidy, `go test -short`, the race detector, `golangci-lint`,
and `govulncheck` across Linux, macOS, and Windows.

## Need Support

Questions, bug reports, or feature requests? Open an issue on the repository,
or reach out to the maintainers at [hello@nuvraxis.com](mailto:hello@nuvraxis.com).

## License

MIT — see [`LICENSE`](LICENSE).
