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
  `ReceiptHandle`-based Ack/Nack); live-Redis tests are gated behind `-short`
- ✅ `Pool[T]` — a concurrent worker pool (configurable concurrency + backoff)
  and `Middleware[T]` composition (`Chain`, `LoggingMiddleware`,
  `RecoveryMiddleware`, plus OpenTelemetry tracing in [`otelmw`](otelmw/))
- 🚧 A Postgres broker and a shared conformance test suite are still planned
  (you'll see them referenced in the `Makefile` and CI).

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
the Redis set needs a running Redis (see its [README](examples/redisbroker/)):

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

Run the examples with `go run` — or `make run EX=<path>`. The `membroker/*`
examples need no setup; the `redisbroker/*` examples need a running Redis
(`make deps-up`, or `docker run --rm -p 6379:6379 redis:7`). See
[`examples/`](examples/):

```sh
go run ./examples/membroker/pool       # or: make run EX=membroker/pool
go run ./examples/redisbroker/pool     # or: make run EX=redisbroker/pool
```

CI runs build/vet/tidy, `go test -short`, the race detector, `golangci-lint`,
and `govulncheck` across Linux, macOS, and Windows.

## License

See the repository for license details.
