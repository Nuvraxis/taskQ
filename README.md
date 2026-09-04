# taskQ

A small, type-safe task queue for Go, built around a deliberately tiny broker
interface. Producers enqueue strongly-typed payloads; a pluggable broker
handles delivery, acknowledgement, and requeue. The core has no dependencies
beyond [`google/uuid`](https://github.com/google/uuid).

```
Queue[T]  ──Enqueue──▶  Broker  ──Dequeue──▶  worker  ──▶  Handler[T]
   (marshals T→JSON)    (transport)          (decode JSON→T)   (your code)
                              ▲                     │
                              └──── Ack / Nack ─────┘
```

![taskQ package architecture](taskQ%20package%20architecture.png)

## Status

taskQ is early and evolving. What's shipping today:

- ✅ Core types and the `Broker` contract
- ✅ [`membroker`](membroker/) — an in-memory broker for local dev and tests
- ✅ [`redisbroker`](redisbroker/) — a Redis Streams broker (consumer groups,
  `ReceiptHandle`-based Ack/Nack); live-Redis tests are gated behind `-short`
- 🚧 A Postgres broker, a conformance test suite, and a built-in worker pool
  are planned (you'll see them referenced in the `Makefile` and CI). Until the
  worker pool lands, consuming means calling the `Broker` directly — the
  [examples](examples/) show a ~30-line loop (and a reusable `Pool[T]`) that
  do exactly that.

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

See [`examples/`](examples/) for complete, runnable programs — including a
worker pool with exponential-backoff retries. The in-memory set needs no
setup; the Redis set needs a running Redis (see its
[README](examples/redisbroker/)):

```sh
# in-memory (zero setup)
go run ./examples/membroker/basic
go run ./examples/membroker/retry
go run ./examples/membroker/pool

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
