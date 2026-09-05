// Command pool runs the built-in taskq.Pool against pgbroker with a
// middleware chain. Several workers dequeue concurrently — Postgres'
// SELECT ... FOR UPDATE SKIP LOCKED hands each row to exactly one worker —
// and failures retry with backoff. Shutdown is context-driven: pgbroker, like
// redisbroker, has no Close(), so a cancelled context is how Pool.Run stops.
//
// Requires a running Postgres (default DSN
// postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable,
// override with TASKQ_POSTGRES_DSN):
//
//	docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16
//
// Run it with:
//
//	go run ./examples/pgbroker/pool
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/examples/pgbroker/internal/pgdemo"
	"github.com/Nuvraxis/taskQ/pgbroker"
)

// PaymentJob is the payload type carried by tasks on the "payments" queue.
type PaymentJob struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"` // in cents
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, ok := pgdemo.Connect(ctx)
	if !ok {
		return nil // Postgres unreachable — guidance printed, skip cleanly
	}
	defer pool.Close()

	// WithLeaseDuration is pgbroker's visibility timeout: how long a claimed
	// message stays invisible before its lease expires and it's redelivered
	// (crash recovery, no reaper needed). Keep it comfortably above the
	// handler's worst-case runtime. WithQueuePrefix isolates this example's
	// rows; a short WithPollInterval keeps shutdown snappy.
	broker := pgbroker.New(pool,
		pgbroker.WithQueuePrefix("example:"),
		pgbroker.WithLeaseDuration(30*time.Second),
		pgbroker.WithPollInterval(100*time.Millisecond),
	)
	defer func() { _, _ = broker.PurgeQueue(context.Background(), "payments") }()

	payments := taskq.NewQueue[PaymentJob](broker, "payments", taskq.WithDefaultMaxRetry(3))

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var settled sync.WaitGroup
	handler := taskq.Chain[PaymentJob](
		charge,
		taskq.LoggingMiddleware[PaymentJob](logger), // outermost: logs outcome + duration
		recorder[PaymentJob](settled.Done),          // custom: signals completion on terminal outcomes
		taskq.RecoveryMiddleware[PaymentJob](),      // innermost: a panic becomes a *PanicError
	)

	// Four workers competing for the same queue; SKIP LOCKED guarantees each
	// row goes to exactly one of them.
	p := taskq.NewPool(broker, "payments", handler,
		taskq.WithConcurrency(4),
		taskq.WithBackoffStrategy(taskq.ExponentialBackoff{
			Base: 25 * time.Millisecond, Max: 500 * time.Millisecond, Factor: 2, Jitter: true,
		}),
	)

	runErr := make(chan error, 1)
	go func() { runErr <- p.Run(ctx) }()

	// Five flaky charges (succeed on the third attempt) plus one that always
	// panics (recovered, then given up after 1 retry).
	type enq struct {
		job  PaymentJob
		opts []taskq.EnqueueOption
	}
	batch := []enq{
		{job: PaymentJob{OrderID: "order-01", Amount: 500}},
		{job: PaymentJob{OrderID: "order-02", Amount: 1000}},
		{job: PaymentJob{OrderID: "order-03", Amount: 1500}},
		{job: PaymentJob{OrderID: "order-04", Amount: 2000}},
		{job: PaymentJob{OrderID: "order-05", Amount: 2500}},
		{job: PaymentJob{OrderID: "poison", Amount: 999}, opts: []taskq.EnqueueOption{taskq.WithMaxRetry(1)}},
	}
	settled.Add(len(batch))
	for _, e := range batch {
		if err := payments.Enqueue(ctx, e.job, e.opts...); err != nil {
			return fmt.Errorf("enqueue %s: %w", e.job.OrderID, err)
		}
	}

	// Wait for every task to settle, then cancel so Pool.Run returns.
	settled.Wait()
	cancel()
	if err := <-runErr; err != nil {
		return fmt.Errorf("pool.Run: %w", err)
	}

	log.Println("all tasks settled; pool stopped")
	return nil
}

// charge simulates a payment. Its outcome depends on how many times the task
// has already been tried, so retries can eventually succeed.
func charge(_ context.Context, task taskq.Task[PaymentJob]) error {
	if task.Payload.OrderID == "poison" {
		panic("connection pool exhausted")
	}
	if task.Attempts < 2 { // fail the first two attempts, succeed on the third
		return errors.New("payment gateway timeout")
	}
	return nil
}

// recorder is a small Middleware[T] that calls done exactly once per task,
// when it reaches a terminal state (succeeded, or failed with no retries
// left) — which is how run() knows when to shut the pool down.
func recorder[T any](done func()) taskq.Middleware[T] {
	return func(next taskq.Handler[T]) taskq.Handler[T] {
		return func(ctx context.Context, task taskq.Task[T]) error {
			err := next(ctx, task)
			// task.Attempts is the delivery count before this attempt; the
			// Pool gives up once it reaches MaxRetry.
			if err == nil || task.Attempts >= task.MaxRetry {
				done()
			}
			return err
		}
	}
}
