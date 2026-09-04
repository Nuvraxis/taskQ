// Command middleware shows how to wrap a Handler[T] with taskQ middleware:
// structured logging, a hand-written recorder, and panic recovery — composed
// with taskq.Chain and run by the built-in taskq.Pool.
//
// Middleware order matters. taskq.Chain(base, A, B, C) runs as A(B(C(base))):
// the first listed is outermost — it sees the task first and the returned
// error last. Per the package docs, RecoveryMiddleware goes LAST (innermost,
// directly wrapping the handler) so the *PanicError it produces is visible to
// the middleware listed before it — here, the recorder and the logger.
//
// Run it with:
//
//	go run ./examples/membroker/middleware
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
	"github.com/Nuvraxis/taskQ/membroker"
)

// EmailJob is the payload type carried by tasks on the "emails" queue.
type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	broker := membroker.New()

	// Queue with room for a couple of retries (the flaky job needs them).
	emails := taskq.NewQueue[EmailJob](broker, "emails", taskq.WithDefaultMaxRetry(3))

	// Structured logger for LoggingMiddleware. Info level keeps output focused
	// (LoggingMiddleware emits the per-start line at Debug).
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// A hand-written middleware needs somewhere to record into, and a way to
	// tell run() when every task has reached a terminal state.
	st := &stats{}
	var settled sync.WaitGroup

	// Compose the chain — outermost to innermost: logging → recorder → recovery.
	handler := taskq.Chain[EmailJob](
		send,
		taskq.LoggingMiddleware[EmailJob](logger), // logs outcome + duration (Warn on retry, Error on give-up)
		recorder[EmailJob](st, settled.Done),      // custom: tallies terminal outcomes, signals completion
		taskq.RecoveryMiddleware[EmailJob](),      // innermost: turns a handler panic into a *PanicError
	)

	// The built-in Pool owns the dequeue → decode → handle → ack/nack loop and
	// the retry backoff — the consumer-side counterpart to Queue[T].
	pool := taskq.NewPool(broker, "emails", handler,
		taskq.WithConcurrency(3),
		taskq.WithBackoffStrategy(taskq.ExponentialBackoff{
			Base: 20 * time.Millisecond, Max: 200 * time.Millisecond, Factor: 2, Jitter: true,
		}),
	)

	runErr := make(chan error, 1)
	go func() { runErr <- pool.Run(ctx) }()

	// A mix of outcomes to exercise each middleware.
	type enq struct {
		job  EmailJob
		opts []taskq.EnqueueOption
	}
	batch := []enq{
		{job: EmailJob{To: "ann@example.com", Subject: "welcome"}},                                                      // succeeds first try
		{job: EmailJob{To: "flaky@example.com", Subject: "receipt"}},                                                    // fails twice, then succeeds
		{job: EmailJob{To: "panic@example.com", Subject: "digest"}, opts: []taskq.EnqueueOption{taskq.WithMaxRetry(1)}}, // panics → recovered, then given up
		{job: EmailJob{To: "bob@example.com", Subject: "welcome"}},                                                      // succeeds first try
	}
	settled.Add(len(batch))
	for _, e := range batch {
		if err := emails.Enqueue(ctx, e.job, e.opts...); err != nil {
			return fmt.Errorf("enqueue %s: %w", e.job.To, err)
		}
	}

	// Wait for every task to settle, then close the broker so Pool.Run
	// returns (membroker's Dequeue reports ErrQueueClosed once drained).
	settled.Wait()
	broker.Close()
	if err := <-runErr; err != nil {
		return fmt.Errorf("pool.Run: %w", err)
	}

	fmt.Printf("\nsummary: %d succeeded, %d failed, %d panicked\n", st.succeeded, st.failed, st.panicked)
	return nil
}

// send is the business handler. Its behavior varies by recipient to exercise
// each middleware: a clean success, a transient failure that recovers on
// retry, and a panic.
func send(_ context.Context, task taskq.Task[EmailJob]) error {
	switch task.Payload.To {
	case "panic@example.com":
		panic("connection pool exhausted")
	case "flaky@example.com":
		if task.Attempts < 2 { // fail the first two attempts, succeed on the third
			return errors.New("smtp 421 service not available")
		}
	}
	return nil // pretend the send succeeded
}

// stats tallies terminal task outcomes. Guarded by a mutex because Pool runs
// the handler (and therefore this middleware) on multiple worker goroutines.
type stats struct {
	mu                          sync.Mutex
	succeeded, failed, panicked int
}

// recorder is a hand-written Middleware[T]: wrap a Handler, do work around
// next(). When a task reaches a terminal state (succeeded, or failed with no
// retries left) it tallies the outcome and calls done exactly once for that
// task — which is how run() knows when to shut down.
func recorder[T any](st *stats, done func()) taskq.Middleware[T] {
	return func(next taskq.Handler[T]) taskq.Handler[T] {
		return func(ctx context.Context, task taskq.Task[T]) error {
			err := next(ctx, task)

			// task.Attempts is the delivery count before this attempt; the
			// Pool gives up once it reaches MaxRetry (same rule the built-in
			// LoggingMiddleware uses to log "will retry" vs "giving up").
			if err != nil && task.Attempts < task.MaxRetry {
				return err // will be retried — not settled yet
			}

			st.mu.Lock()
			switch {
			case err == nil:
				st.succeeded++
			case isPanic(err):
				st.panicked++
			default:
				st.failed++
			}
			st.mu.Unlock()

			done()
			return err
		}
	}
}

// isPanic reports whether err came from RecoveryMiddleware catching a panic.
func isPanic(err error) bool {
	var pe *taskq.PanicError
	return errors.As(err, &pe)
}
