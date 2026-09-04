// Command pool shows a reusable worker Pool that consumes a queue with a
// fixed number of workers and retries failed tasks using exponential backoff
// with jitter. It's a preview of the built-in worker pool taskQ plans to
// ship — until then, this is the pattern to copy.
//
// The membroker has no delayed-delivery / visibility timeout, so "backoff"
// here means the worker waits before requeuing with Nack (which puts the task
// at the back of the queue). A production broker would instead hand the task
// back with a not-before timestamp so the worker isn't tied up sleeping.
//
// Run it with:
//
//	go run ./examples/membroker/pool
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/membroker"
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

// run wires up the broker, pool, and producer. It returns errors rather than
// calling log.Fatal directly, so its deferred cleanup (cancel) always runs —
// log.Fatal would call os.Exit and skip every pending defer.
func run() error {
	// A cancellable context is the pool's shutdown signal: cancelling it
	// stops workers that are blocked in Dequeue or sleeping between retries.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker := membroker.New()
	payments := taskq.NewQueue[PaymentJob](broker, "payments", taskq.WithDefaultMaxRetry(4))

	// settled counts tasks that have reached a terminal state (succeeded or
	// given up), so run knows when it's safe to shut the pool down.
	var settled sync.WaitGroup

	pool := &Pool[PaymentJob]{
		broker:  broker,
		queue:   "payments",
		workers: 3,
		handler: charge,
		// Exponential backoff with full jitter: 50ms, 100ms, 200ms, … capped
		// at 1s, each randomized to spread retries out.
		backoff: expoBackoff(50*time.Millisecond, 1*time.Second),
		// label makes the (otherwise generic) pool logs human-readable.
		label: func(t taskq.Task[PaymentJob]) string { return t.Payload.OrderID },
		onSettled: func(t taskq.Task[PaymentJob], err error) {
			if err != nil {
				log.Printf("  ✗ %s dead-lettered after %d attempts: %v", t.Payload.OrderID, t.Attempts+1, err)
			} else {
				log.Printf("  ✓ %s charged ($%.2f) after %d attempt(s)", t.Payload.OrderID, float64(t.Payload.Amount)/100, t.Attempts+1)
			}
			settled.Done()
		},
	}

	poolDone := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(poolDone)
	}()

	// Produce a batch: five flaky charges that succeed on their third
	// attempt, plus one that always fails (capped at 2 retries so it's
	// dead-lettered quickly instead of looping).
	for i := 1; i <= 5; i++ {
		settled.Add(1)
		job := PaymentJob{OrderID: fmt.Sprintf("order-%02d", i), Amount: i * 500}
		if err := payments.Enqueue(ctx, job); err != nil {
			return fmt.Errorf("enqueue: %w", err)
		}
	}
	settled.Add(1)
	if err := payments.Enqueue(ctx, PaymentJob{OrderID: "poison", Amount: 999}, taskq.WithMaxRetry(2)); err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}

	// Wait until every task settles, then shut down: cancel unblocks the
	// workers, Close drains and stops the broker, and we wait for Run.
	settled.Wait()
	cancel()
	broker.Close()
	<-poolDone

	log.Println("all tasks settled; pool stopped")
	return nil
}

// charge simulates a payment attempt. Its outcome depends on how many times
// the task has already been tried, so retries can eventually succeed.
func charge(_ context.Context, task taskq.Task[PaymentJob]) error {
	if task.Payload.OrderID == "poison" {
		return errors.New("card permanently declined")
	}
	if task.Attempts < 2 { // fail the first two attempts, succeed on the third
		return errors.New("payment gateway timeout")
	}
	return nil
}

// Pool consumes queue with a fixed number of workers, invoking handler for
// each task and retrying failures with backoff until MaxRetry is reached.
//
// It's deliberately small and self-contained — the kind of consumer you'd
// otherwise hand-write per project until taskQ ships a built-in one.
type Pool[T any] struct {
	broker  *membroker.Broker
	queue   string
	workers int
	handler taskq.Handler[T]

	// backoff returns how long to wait before the nth retry (n is 1-based).
	backoff func(attempt int) time.Duration
	// label renders a human-readable name for a task in logs; optional.
	label func(taskq.Task[T]) string
	// onSettled is called once per task when it reaches a terminal state:
	// err == nil on success, non-nil when retries are exhausted. Optional.
	onSettled func(taskq.Task[T], error)
}

// Run starts the workers and blocks until every worker exits, which happens
// once ctx is cancelled or the broker is closed and drained.
func (p *Pool[T]) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for id := 0; id < p.workers; id++ {
		wg.Go(func() { p.worker(ctx, id) })
	}
	wg.Wait()
}

func (p *Pool[T]) worker(ctx context.Context, id int) {
	for {
		msg, err := p.broker.Dequeue(ctx, p.queue)
		if err != nil {
			// ErrQueueClosed (drained) or a context error — time to stop.
			return
		}
		p.process(ctx, id, msg)
	}
}

func (p *Pool[T]) process(ctx context.Context, id int, msg *taskq.Message) {
	var payload T
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		log.Printf("worker %d: dropping undecodable %s: %v", id, msg.ID, err)
		_ = p.broker.Ack(ctx, *msg) // can't retry what we can't decode
		return
	}

	task := taskq.Task[T]{
		ID:         msg.ID,
		Queue:      msg.Queue,
		Payload:    payload,
		Attempts:   msg.Attempts,
		MaxRetry:   msg.MaxRetry,
		EnqueuedAt: msg.EnqueuedAt,
	}

	err := p.handler(ctx, task)
	if err == nil {
		_ = p.broker.Ack(ctx, *msg)
		p.settle(task, nil)
		return
	}

	// Handler failed. Give up once retries are exhausted.
	if msg.Attempts >= msg.MaxRetry {
		_ = p.broker.Ack(ctx, *msg) // remove it; a real system would dead-letter
		p.settle(task, err)
		return
	}

	// Otherwise wait out the backoff, then requeue with an incremented count.
	attempt := msg.Attempts + 1
	delay := p.backoff(attempt)
	log.Printf("worker %d: %s failed (%v) — retry %d/%d in %s",
		id, p.name(task), err, attempt, msg.MaxRetry, delay.Round(time.Millisecond))

	// If we're shutting down mid-wait, requeue immediately so the task isn't
	// lost, then let the worker exit on its next Dequeue.
	_ = sleepCtx(ctx, delay)
	msg.Attempts++
	_ = p.broker.Nack(ctx, *msg, err)
}

func (p *Pool[T]) settle(task taskq.Task[T], err error) {
	if p.onSettled != nil {
		p.onSettled(task, err)
	}
}

func (p *Pool[T]) name(task taskq.Task[T]) string {
	if p.label != nil {
		return p.label(task)
	}
	return task.ID
}

// expoBackoff returns an exponential backoff with full jitter: the nth retry
// waits a random duration in [0, min(ceiling, base*2^(n-1))]. Full jitter
// (see AWS's "Exponential Backoff And Jitter") spreads retries out so a burst
// of failures doesn't turn into a synchronized retry storm.
func expoBackoff(base, ceiling time.Duration) func(attempt int) time.Duration {
	return func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		window := base << (attempt - 1) // base * 2^(attempt-1)
		if window <= 0 || window > ceiling {
			window = ceiling // overflowed, or past the cap
		}
		return time.Duration(rand.Int64N(int64(window) + 1))
	}
}

// sleepCtx waits for d or until ctx is done, whichever comes first. It
// reports true if the full duration elapsed, false if ctx was cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
