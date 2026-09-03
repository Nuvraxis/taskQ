// Command basic is the smallest useful taskQ program: enqueue a batch of
// typed jobs onto the in-memory broker, fan them out to a few worker
// goroutines, and shut down cleanly once the queue drains.
//
// Run it with:
//
//	go run ./examples/membroker/basic
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/membroker"
)

// EmailJob is the payload type carried by tasks on the "emails" queue.
// Queue[EmailJob] marshals it to JSON on Enqueue; the worker unmarshals it
// back on the way out.
type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func main() {
	ctx := context.Background()

	// The in-memory broker needs no external service — ideal for local dev,
	// tests, and examples. Close it when you're done so blocked Dequeue
	// calls return and the workers can exit.
	broker := membroker.New()

	// Bind the broker to a queue name and a payload type.
	emails := taskq.NewQueue[EmailJob](broker, "emails")

	// Start the workers first. Dequeue blocks until a message arrives, so
	// they simply wait until we start producing.
	const numWorkers = 3
	var wg sync.WaitGroup
	for id := 0; id < numWorkers; id++ {
		wg.Go(func() {
			work(ctx, broker, "emails", func(_ context.Context, task taskq.Task[EmailJob]) error {
				log.Printf("worker %d: sending email to %s — %q",
					id, task.Payload.To, task.Payload.Subject)
				return nil // pretend the send succeeded
			})
		})
	}

	// Produce a batch of jobs.
	for i := 1; i <= 6; i++ {
		job := EmailJob{
			To:      fmt.Sprintf("user%d@example.com", i),
			Subject: fmt.Sprintf("Welcome, user %d!", i),
		}
		if err := emails.Enqueue(ctx, job); err != nil {
			log.Fatalf("enqueue: %v", err)
		}
	}

	// Let the workers drain the queue, then close. After Close, Dequeue on
	// an empty queue returns ErrQueueClosed, which is the workers' cue to
	// stop.
	time.Sleep(100 * time.Millisecond)
	broker.Close()
	wg.Wait()

	log.Println("all jobs processed")
}

// work is a minimal consume loop. It dequeues a raw Message, decodes it into
// a typed Task[T], and hands it to h. This bridges the Broker's byte-level
// Message to the typed Handler[T] — the same job a built-in worker pool will
// eventually do for you.
//
// For clarity this example always Acks; see the "retry" example for how to
// requeue failed tasks with Nack up to MaxRetry times.
func work[T any](ctx context.Context, b *membroker.Broker, queue string, h taskq.Handler[T]) {
	for {
		msg, err := b.Dequeue(ctx, queue)
		if err != nil {
			if errors.Is(err, taskq.ErrQueueClosed) {
				return // queue closed and fully drained — clean exit
			}
			log.Printf("dequeue: %v", err)
			return
		}

		var payload T
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			log.Printf("dropping undecodable message %s: %v", msg.ID, err)
			_ = b.Ack(ctx, *msg) // can't retry what we can't decode
			continue
		}

		task := taskq.Task[T]{
			ID:         msg.ID,
			Queue:      msg.Queue,
			Payload:    payload,
			Attempts:   msg.Attempts,
			MaxRetry:   msg.MaxRetry,
			EnqueuedAt: msg.EnqueuedAt,
		}

		if err := h(ctx, task); err != nil {
			log.Printf("handler error for %s: %v", msg.ID, err)
		}
		_ = b.Ack(ctx, *msg)
	}
}
