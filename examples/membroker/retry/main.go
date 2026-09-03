// Command retry demonstrates taskQ's retry semantics on the in-memory
// broker. The broker itself has no retry policy — Nack simply requeues a
// message verbatim — so the worker loop owns the "how many times" decision,
// comparing Message.Attempts against Message.MaxRetry.
//
// Two jobs are enqueued:
//   - "order-42" fails twice with a transient error, then succeeds.
//   - "poison"   fails permanently and is given up on once retries run out.
//
// Run it with:
//
//	go run ./examples/membroker/retry
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
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
	ctx := context.Background()

	broker := membroker.New()

	// WithDefaultMaxRetry sets the retry budget for every task enqueued
	// through this queue. Individual Enqueue calls can override it with
	// WithMaxRetry (see the "poison" job below).
	payments := taskq.NewQueue[PaymentJob](broker, "payments", taskq.WithDefaultMaxRetry(5))

	// One worker keeps the output easy to follow; scale this up freely.
	var wg sync.WaitGroup
	wg.Go(func() {
		work(ctx, broker, "payments", handle)
	})

	// A transient failure: fails twice, then succeeds on the third attempt.
	// Uses the queue default of 5 retries.
	if err := payments.Enqueue(ctx, PaymentJob{OrderID: "order-42", Amount: 1999}); err != nil {
		log.Fatalf("enqueue: %v", err)
	}

	// A permanent failure: always errors. Capped at 2 retries (3 attempts
	// total) so it doesn't loop forever before being dead-lettered.
	if err := payments.Enqueue(ctx, PaymentJob{OrderID: "poison", Amount: 500}, taskq.WithMaxRetry(2)); err != nil {
		log.Fatalf("enqueue: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	broker.Close()
	wg.Wait()
}

// handle simulates charging a payment. Its success depends on how many times
// the task has already been attempted, so retries can eventually succeed.
func handle(_ context.Context, task taskq.Task[PaymentJob]) error {
	attempt := task.Attempts + 1 // this delivery is attempt number Attempts+1

	if task.Payload.OrderID == "poison" {
		log.Printf("[attempt %d] charge %s: card declined (permanent)", attempt, task.Payload.OrderID)
		return errors.New("card declined")
	}

	// Transient glitch on the first two attempts, success on the third.
	if task.Attempts < 2 {
		log.Printf("[attempt %d] charge %s: gateway timeout, will retry", attempt, task.Payload.OrderID)
		return errors.New("gateway timeout")
	}

	log.Printf("[attempt %d] charge %s: success ($%.2f)", attempt, task.Payload.OrderID, float64(task.Payload.Amount)/100)
	return nil
}

// work is a consume loop with retry handling. On a handler error it requeues
// the task with an incremented attempt count while retries remain, and gives
// up (Ack, removing it for good) once Attempts reaches MaxRetry. In a real
// system the "give up" branch is where you'd write to a dead-letter queue.
func work(ctx context.Context, b *membroker.Broker, queue string, h taskq.Handler[PaymentJob]) {
	for {
		msg, err := b.Dequeue(ctx, queue)
		if err != nil {
			if errors.Is(err, taskq.ErrQueueClosed) {
				return // queue closed and fully drained — clean exit
			}
			log.Printf("dequeue: %v", err)
			return
		}

		var payload PaymentJob
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			log.Printf("dropping undecodable message %s: %v", msg.ID, err)
			_ = b.Ack(ctx, *msg)
			continue
		}

		task := taskq.Task[PaymentJob]{
			ID:         msg.ID,
			Queue:      msg.Queue,
			Payload:    payload,
			Attempts:   msg.Attempts,
			MaxRetry:   msg.MaxRetry,
			EnqueuedAt: msg.EnqueuedAt,
		}

		if err := h(ctx, task); err != nil {
			if msg.Attempts < msg.MaxRetry {
				msg.Attempts++             // persist the incremented count on redelivery
				_ = b.Nack(ctx, *msg, err) // requeue at the back of the queue
			} else {
				log.Printf("dead-letter %s (%s): gave up after %d attempts: %v",
					msg.ID, payload.OrderID, msg.Attempts+1, err)
				_ = b.Ack(ctx, *msg) // retries exhausted — remove it
			}
			continue
		}

		_ = b.Ack(ctx, *msg)
	}
}
