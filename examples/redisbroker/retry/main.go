// Command retry demonstrates redisbroker retry semantics with a single
// worker. On failure the worker increments Attempts and Nacks; on Redis
// Streams that writes a *new* entry (with a fresh ReceiptHandle) at the back
// of the stream and acknowledges the old one — so a redelivery is a genuine
// new entry, not an in-place requeue. Retries continue until MaxRetry, after
// which the task is dead-lettered (Ack, which removes it).
//
// Requires a running Redis (default localhost:6379, override with
// TASKQ_REDIS_ADDR):
//
//	docker run --rm -p 6379:6379 redis:7      # or: make deps-up
//
// Run it with:
//
//	go run ./examples/redisbroker/retry
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/redisbroker"
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
	client, ok := dialRedis()
	if !ok {
		return nil // Redis unreachable — guidance already printed, skip cleanly
	}
	defer func() { _ = client.Close() }()

	// Cancelling ctx is how we stop the worker — redisbroker has no Close().
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A short block timeout keeps Dequeue responsive to cancellation even
	// when the stream is momentarily empty between retries.
	broker := redisbroker.New(client,
		redisbroker.WithKeyPrefix("taskq:example:"),
		redisbroker.WithBlockTimeout(500*time.Millisecond),
	)
	payments := taskq.NewQueue[PaymentJob](broker, "payments", taskq.WithDefaultMaxRetry(5))

	// settled counts tasks that reach a terminal state (succeeded or given
	// up), so we know when it's safe to stop the worker.
	var settled sync.WaitGroup
	onSettled := func(t taskq.Task[PaymentJob], err error) {
		if err != nil {
			log.Printf("  ✗ %s dead-lettered after %d attempts: %v", t.Payload.OrderID, t.Attempts+1, err)
		} else {
			log.Printf("  ✓ %s charged after %d attempt(s)", t.Payload.OrderID, t.Attempts+1)
		}
		settled.Done()
	}

	var wg sync.WaitGroup
	wg.Go(func() { work(ctx, broker, "payments", charge, onSettled) })

	// A transient failure: fails twice, then succeeds (queue default: 5 retries).
	settled.Add(1)
	if err := payments.Enqueue(ctx, PaymentJob{OrderID: "order-42", Amount: 1999}); err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}
	// A permanent failure, capped at 2 retries so it's dead-lettered quickly.
	settled.Add(1)
	if err := payments.Enqueue(ctx, PaymentJob{OrderID: "poison", Amount: 500}, taskq.WithMaxRetry(2)); err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}

	settled.Wait()
	cancel()
	wg.Wait()
	return nil
}

// charge simulates a payment. Its outcome depends on how many times the task
// has already been attempted, so retries can eventually succeed.
func charge(_ context.Context, task taskq.Task[PaymentJob]) error {
	if task.Payload.OrderID == "poison" {
		return errors.New("card permanently declined")
	}
	if task.Attempts < 2 { // fail the first two attempts, succeed on the third
		return errors.New("payment gateway timeout")
	}
	return nil
}

// work is a single-worker consume loop with retry handling. On failure it
// requeues with an incremented attempt count (Nack) while retries remain, and
// gives up (Ack) once Attempts reaches MaxRetry. It stops when ctx is
// cancelled, which makes Dequeue return a context error.
func work(
	ctx context.Context,
	b *redisbroker.Broker,
	queue string,
	h taskq.Handler[PaymentJob],
	onSettled func(taskq.Task[PaymentJob], error),
) {
	for {
		msg, err := b.Dequeue(ctx, queue)
		if err != nil {
			return // ctx cancelled (shutdown) or a redis error — stop
		}

		var payload PaymentJob
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			log.Printf("dropping undecodable %s: %v", msg.ID, err)
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
				msg.Attempts++
				log.Printf("%s failed (%v) — requeueing, attempt %d/%d",
					payload.OrderID, err, msg.Attempts, msg.MaxRetry)
				_ = b.Nack(ctx, *msg, err) // writes a new stream entry at the back
				continue
			}
			_ = b.Ack(ctx, *msg) // retries exhausted — remove it
			onSettled(task, err)
			continue
		}

		_ = b.Ack(ctx, *msg)
		onSettled(task, nil)
	}
}

// dialRedis builds a client from TASKQ_REDIS_ADDR (default localhost:6379)
// and pings it, returning ok=false with guidance when Redis is unreachable.
func dialRedis() (*redis.Client, bool) {
	addr := os.Getenv("TASKQ_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		log.Printf("redis not reachable at %s: %v", addr, err)
		log.Printf("start one with:  docker run --rm -p 6379:6379 redis:7   (or: make deps-up)")
		return nil, false
	}
	log.Printf("connected to redis at %s", addr)
	return client, true
}
