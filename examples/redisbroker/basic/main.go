// Command basic shows the redisbroker fundamentals: connect to Redis, enqueue
// typed jobs onto a stream-backed queue, read them back through a consumer
// group, and Ack each one by its ReceiptHandle (the Redis Stream entry ID
// that Dequeue records on the message).
//
// Unlike membroker, redisbroker takes a *redis.Client it does not own (you
// create and Close it) and has no Close()/ErrQueueClosed — a consumer stops
// when its context is cancelled.
//
// Requires a running Redis (default localhost:6379, override with
// TASKQ_REDIS_ADDR):
//
//	docker run --rm -p 6379:6379 redis:7      # or: make deps-up
//
// Run it with:
//
//	go run ./examples/redisbroker/basic
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/redisbroker"
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
	client, ok := dialRedis()
	if !ok {
		return nil // Redis unreachable — guidance already printed, skip cleanly
	}
	defer func() { _ = client.Close() }() // redisbroker doesn't own the client; the caller does

	ctx := context.Background()

	// WithKeyPrefix namespaces this example's stream keys so it won't collide
	// with real queues on a shared Redis. The "emails" queue maps to the
	// "taskq:example:emails" stream.
	broker := redisbroker.New(client, redisbroker.WithKeyPrefix("taskq:example:"))
	emails := taskq.NewQueue[EmailJob](broker, "emails")

	// Produce a batch of jobs (each XADDed to the stream).
	const jobs = 5
	for i := 1; i <= jobs; i++ {
		job := EmailJob{
			To:      fmt.Sprintf("user%d@example.com", i),
			Subject: fmt.Sprintf("Welcome, user %d!", i),
		}
		if err := emails.Enqueue(ctx, job); err != nil {
			return fmt.Errorf("enqueue: %w", err)
		}
	}

	// Consume them back. We enqueued `jobs`, so pull exactly that many, then
	// Ack each — Ack does XACK + XDEL, which is what actually removes the
	// entry from the stream.
	for i := 0; i < jobs; i++ {
		msg, err := broker.Dequeue(ctx, "emails")
		if err != nil {
			return fmt.Errorf("dequeue: %w", err)
		}

		var job EmailJob
		if err := json.Unmarshal(msg.Payload, &job); err != nil {
			return fmt.Errorf("decode %s: %w", msg.ID, err)
		}
		log.Printf("sending email to %s — %q", job.To, job.Subject)

		if err := broker.Ack(ctx, *msg); err != nil {
			return fmt.Errorf("ack %s: %w", msg.ID, err)
		}
	}

	log.Printf("processed %d jobs", jobs)
	return nil
}

// dialRedis builds a client from TASKQ_REDIS_ADDR (default localhost:6379)
// and pings it. It returns ok=false with actionable guidance when Redis is
// unreachable, so the example can skip instead of crashing.
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
