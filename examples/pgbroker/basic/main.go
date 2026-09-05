// Command basic shows the pgbroker fundamentals: connect to Postgres, apply
// the schema, enqueue typed jobs, consume them (SELECT ... FOR UPDATE SKIP
// LOCKED under the hood), Ack each by its ReceiptHandle, and inspect the
// queue with pgbroker's admin methods.
//
// pgbroker takes a caller-owned *pgxpool.Pool (it never closes it) and has no
// Close()/ErrQueueClosed — like redisbroker, a consumer stops when its
// context is cancelled. It also needs its table to exist; pgdemo.Connect
// applies pgbroker/schema.sql for you.
//
// Requires a running Postgres (default DSN
// postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable,
// override with TASKQ_POSTGRES_DSN):
//
//	docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16
//
// Run it with:
//
//	go run ./examples/pgbroker/basic
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/examples/pgbroker/internal/pgdemo"
	"github.com/Nuvraxis/taskQ/pgbroker"
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

	pool, ok := pgdemo.Connect(ctx)
	if !ok {
		return nil // Postgres unreachable — guidance printed, skip cleanly
	}
	defer pool.Close() // pgbroker doesn't own the pool; the caller does

	// WithQueuePrefix namespaces this example's rows inside the shared
	// taskq_messages table. Purge on exit so re-runs start clean.
	broker := pgbroker.New(pool, pgbroker.WithQueuePrefix("example:"))
	defer func() { _, _ = broker.PurgeQueue(context.Background(), "emails") }()

	emails := taskq.NewQueue[EmailJob](broker, "emails")

	// Produce a batch (each row INSERTed, with a LISTEN/NOTIFY wake-up).
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

	// pgbroker ships admin helpers beyond the taskq.Broker interface.
	if depth, err := broker.QueueDepth(ctx, "emails"); err == nil {
		log.Printf("queue depth after enqueue: %d", depth)
	}

	// Consume them back. We enqueued `jobs`, so pull exactly that many, then
	// Ack each by its ReceiptHandle (the claim token this delivery holds) —
	// Ack deletes the row.
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

	if depth, err := broker.QueueDepth(ctx, "emails"); err == nil {
		log.Printf("queue depth after processing: %d", depth)
	}
	return nil
}
