package pgbroker

import (
	"context"
	_ "embed"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/taskqtest"
)

//go:embed schema.sql
var schemaSQL string

// newTestPool connects to a live Postgres instance for integration tests.
// Skipped entirely under -short, same convention as redisbroker's
// newTestClient — CI's cross-platform jobs rely on this. DSN defaults to
// the local/CI convention (see ci.yaml's TASKQ_TEST_POSTGRES_DSN); override
// with TASKQ_TEST_POSTGRES_DSN if your Postgres lives elsewhere.
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping pgbroker tests in -short mode (requires live Postgres)")
	}

	dsn := os.Getenv("TASKQ_TEST_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:changeme@postgres-my-postgres-postgresql-1.tail4fc230.ts.net:5432/taskq_test?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping: could not create pool for %s: %v", dsn, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("skipping: could not reach Postgres at %s: %v", dsn, err)
	}

	// schema.sql is all CREATE TABLE/INDEX IF NOT EXISTS, so applying it
	// on every test run is safe and means tests don't depend on any
	// manual out-of-band setup — CI's postgres service container starts
	// empty, and a local dev database may not have the table yet either.
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		pool.Close()
		t.Fatalf("applying schema.sql: %v", err)
	}

	t.Cleanup(pool.Close)
	return pool
}

// TestConformance runs the shared Broker conformance suite (taskqtest)
// against pgbroker. Each subtest gets its own broker under a fresh random
// queue prefix — unlike redisbroker's isolated stream keys, every
// pgbroker queue lives in the same taskq_messages table, so without a
// prefix two subtests both using taskqtest's fixed queue names (e.g.
// "roundtrip") would collide on one shared Postgres.
func TestConformance(t *testing.T) {
	pool := newTestPool(t)

	taskqtest.TestConformance(t, func(t *testing.T) taskq.Broker {
		prefix := "conformance-" + uuid.NewString() + ":"
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = pool.Exec(ctx, "DELETE FROM taskq_messages WHERE queue LIKE $1", prefix+"%")
		})
		return New(pool, WithQueuePrefix(prefix), WithPollInterval(50*time.Millisecond))
	})
}

// The tests below check pgbroker-specific behavior the generic
// conformance suite can't see, because it only knows the taskq.Broker
// interface: lease expiry, ReceiptHandle uniqueness across redelivery, and
// direct row-level assertions via SQL. Analogous to redisbroker's
// ReceiptHandle/XLEN tests.

func newIsolatedBroker(t *testing.T, pool *pgxpool.Pool, opts ...Option) (*Broker, string) {
	t.Helper()
	prefix := "test-" + uuid.NewString() + ":"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, "DELETE FROM taskq_messages WHERE queue LIKE $1", prefix+"%")
	})
	allOpts := append([]Option{WithQueuePrefix(prefix)}, opts...)
	return New(pool, allOpts...), "jobs"
}

func newMessage(queue, payload string) taskq.Message {
	return taskq.Message{
		ID:         uuid.NewString(),
		Queue:      queue,
		Payload:    []byte(payload),
		MaxRetry:   3,
		EnqueuedAt: time.Now().UTC(),
	}
}

func TestDequeue_PopulatesReceiptHandle(t *testing.T) {
	pool := newTestPool(t)
	b, queue := newIsolatedBroker(t, pool)
	ctx := context.Background()

	if err := b.Enqueue(ctx, newMessage(queue, "hello")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	got, err := b.Dequeue(ctx, queue)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if got.ReceiptHandle == "" {
		t.Error("ReceiptHandle is empty, want a claim token (membroker leaves this blank; pgbroker must populate it)")
	}
}

func TestAck_RemovesRow(t *testing.T) {
	pool := newTestPool(t)
	b, queue := newIsolatedBroker(t, pool)
	ctx := context.Background()

	msg := newMessage(queue, "x")
	if err := b.Enqueue(ctx, msg); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	got, err := b.Dequeue(ctx, queue)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if err := b.Ack(ctx, *got); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM taskq_messages WHERE id = $1", msg.ID).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 0 {
		t.Errorf("row count for %s = %d after Ack, want 0", msg.ID, count)
	}
}

func TestNack_AssignsNewReceiptHandleAndAdvancesSortKey(t *testing.T) {
	pool := newTestPool(t)
	b, queue := newIsolatedBroker(t, pool)
	ctx := context.Background()

	if err := b.Enqueue(ctx, newMessage(queue, "x")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	got, err := b.Dequeue(ctx, queue)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	oldReceipt := got.ReceiptHandle

	got.Attempts++
	if err := b.Nack(ctx, *got, errors.New("transient failure")); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	redelivered, err := b.Dequeue(ctx, queue)
	if err != nil {
		t.Fatalf("Dequeue after Nack: %v", err)
	}
	if redelivered.ReceiptHandle == oldReceipt {
		t.Error("ReceiptHandle unchanged after Nack, want a fresh claim token (requeue = delete+reinsert, not an in-place update)")
	}
	if redelivered.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1 (Nack must persist the caller's count verbatim)", redelivered.Attempts)
	}
}

func TestDequeue_RedeliversAfterLeaseExpires(t *testing.T) {
	pool := newTestPool(t)
	b, queue := newIsolatedBroker(t, pool, WithLeaseDuration(100*time.Millisecond))
	ctx := context.Background()

	msg := newMessage(queue, "x")
	if err := b.Enqueue(ctx, msg); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	first, err := b.Dequeue(ctx, queue)
	if err != nil {
		t.Fatalf("first Dequeue: %v", err)
	}

	// Neither Ack nor Nack — simulating a crashed consumer. A second
	// Dequeue immediately after should find nothing (lease still held);
	// SKIP LOCKED only skips a row locked within another *transaction*,
	// so it's locked_until, not a row lock, that's actually keeping this
	// invisible right now.
	shortCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	if _, err := b.Dequeue(shortCtx, queue); err == nil {
		cancel()
		t.Fatal("Dequeue returned a message while the first delivery's lease was still active")
	}
	cancel()

	// Once the lease expires, the same row should become available again
	// — with no separate reaper process; the same WHERE clause that finds
	// fresh messages finds expired-lease ones too.
	waitCtx, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
	redelivered, err := b.Dequeue(waitCtx, queue)
	if err != nil {
		t.Fatalf("Dequeue after lease expiry: %v", err)
	}
	if redelivered.ID != first.ID {
		t.Errorf("redelivered ID = %q, want %q", redelivered.ID, first.ID)
	}
	if redelivered.ReceiptHandle == first.ReceiptHandle {
		t.Error("ReceiptHandle unchanged after lease-expiry redelivery, want a fresh claim token")
	}
}
