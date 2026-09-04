package redisbroker

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/taskqtest"
)

// newTestClient connects to a live Redis instance for integration tests.
// Skipped entirely under -short (per the roadmap's "gate live-service
// tests behind testing.Short()" convention — CI's cross-platform jobs rely
// on this). Address defaults to localhost:6379; override with
// TASKQ_TEST_REDIS_ADDR if your Redis lives elsewhere.
func newTestClient(t *testing.T) *redis.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping redisbroker tests in -short mode (requires live Redis)")
	}

	addr := os.Getenv("TASKQ_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{Addr: addr})

	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		t.Skipf("skipping: could not reach Redis at %s: %v", addr, err)
	}

	t.Cleanup(func() { _ = client.Close() })
	return client
}

// newTestBroker returns a Broker on a fresh, uniquely-named queue and
// registers cleanup of that queue's stream key so tests don't leave
// garbage behind on a shared Redis instance.
func newTestBroker(t *testing.T, client *redis.Client, opts ...Option) (*Broker, string) {
	t.Helper()
	queue := "test-" + uuid.NewString()
	b := New(client, opts...)
	key := b.streamKey(queue)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = client.Del(ctx, key)
	})

	return b, queue
}

// cleanupStreamsWithPrefix deletes every Redis key under prefix once the
// test finishes. Used by TestConformance, where taskqtest drives fixed
// queue names ("roundtrip", "fifo", "ack", …) against a single broker —
// isolation between subtests, and between separate test runs, comes from
// giving each subtest's broker its own key prefix rather than from unique
// queue names.
func cleanupStreamsWithPrefix(t *testing.T, client *redis.Client, prefix string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var keys []string
		iter := client.Scan(ctx, 0, prefix+"*", 0).Iterator()
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if err := iter.Err(); err != nil {
			t.Logf("cleanup: scan for prefix %q: %v", prefix, err)
			return
		}
		if len(keys) > 0 {
			_ = client.Del(ctx, keys...)
		}
	})
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

// TestConformance runs the shared Broker conformance suite (taskqtest)
// against redisbroker — proving it satisfies the same contract as
// membroker, not a redis-specific variant of it. Each subtest gets its own
// broker under a fresh random key prefix, since taskqtest exercises fixed
// queue names.
func TestConformance(t *testing.T) {
	client := newTestClient(t)

	taskqtest.TestConformance(t, func(t *testing.T) taskq.Broker {
		prefix := "conformance-" + uuid.NewString() + ":"
		cleanupStreamsWithPrefix(t, client, prefix)
		return New(client, WithKeyPrefix(prefix), WithBlockTimeout(200*time.Millisecond))
	})
}

// The tests below check redis-specific behavior the generic conformance
// suite can't see, because it only knows the taskq.Broker interface: it
// can't tell a membroker-empty ReceiptHandle from a redis one, or peek at a
// stream's XLEN. These fill that gap.

func TestDequeue_PopulatesReceiptHandle(t *testing.T) {
	client := newTestClient(t)
	b, queue := newTestBroker(t, client)
	ctx := context.Background()

	if err := b.Enqueue(ctx, newMessage(queue, "hello")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	got, err := b.Dequeue(ctx, queue)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if got.ReceiptHandle == "" {
		t.Error("ReceiptHandle is empty, want a stream entry ID (membroker leaves this blank; redisbroker must populate it)")
	}
}

func TestAck_RemovesEntry(t *testing.T) {
	client := newTestClient(t)
	b, queue := newTestBroker(t, client)
	ctx := context.Background()

	if err := b.Enqueue(ctx, newMessage(queue, "x")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	msg, err := b.Dequeue(ctx, queue)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if err := b.Ack(ctx, *msg); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	key := b.streamKey(queue)
	length, err := client.XLen(ctx, key).Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if length != 0 {
		t.Errorf("stream length = %d after Ack, want 0 (Ack should XACK+XDEL, not just leave the entry pending)", length)
	}
}

func TestNack_AssignsNewReceiptHandle(t *testing.T) {
	client := newTestClient(t)
	b, queue := newTestBroker(t, client)
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
		t.Error("ReceiptHandle unchanged after Nack, want a new stream entry ID (requeue = new XADD; the old entry is XACK+XDEL'd)")
	}
	if redelivered.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1 (Nack must persist the caller's count)", redelivered.Attempts)
	}
}
