package membroker

import (
	"context"
	"errors"
	"testing"
	"time"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/taskqtest"
)

// TestConformance runs the full Broker conformance suite (taskqtest) against
// membroker. Round-trip, FIFO order, blocking Dequeue, context cancellation,
// Ack/Nack semantics, queue isolation, and the concurrent stress test all
// live in taskqtest now, shared with redisbroker and (eventually) pgbroker.
func TestConformance(t *testing.T) {
	taskqtest.TestConformance(t, func(t *testing.T) taskq.Broker {
		b := New()
		t.Cleanup(func() { b.Close() })
		return b
	})
}

// The tests below are membroker-specific: Close is not part of the Broker
// interface (redisbroker has no Close at all — its client is caller-owned),
// so this behavior can't live in the generic conformance suite. It stays
// here as membroker's own contract with its callers.

func TestClose_UnblocksPendingDequeue(t *testing.T) {
	b := New()
	ctx := context.Background()

	errCh := make(chan error, 1)
	go func() {
		_, err := b.Dequeue(ctx, "jobs")
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	b.Close()

	select {
	case err := <-errCh:
		if !errors.Is(err, taskq.ErrQueueClosed) {
			t.Errorf("got err %v, want taskq.ErrQueueClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Dequeue did not unblock after Close")
	}
}

func TestClose_DrainsBufferedMessagesFirst(t *testing.T) {
	b := New()
	ctx := context.Background()

	msg := taskq.Message{ID: "1", Queue: "jobs"}
	if err := b.Enqueue(ctx, msg); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	b.Close()

	got, err := b.Dequeue(ctx, "jobs")
	if err != nil {
		t.Fatalf("Dequeue after Close should still drain buffered message: %v", err)
	}
	if got.ID != msg.ID {
		t.Errorf("got ID %q, want %q", got.ID, msg.ID)
	}

	if _, err := b.Dequeue(ctx, "jobs"); !errors.Is(err, taskq.ErrQueueClosed) {
		t.Errorf("got err %v, want taskq.ErrQueueClosed once drained", err)
	}
}

func TestEnqueue_AfterCloseErrors(t *testing.T) {
	b := New()
	b.Close()

	err := b.Enqueue(context.Background(), taskq.Message{ID: "1", Queue: "jobs"})
	if !errors.Is(err, taskq.ErrQueueClosed) {
		t.Errorf("got err %v, want taskq.ErrQueueClosed", err)
	}
}
