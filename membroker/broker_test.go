package membroker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Nuvraxis/taskQ"
)

func TestEnqueueDequeue_Roundtrip(t *testing.T) {
	tests := []struct {
		name string
		msgs []taskq.Message
	}{
		{
			name: "single message",
			msgs: []taskq.Message{
				{ID: "1", Queue: "emails", Payload: []byte(`{"to":"a@example.com"}`)},
			},
		},
		{
			name: "multiple messages preserve FIFO order",
			msgs: []taskq.Message{
				{ID: "1", Queue: "emails", Payload: []byte(`"first"`)},
				{ID: "2", Queue: "emails", Payload: []byte(`"second"`)},
				{ID: "3", Queue: "emails", Payload: []byte(`"third"`)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := New()
			ctx := context.Background()

			for _, m := range tt.msgs {
				if err := b.Enqueue(ctx, m); err != nil {
					t.Fatalf("Enqueue(%s): %v", m.ID, err)
				}
			}

			for _, want := range tt.msgs {
				got, err := b.Dequeue(ctx, want.Queue)
				if err != nil {
					t.Fatalf("Dequeue: %v", err)
				}
				if got.ID != want.ID {
					t.Errorf("Dequeue order: got ID %q, want %q", got.ID, want.ID)
				}
			}
		})
	}
}

func TestDequeue_BlocksUntilEnqueue(t *testing.T) {
	b := New()
	ctx := context.Background()

	type result struct {
		msg *taskq.Message
		err error
	}
	resultCh := make(chan result, 1)

	go func() {
		msg, err := b.Dequeue(ctx, "jobs")
		resultCh <- result{msg, err}
	}()

	// Not a hard guarantee the goroutine has reached cond.Wait yet, but
	// enough in practice to catch a Dequeue that wrongly returns early on
	// an empty queue instead of blocking.
	time.Sleep(20 * time.Millisecond)

	want := taskq.Message{ID: "1", Queue: "jobs", Payload: []byte(`"x"`)}
	if err := b.Enqueue(ctx, want); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case r := <-resultCh:
		if r.err != nil {
			t.Fatalf("Dequeue: %v", r.err)
		}
		if r.msg.ID != want.ID {
			t.Errorf("got ID %q, want %q", r.msg.ID, want.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Dequeue did not unblock after Enqueue")
	}
}

func TestDequeue_ContextCancellation(t *testing.T) {
	b := New()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := b.Dequeue(ctx, "empty-queue")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got err %v, want context.DeadlineExceeded", err)
	}
}

func TestNack_Requeues(t *testing.T) {
	b := New()
	ctx := context.Background()

	msg := taskq.Message{ID: "1", Queue: "jobs", Payload: []byte(`"x"`)}
	if err := b.Enqueue(ctx, msg); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := b.Dequeue(ctx, "jobs")
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if err := b.Nack(ctx, *got, errors.New("handler failed")); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	redelivered, err := b.Dequeue(ctx, "jobs")
	if err != nil {
		t.Fatalf("Dequeue after Nack: %v", err)
	}
	if redelivered.ID != msg.ID {
		t.Errorf("got ID %q, want %q", redelivered.ID, msg.ID)
	}
}

func TestNack_GoesToBackOfQueue(t *testing.T) {
	b := New()
	ctx := context.Background()

	first := taskq.Message{ID: "1", Queue: "jobs"}
	second := taskq.Message{ID: "2", Queue: "jobs"}
	if err := b.Enqueue(ctx, first); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := b.Enqueue(ctx, second); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := b.Dequeue(ctx, "jobs")
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if err := b.Nack(ctx, *got, errors.New("boom")); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// second should come before the requeued first.
	next, err := b.Dequeue(ctx, "jobs")
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if next.ID != second.ID {
		t.Errorf("got ID %q, want %q (second before requeued first)", next.ID, second.ID)
	}
}

func TestAck_DoesNotError(t *testing.T) {
	b := New()
	ctx := context.Background()

	msg := taskq.Message{ID: "1", Queue: "jobs"}
	if err := b.Enqueue(ctx, msg); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	got, err := b.Dequeue(ctx, "jobs")
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if err := b.Ack(ctx, *got); err != nil {
		t.Errorf("Ack: %v", err)
	}
}

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

func TestConcurrentProducersConsumers(t *testing.T) {
	const (
		numProducers = 10
		numConsumers = 5
		perProducer  = 100
	)
	total := numProducers * perProducer

	b := New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var producerWG sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		producerWG.Add(1)
		go func(p int) {
			defer producerWG.Done()
			for i := 0; i < perProducer; i++ {
				msg := taskq.Message{ID: fmt.Sprintf("p%d-%d", p, i), Queue: "load"}
				if err := b.Enqueue(ctx, msg); err != nil {
					t.Errorf("Enqueue: %v", err)
					return
				}
			}
		}(p)
	}

	seen := make(chan string, total)
	var consumerWG sync.WaitGroup
	for c := 0; c < numConsumers; c++ {
		consumerWG.Add(1)
		go func() {
			defer consumerWG.Done()
			for {
				msg, err := b.Dequeue(ctx, "load")
				if err != nil {
					return // ctx done or closed once drained — expected
				}
				seen <- msg.ID
			}
		}()
	}

	producerWG.Wait()

	deadline := time.After(5 * time.Second)
drain:
	for len(seen) < total {
		select {
		case <-deadline:
			break drain
		case <-time.After(10 * time.Millisecond):
		}
	}
	b.Close()
	consumerWG.Wait()
	close(seen)

	ids := make(map[string]bool, total)
	for id := range seen {
		if ids[id] {
			t.Errorf("duplicate delivery of %q", id)
		}
		ids[id] = true
	}
	if len(ids) != total {
		t.Errorf("got %d unique messages, want %d", len(ids), total)
	}
}
