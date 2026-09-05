// Package taskqtest is the conformance suite every Broker implementation —
// membroker, redisbroker, pgbroker, and any third-party backend — must pass.
// It exercises a Broker purely through the taskq.Broker interface, so it has
// no dependency on any concrete backend.
//
// A backend's own test package wires this in with a small factory, e.g.:
//
//	func TestConformance(t *testing.T) {
//	    taskqtest.TestConformance(t, func(t *testing.T) taskq.Broker {
//	        b := membroker.New()
//	        t.Cleanup(func() { b.Close() })
//	        return b
//	    })
//	}
package taskqtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	taskq "github.com/Nuvraxis/taskQ"
)

// BrokerFactory constructs a fresh, ready-to-use Broker for a single
// (sub)test. Implementations register any teardown (closing connections,
// stopping servers, flushing state) via t.Cleanup, so TestConformance never
// needs to know how a given backend shuts down.
type BrokerFactory func(t *testing.T) taskq.Broker

// TestConformance runs the full suite against brokers built by newBroker.
// Each behavior gets its own subtest with its own broker instance, so a
// failure in one doesn't contaminate the others.
func TestConformance(t *testing.T, newBroker BrokerFactory) {
	t.Run("EnqueueDequeueRoundTrip", func(t *testing.T) { testRoundTrip(t, newBroker) })
	t.Run("FIFOOrdering", func(t *testing.T) { testFIFOOrdering(t, newBroker) })
	t.Run("DequeueBlocksUntilEnqueue", func(t *testing.T) { testBlockingDequeue(t, newBroker) })
	t.Run("DequeueRespectsContextCancellation", func(t *testing.T) { testContextCancel(t, newBroker) })
	t.Run("AckRemovesMessage", func(t *testing.T) { testAckRemoves(t, newBroker) })
	t.Run("NackRequeuesVerbatim", func(t *testing.T) { testNackRequeues(t, newBroker) })
	t.Run("QueuesAreIndependent", func(t *testing.T) { testQueueIsolation(t, newBroker) })
	t.Run("ConcurrentProducersConsumers", func(t *testing.T) { testConcurrency(t, newBroker) })
}

// dequeueWithTimeout dequeues with a generous timeout so a broker that's
// stuck — rather than correctly blocking — fails the test instead of hanging
// the whole run.
func dequeueWithTimeout(t *testing.T, b taskq.Broker, queue string) *taskq.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg, err := b.Dequeue(ctx, queue)
	if err != nil {
		t.Fatalf("Dequeue(%q): %v", queue, err)
	}
	return msg
}

// expectNoMessage asserts queue has nothing available within a short window.
// Used to prove Ack actually removed a message (rather than leaving it for
// redelivery) and that queues don't leak into each other.
func expectNoMessage(t *testing.T, b taskq.Broker, queue string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if msg, err := b.Dequeue(ctx, queue); err == nil {
		t.Fatalf("Dequeue(%q) returned message %q, want the queue to be empty", queue, msg.ID)
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dequeue(%q) returned unexpected error: %v", queue, err)
	}
}

func testRoundTrip(t *testing.T, newBroker BrokerFactory) {
	ctx := context.Background()
	b := newBroker(t)

	want := taskq.Message{
		ID:         uuid.NewString(),
		Queue:      "roundtrip",
		Payload:    []byte(`{"hello":"world"}`),
		MaxRetry:   3,
		EnqueuedAt: time.Now(),
	}
	if err := b.Enqueue(ctx, want); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got := dequeueWithTimeout(t, b, "roundtrip")
	if got.ID != want.ID {
		t.Errorf("ID = %q, want %q", got.ID, want.ID)
	}
	if got.Queue != want.Queue {
		t.Errorf("Queue = %q, want %q", got.Queue, want.Queue)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Errorf("Payload = %s, want %s", got.Payload, want.Payload)
	}
	if got.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0 on first delivery", got.Attempts)
	}
	if got.MaxRetry != want.MaxRetry {
		t.Errorf("MaxRetry = %d, want %d", got.MaxRetry, want.MaxRetry)
	}
	if got.EnqueuedAt.IsZero() {
		t.Error("EnqueuedAt is zero, want a stamped time by the time it's delivered")
	}
}

func testFIFOOrdering(t *testing.T, newBroker BrokerFactory) {
	ctx := context.Background()
	b := newBroker(t)
	const queue = "fifo"

	ids := make([]string, 3)
	for i := range ids {
		ids[i] = uuid.NewString()
		msg := taskq.Message{
			ID:         ids[i],
			Queue:      queue,
			Payload:    []byte(fmt.Sprintf(`{"i":%d}`, i)),
			EnqueuedAt: time.Now(),
		}
		if err := b.Enqueue(ctx, msg); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	for i, wantID := range ids {
		got := dequeueWithTimeout(t, b, queue)
		if got.ID != wantID {
			t.Errorf("Dequeue %d returned ID %q, want %q (FIFO order broken)", i, got.ID, wantID)
		}
	}
}

func testBlockingDequeue(t *testing.T, newBroker BrokerFactory) {
	ctx := context.Background()
	b := newBroker(t)
	const queue = "blocking"

	resultCh := make(chan *taskq.Message, 1)
	errCh := make(chan error, 1)
	go func() {
		msg, err := b.Dequeue(ctx, queue)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- msg
	}()

	select {
	case <-resultCh:
		t.Fatal("Dequeue returned before any message was enqueued")
	case err := <-errCh:
		t.Fatalf("Dequeue returned early with error: %v", err)
	case <-time.After(100 * time.Millisecond):
		// still blocked, as expected
	}

	id := uuid.NewString()
	if err := b.Enqueue(ctx, taskq.Message{ID: id, Queue: queue, Payload: []byte("{}"), EnqueuedAt: time.Now()}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case msg := <-resultCh:
		if msg.ID != id {
			t.Errorf("Dequeue returned ID %q, want %q", msg.ID, id)
		}
	case err := <-errCh:
		t.Fatalf("Dequeue returned error after Enqueue: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Dequeue did not unblock after Enqueue")
	}
}

func testContextCancel(t *testing.T, newBroker BrokerFactory) {
	b := newBroker(t)
	const queue = "cancel"
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		_, err := b.Dequeue(ctx, queue)
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond) // let the goroutine actually block
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Dequeue returned nil error after context cancellation")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Dequeue error = %v, want errors.Is(err, context.Canceled)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Dequeue did not return after context cancellation")
	}
}

func testAckRemoves(t *testing.T, newBroker BrokerFactory) {
	ctx := context.Background()
	b := newBroker(t)
	const queue = "ack"

	id := uuid.NewString()
	if err := b.Enqueue(ctx, taskq.Message{ID: id, Queue: queue, Payload: []byte("{}"), EnqueuedAt: time.Now()}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	msg := dequeueWithTimeout(t, b, queue)
	if err := b.Ack(ctx, *msg); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	expectNoMessage(t, b, queue)
}

func testNackRequeues(t *testing.T, newBroker BrokerFactory) {
	ctx := context.Background()
	b := newBroker(t)
	const queue = "nack"

	idA, idB := uuid.NewString(), uuid.NewString()
	for _, id := range []string{idA, idB} {
		msg := taskq.Message{ID: id, Queue: queue, Payload: []byte("{}"), MaxRetry: 5, EnqueuedAt: time.Now()}
		if err := b.Enqueue(ctx, msg); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}

	a := dequeueWithTimeout(t, b, queue)
	if a.ID != idA {
		t.Fatalf("got %q first, want %q", a.ID, idA)
	}

	// Nack with a caller-chosen Attempts value; the broker must persist it
	// verbatim on redelivery rather than computing its own.
	a.Attempts = 7
	if err := b.Nack(ctx, *a, errors.New("transient")); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// B was never touched, so it should come back before the requeued A.
	b2 := dequeueWithTimeout(t, b, queue)
	if b2.ID != idB {
		t.Errorf("got %q, want %q (Nack should requeue to the back)", b2.ID, idB)
	}

	redelivered := dequeueWithTimeout(t, b, queue)
	if redelivered.ID != idA {
		t.Errorf("got %q, want %q (redelivered A)", redelivered.ID, idA)
	}
	if redelivered.Attempts != 7 {
		t.Errorf("Attempts = %d, want 7 (Nack must persist the caller's count verbatim)", redelivered.Attempts)
	}
}

func testQueueIsolation(t *testing.T, newBroker BrokerFactory) {
	ctx := context.Background()
	b := newBroker(t)

	idA := uuid.NewString()
	if err := b.Enqueue(ctx, taskq.Message{ID: idA, Queue: "queue-a", Payload: []byte("{}"), EnqueuedAt: time.Now()}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	expectNoMessage(t, b, "queue-b")

	got := dequeueWithTimeout(t, b, "queue-a")
	if got.ID != idA {
		t.Errorf("got %q, want %q", got.ID, idA)
	}
}

func testConcurrency(t *testing.T, newBroker BrokerFactory) {
	if testing.Short() {
		t.Skip("stress test skipped in -short mode")
	}
	ctx := context.Background()
	b := newBroker(t)
	const queue = "concurrency"
	const producers = 10
	const perProducer = 50
	const total = producers * perProducer

	var produceWG sync.WaitGroup
	for p := 0; p < producers; p++ {
		produceWG.Go(func() {
			for i := 0; i < perProducer; i++ {
				msg := taskq.Message{
					ID:         uuid.NewString(),
					Queue:      queue,
					Payload:    []byte(fmt.Sprintf(`{"p":%d,"i":%d}`, p, i)),
					EnqueuedAt: time.Now(),
				}
				if err := b.Enqueue(ctx, msg); err != nil {
					t.Errorf("producer %d: Enqueue: %v", p, err)
					return
				}
			}
		})
	}

	seen := make(map[string]int)
	var mu sync.Mutex
	var consumeWG sync.WaitGroup
	const consumers = 5
	for c := 0; c < consumers; c++ {
		consumeWG.Go(func() {
			for {
				dctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				msg, err := b.Dequeue(dctx, queue)
				cancel()
				if err != nil {
					return // 2s idle: assume the batch is drained
				}
				mu.Lock()
				seen[msg.ID]++
				mu.Unlock()
				if err := b.Ack(ctx, *msg); err != nil {
					t.Errorf("Ack: %v", err)
				}
			}
		})
	}

	produceWG.Wait()
	consumeWG.Wait()

	if len(seen) != total {
		t.Errorf("received %d distinct messages, want %d (lost delivery)", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("message %s delivered %d times, want exactly 1 (duplicate delivery)", id, n)
		}
	}
}
