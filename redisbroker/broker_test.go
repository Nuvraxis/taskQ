// redisbroker/broker_test.go

package redisbroker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	taskq "github.com/Nuvraxis/taskQ"
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

func newMessage(queue, payload string) taskq.Message {
	return taskq.Message{
		ID:         uuid.NewString(),
		Queue:      queue,
		Payload:    []byte(payload),
		MaxRetry:   3,
		EnqueuedAt: time.Now().UTC(),
	}
}

func TestEnqueueDequeue_Roundtrip(t *testing.T) {
	client := newTestClient(t)
	b, queue := newTestBroker(t, client)
	ctx := context.Background()

	want := newMessage(queue, "hello")
	if err := b.Enqueue(ctx, want); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := b.Dequeue(ctx, queue)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	if got.ID != want.ID {
		t.Errorf("ID = %q, want %q", got.ID, want.ID)
	}
	if got.Queue != want.Queue {
		t.Errorf("Queue = %q, want %q", got.Queue, want.Queue)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, want.Payload)
	}
	if got.MaxRetry != want.MaxRetry {
		t.Errorf("MaxRetry = %d, want %d", got.MaxRetry, want.MaxRetry)
	}
	if got.ReceiptHandle == "" {
		t.Error("ReceiptHandle is empty, want a stream entry ID")
	}
}

func TestDequeue_FIFOOrder(t *testing.T) {
	client := newTestClient(t)
	b, queue := newTestBroker(t, client)
	ctx := context.Background()

	var want []string
	for i := 0; i < 5; i++ {
		msg := newMessage(queue, fmt.Sprintf("payload-%d", i))
		want = append(want, msg.ID)
		if err := b.Enqueue(ctx, msg); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	var got []string
	for i := 0; i < 5; i++ {
		msg, err := b.Dequeue(ctx, queue)
		if err != nil {
			t.Fatalf("Dequeue %d: %v", i, err)
		}
		got = append(got, msg.ID)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Errorf("position %d: got %q, want %q (order: %v)", i, got[i], want[i], got)
		}
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
		t.Errorf("stream length = %d after Ack, want 0", length)
	}
}

func TestNack_RequeuesAtBack(t *testing.T) {
	client := newTestClient(t)
	b, queue := newTestBroker(t, client)
	ctx := context.Background()

	first := newMessage(queue, "first")
	second := newMessage(queue, "second")
	if err := b.Enqueue(ctx, first); err != nil {
		t.Fatalf("Enqueue first: %v", err)
	}
	if err := b.Enqueue(ctx, second); err != nil {
		t.Fatalf("Enqueue second: %v", err)
	}

	got1, err := b.Dequeue(ctx, queue)
	if err != nil {
		t.Fatalf("Dequeue 1: %v", err)
	}
	if got1.ID != first.ID {
		t.Fatalf("Dequeue 1 = %q, want %q (first)", got1.ID, first.ID)
	}
	oldReceipt := got1.ReceiptHandle

	got1.Attempts++
	if err := b.Nack(ctx, *got1, errors.New("transient failure")); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// second should now come before the requeued first — Nack moves to back.
	got2, err := b.Dequeue(ctx, queue)
	if err != nil {
		t.Fatalf("Dequeue 2: %v", err)
	}
	if got2.ID != second.ID {
		t.Errorf("Dequeue 2 = %q, want %q (second, before requeued first)", got2.ID, second.ID)
	}

	got3, err := b.Dequeue(ctx, queue)
	if err != nil {
		t.Fatalf("Dequeue 3: %v", err)
	}
	if got3.ID != first.ID {
		t.Errorf("Dequeue 3 = %q, want %q (requeued first)", got3.ID, first.ID)
	}
	if got3.Attempts != 1 {
		t.Errorf("requeued Attempts = %d, want 1", got3.Attempts)
	}
	if got3.ReceiptHandle == oldReceipt {
		t.Error("ReceiptHandle unchanged after Nack, want a new stream entry ID")
	}
}

func TestDequeue_BlocksUntilEnqueue(t *testing.T) {
	client := newTestClient(t)
	b, queue := newTestBroker(t, client, WithBlockTimeout(200*time.Millisecond))
	ctx := context.Background()

	type result struct {
		msg *taskq.Message
		err error
	}
	done := make(chan result, 1)
	go func() {
		msg, err := b.Dequeue(ctx, queue)
		done <- result{msg, err}
	}()

	select {
	case r := <-done:
		t.Fatalf("Dequeue returned early with nothing enqueued: msg=%v err=%v", r.msg, r.err)
	case <-time.After(500 * time.Millisecond):
		// expected: still blocked
	}

	want := newMessage(queue, "arrived")
	if err := b.Enqueue(ctx, want); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Dequeue: %v", r.err)
		}
		if r.msg.ID != want.ID {
			t.Errorf("ID = %q, want %q", r.msg.ID, want.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Dequeue did not unblock after Enqueue")
	}
}

func TestDequeue_ContextCancellation(t *testing.T) {
	client := newTestClient(t)
	b, queue := newTestBroker(t, client, WithBlockTimeout(200*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		msg *taskq.Message
		err error
	}
	done := make(chan result, 1)
	go func() {
		msg, err := b.Dequeue(ctx, queue)
		done <- result{msg, err}
	}()

	time.Sleep(100 * time.Millisecond) // let it start blocking
	cancel()

	select {
	case r := <-done:
		if !errors.Is(r.err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", r.err)
		}
		if r.msg != nil {
			t.Errorf("msg = %v, want nil", r.msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Dequeue did not return promptly after cancellation")
	}
}

func TestConcurrentProducersConsumers(t *testing.T) {
	client := newTestClient(t)
	b, queue := newTestBroker(t, client, WithBlockTimeout(200*time.Millisecond))

	const (
		producers    = 5
		messagesEach = 20
		consumers    = 4
		total        = producers * messagesEach
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var enqueueWG sync.WaitGroup
	for p := 0; p < producers; p++ {
		enqueueWG.Add(1)
		go func(p int) {
			defer enqueueWG.Done()
			for i := 0; i < messagesEach; i++ {
				msg := newMessage(queue, fmt.Sprintf("p%d-m%d", p, i))
				if err := b.Enqueue(ctx, msg); err != nil {
					t.Errorf("producer %d: Enqueue: %v", p, err)
					return
				}
			}
		}(p)
	}
	enqueueWG.Wait()

	var (
		mu       sync.Mutex
		seen     = make(map[string]int)
		received int32
	)

	var consumeWG sync.WaitGroup
	for c := 0; c < consumers; c++ {
		consumeWG.Add(1)
		go func() {
			defer consumeWG.Done()
			for {
				if atomic.LoadInt32(&received) >= total {
					return
				}
				msg, err := b.Dequeue(ctx, queue)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return
					}
					t.Errorf("Dequeue: %v", err)
					return
				}

				mu.Lock()
				seen[msg.ID]++
				mu.Unlock()

				if err := b.Ack(ctx, *msg); err != nil {
					t.Errorf("Ack: %v", err)
				}
				atomic.AddInt32(&received, 1)
			}
		}()
	}

	// Stop consumers once everything's been received, rather than waiting
	// for their next BLOCK to time out on its own.
	stopWatcher := make(chan struct{})
	go func() {
		defer close(stopWatcher)
		for atomic.LoadInt32(&received) < total {
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
		cancel()
	}()
	<-stopWatcher
	consumeWG.Wait()

	if got := int(atomic.LoadInt32(&received)); got != total {
		t.Fatalf("received %d messages, want %d", got, total)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != total {
		t.Errorf("%d distinct message IDs seen, want %d", len(seen), total)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("message %s delivered %d times, want 1", id, count)
		}
	}
}
