package taskq_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/membroker"
)

type job struct {
	N int `json:"n"`
}

// waitFor polls cond until it returns true or timeout elapses, failing the
// test on timeout.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("condition not met within %s", timeout)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestPool_Success verifies a successfully handled task is Acked (removed)
// and the handler observes the decoded payload.
func TestPool_Success(t *testing.T) {
	t.Log("a task whose handler returns nil should be Acked and not redelivered")

	broker := membroker.New()
	defer broker.Close()

	queue := taskq.NewQueue[job](broker, "success")
	if err := queue.Enqueue(context.Background(), job{N: 7}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	var (
		calls int32
		got   job
		mu    sync.Mutex
	)
	handler := func(_ context.Context, task taskq.Task[job]) error {
		atomic.AddInt32(&calls, 1)
		mu.Lock()
		got = task.Payload
		mu.Unlock()
		return nil
	}

	pool := taskq.NewPool[job](broker, "success", handler, taskq.WithConcurrency(1))

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- pool.Run(ctx) }()

	waitFor(t, func() bool { return atomic.LoadInt32(&calls) == 1 }, time.Second)

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got.N != 7 {
		t.Errorf("handler saw payload %+v, want {N:7}", got)
	}
}

// TestPool_RetryThenGiveUp verifies a task that always fails is retried up
// to MaxRetry times (via Nack) and then Acked (given up on), with the
// handler invoked exactly MaxRetry+1 times.
func TestPool_RetryThenGiveUp(t *testing.T) {
	t.Log("a handler that always errors should be retried MaxRetry times, then abandoned")

	broker := membroker.New()
	defer broker.Close()

	const maxRetry = 2
	queue := taskq.NewQueue[job](broker, "retry", taskq.WithDefaultMaxRetry(maxRetry))
	if err := queue.Enqueue(context.Background(), job{N: 1}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	var calls int32
	wantErr := errors.New("boom")
	handler := func(_ context.Context, _ taskq.Task[job]) error {
		atomic.AddInt32(&calls, 1)
		return wantErr
	}

	pool := taskq.NewPool[job](
		broker, "retry", handler,
		taskq.WithConcurrency(1),
		taskq.WithBackoffStrategy(taskq.ConstantBackoff{Delay: 5 * time.Millisecond}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- pool.Run(ctx) }()

	// MaxRetry+1 total attempts, then it should stay put — give the extra
	// backoff window a chance to fire and confirm nothing beyond that lands.
	waitFor(t, func() bool { return atomic.LoadInt32(&calls) == maxRetry+1 }, time.Second)
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != maxRetry+1 {
		t.Errorf("handler called %d times, want exactly %d (task should have been abandoned)", got, maxRetry+1)
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
}

// TestPool_ConcurrentWorkers verifies every enqueued task is delivered
// exactly once across multiple concurrent workers, with no duplicate or
// lost delivery.
func TestPool_ConcurrentWorkers(t *testing.T) {
	t.Log("N concurrent workers should process every task exactly once")

	broker := membroker.New()
	defer broker.Close()

	const total = 100
	queue := taskq.NewQueue[job](broker, "concurrent")
	for i := 0; i < total; i++ {
		if err := queue.Enqueue(context.Background(), job{N: i}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	seen := make(chan int, total)
	handler := func(_ context.Context, task taskq.Task[job]) error {
		seen <- task.Payload.N
		return nil
	}

	pool := taskq.NewPool[job](broker, "concurrent", handler, taskq.WithConcurrency(8))

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- pool.Run(ctx) }()

	waitFor(t, func() bool { return len(seen) == total }, 5*time.Second)
	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	close(seen)

	got := make(map[int]bool, total)
	for n := range seen {
		if got[n] {
			t.Errorf("duplicate delivery of task %d", n)
		}
		got[n] = true
	}
	if len(got) != total {
		t.Errorf("got %d unique deliveries, want %d", len(got), total)
	}
}

// TestPool_ContextCancellation verifies Run returns promptly once ctx is
// canceled, and waits for an in-flight handler to finish first.
func TestPool_ContextCancellation(t *testing.T) {
	t.Log("Run should return once ctx is canceled, after any in-flight handler completes")

	broker := membroker.New()
	defer broker.Close()

	queue := taskq.NewQueue[job](broker, "cancel")
	if err := queue.Enqueue(context.Background(), job{N: 1}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var finished int32
	handler := func(_ context.Context, _ taskq.Task[job]) error {
		close(started)
		<-release
		atomic.StoreInt32(&finished, 1)
		return nil
	}

	pool := taskq.NewPool[job](broker, "cancel", handler, taskq.WithConcurrency(1))

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- pool.Run(ctx) }()

	<-started // handler is now blocked inside, holding the only worker
	cancel()  // Run should NOT return yet — the handler hasn't returned

	select {
	case <-runErr:
		t.Fatal("Run returned before the in-flight handler finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(release) // let the handler finish

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after ctx cancellation and handler completion")
	}

	if atomic.LoadInt32(&finished) != 1 {
		t.Error("handler never completed")
	}
}

// TestPool_DecodeFailure_GivesUpImmediately verifies a message whose
// Payload doesn't decode into T is Acked (dropped) without ever reaching
// the handler or being retried.
func TestPool_DecodeFailure_GivesUpImmediately(t *testing.T) {
	t.Log("a message with an undecodable payload should be dropped, not retried")

	broker := membroker.New()
	defer broker.Close()

	// Enqueue directly through the Broker, bypassing Queue[T], so the
	// payload can be deliberately invalid JSON for T=job.
	err := broker.Enqueue(context.Background(), taskq.Message{
		ID:       "bad-payload",
		Queue:    "decode-fail",
		Payload:  []byte("not json"),
		MaxRetry: 5,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	var calls int32
	handler := func(_ context.Context, _ taskq.Task[job]) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}

	pool := taskq.NewPool[job](broker, "decode-fail", handler, taskq.WithConcurrency(1))

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- pool.Run(ctx) }()

	// No observable success signal exists for "it got dropped" (the
	// handler is never called), so this relies on a fixed wait rather than
	// waitFor's polling — weaker than the other tests, and a direct
	// consequence of Ack/Nack errors being unobservable until Phase 5.
	time.Sleep(50 * time.Millisecond)

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Errorf("handler called %d times, want 0 (payload should never decode)", got)
	}
}
