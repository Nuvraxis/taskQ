package taskq_test

import (
	"context"
	"errors"
	"slices"
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

// runPool starts pool.Run in the background. The returned stop cancels
// Run's ctx and waits for Run to return, failing the test if it errors or
// takes longer than a second.
func runPool[T any](t *testing.T, pool *taskq.Pool[T]) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- pool.Run(ctx) }()
	return func() {
		t.Helper()
		cancel()
		select {
		case err := <-runErr:
			if err != nil {
				t.Fatalf("Run returned %v, want nil", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Run did not return within 1s of ctx cancellation")
		}
	}
}

// settleCall records one Ack or Nack as the broker received it. ctxErr and
// hasDeadline are captured at call time: the Pool cancels its settle ctx as
// soon as the call returns, so inspecting the ctx afterwards would always
// show it canceled.
type settleCall struct {
	msg         taskq.Message
	ctxErr      error
	hasDeadline bool
}

// stubBroker is a scriptable Broker for Pool tests. Dequeue serves messages
// from a buffered channel and blocks on ctx otherwise. Ack and Nack record
// every call, then defer to ackFn/nackFn when set; with no nackFn, Nack
// requeues the message verbatim like a real broker. Set ackFn/nackFn before
// starting the Pool.
type stubBroker struct {
	msgs   chan taskq.Message
	ackFn  func(ctx context.Context, msg taskq.Message) error
	nackFn func(ctx context.Context, msg taskq.Message, cause error) error

	mu    sync.Mutex
	acks  []settleCall
	nacks []settleCall
}

func newStubBroker(msgs ...taskq.Message) *stubBroker {
	b := &stubBroker{msgs: make(chan taskq.Message, 16)}
	for _, msg := range msgs {
		b.msgs <- msg
	}
	return b
}

func (b *stubBroker) Enqueue(_ context.Context, msg taskq.Message) error {
	b.msgs <- msg
	return nil
}

func (b *stubBroker) Dequeue(ctx context.Context, _ string) (*taskq.Message, error) {
	select {
	case msg := <-b.msgs:
		return &msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *stubBroker) Ack(ctx context.Context, msg taskq.Message) error {
	b.record(ctx, &b.acks, msg)
	if b.ackFn != nil {
		return b.ackFn(ctx, msg)
	}
	return nil
}

func (b *stubBroker) Nack(ctx context.Context, msg taskq.Message, cause error) error {
	b.record(ctx, &b.nacks, msg)
	if b.nackFn != nil {
		return b.nackFn(ctx, msg, cause)
	}
	b.msgs <- msg
	return nil
}

func (b *stubBroker) record(ctx context.Context, calls *[]settleCall, msg taskq.Message) {
	_, hasDeadline := ctx.Deadline()
	b.mu.Lock()
	defer b.mu.Unlock()
	*calls = append(*calls, settleCall{msg: msg, ctxErr: ctx.Err(), hasDeadline: hasDeadline})
}

func (b *stubBroker) ackCalls() []settleCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.acks)
}

func (b *stubBroker) nackCalls() []settleCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.nacks)
}

func stubMessage(id, payload string) taskq.Message {
	return taskq.Message{
		ID:         id,
		Queue:      "stub",
		Payload:    []byte(payload),
		MaxRetry:   3,
		EnqueuedAt: time.Now(),
	}
}

// hookCall records one WithOnDecodeError or WithOnSettleError callback. op
// is empty for decode hooks. ctxErr is the hook ctx's Err() at call time.
type hookCall struct {
	op     string
	msg    taskq.Message
	err    error
	ctxErr error
}

// hookRecorder captures both Pool failure hooks.
type hookRecorder struct {
	mu      sync.Mutex
	decodes []hookCall
	settles []hookCall
}

func (r *hookRecorder) options() []taskq.PoolOption {
	return []taskq.PoolOption{
		taskq.WithOnDecodeError(func(ctx context.Context, msg taskq.Message, err error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.decodes = append(r.decodes, hookCall{msg: msg, err: err, ctxErr: ctx.Err()})
		}),
		taskq.WithOnSettleError(func(ctx context.Context, op string, msg taskq.Message, err error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.settles = append(r.settles, hookCall{op: op, msg: msg, err: err, ctxErr: ctx.Err()})
		}),
	}
}

func (r *hookRecorder) snapshot() (decodes, settles []hookCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.decodes), slices.Clone(r.settles)
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
// Payload doesn't decode into T is reported, then dropped without ever
// reaching the handler or being retried.
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

	decoded := make(chan error, 2)
	pool := taskq.NewPool[job](broker, "decode-fail", handler,
		taskq.WithConcurrency(1),
		taskq.WithOnDecodeError(func(_ context.Context, _ taskq.Message, err error) { decoded <- err }),
	)
	stop := runPool(t, pool)

	select {
	case err := <-decoded:
		if err == nil {
			t.Error("decode hook received a nil error")
		}
	case <-time.After(time.Second):
		t.Fatal("decode hook never fired")
	}

	// Give a (wrong) redelivery a window to show up before stopping.
	time.Sleep(50 * time.Millisecond)
	stop()

	if n := len(decoded); n != 0 {
		t.Errorf("decode hook fired %d more time(s), want 0 (message should not be redelivered)", n)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Errorf("handler called %d times, want 0 (payload should never decode)", got)
	}
}

// TestPool_FailureReporting verifies decode failures and Ack/Nack failures
// reach WithOnDecodeError / WithOnSettleError instead of being dropped, and
// that a Pool with neither hook configured tolerates the same failures.
func TestPool_FailureReporting(t *testing.T) {
	t.Log("decode and settle failures should be reported through the Pool hooks, and be safe without them")

	errAck := errors.New("ack failed")
	errNack := errors.New("nack failed")

	failAck := func(context.Context, taskq.Message) error { return errAck }
	blockAck := func(ctx context.Context, _ taskq.Message) error {
		<-ctx.Done()
		return ctx.Err()
	}

	tests := []struct {
		name    string
		msg     taskq.Message
		fail    bool // handler returns an error instead of nil
		ackFn   func(ctx context.Context, msg taskq.Message) error
		nackFn  func(ctx context.Context, msg taskq.Message, cause error) error
		opts    []taskq.PoolOption
		noHooks bool

		wantHandlerCalls int32
		wantAcks         int
		wantNacks        int
		wantDecodeHooks  int
		wantSettleOps    []string
		wantSettleErr    error // errors.Is target for every settle hook call
	}{
		{
			name:            "decode error fires hook and acks once",
			msg:             stubMessage("bad", "not json"),
			wantAcks:        1,
			wantDecodeHooks: 1,
		},
		{
			name:            "decode error with failing ack fires both hooks",
			msg:             stubMessage("bad", "not json"),
			ackFn:           failAck,
			wantAcks:        1,
			wantDecodeHooks: 1,
			wantSettleOps:   []string{taskq.SettleOpAck},
			wantSettleErr:   errAck,
		},
		{
			name:             "ack error is reported as SettleOpAck",
			msg:              stubMessage("good", `{"n":1}`),
			ackFn:            failAck,
			wantHandlerCalls: 1,
			wantAcks:         1,
			wantSettleOps:    []string{taskq.SettleOpAck},
			wantSettleErr:    errAck,
		},
		{
			name:             "nack error is reported as SettleOpNack",
			msg:              stubMessage("good", `{"n":1}`),
			fail:             true,
			nackFn:           func(context.Context, taskq.Message, error) error { return errNack },
			opts:             []taskq.PoolOption{taskq.WithBackoffStrategy(taskq.ConstantBackoff{Delay: time.Millisecond})},
			wantHandlerCalls: 1,
			wantNacks:        1,
			wantSettleOps:    []string{taskq.SettleOpNack},
			wantSettleErr:    errNack,
		},
		{
			name:             "ack blocked past settle timeout reports DeadlineExceeded",
			msg:              stubMessage("good", `{"n":1}`),
			ackFn:            blockAck,
			opts:             []taskq.PoolOption{taskq.WithSettleTimeout(50 * time.Millisecond)},
			wantHandlerCalls: 1,
			wantAcks:         1,
			wantSettleOps:    []string{taskq.SettleOpAck},
			wantSettleErr:    context.DeadlineExceeded,
		},
		{
			name:     "defaults tolerate decode and ack failures without hooks",
			msg:      stubMessage("bad", "not json"),
			ackFn:    failAck,
			noHooks:  true,
			wantAcks: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newStubBroker(tt.msg)
			b.ackFn = tt.ackFn
			b.nackFn = tt.nackFn

			var calls int32
			handler := func(_ context.Context, _ taskq.Task[job]) error {
				atomic.AddInt32(&calls, 1)
				if tt.fail {
					return errors.New("handler failed")
				}
				return nil
			}

			rec := &hookRecorder{}
			opts := slices.Clone(tt.opts)
			if !tt.noHooks {
				opts = append(opts, rec.options()...)
			}
			stop := runPool(t, taskq.NewPool[job](b, "stub", handler, opts...))

			waitFor(t, func() bool {
				decodes, settles := rec.snapshot()
				return atomic.LoadInt32(&calls) >= tt.wantHandlerCalls &&
					len(b.ackCalls()) >= tt.wantAcks &&
					len(b.nackCalls()) >= tt.wantNacks &&
					len(decodes) >= tt.wantDecodeHooks &&
					len(settles) >= len(tt.wantSettleOps)
			}, time.Second)
			stop()

			if got := atomic.LoadInt32(&calls); got != tt.wantHandlerCalls {
				t.Errorf("handler called %d times, want %d", got, tt.wantHandlerCalls)
			}

			acks, nacks := b.ackCalls(), b.nackCalls()
			if len(acks) != tt.wantAcks {
				t.Errorf("broker got %d Ack calls, want %d", len(acks), tt.wantAcks)
			}
			if len(nacks) != tt.wantNacks {
				t.Errorf("broker got %d Nack calls, want %d", len(nacks), tt.wantNacks)
			}
			for _, c := range slices.Concat(acks, nacks) {
				if !c.hasDeadline {
					t.Errorf("settle call for %s had no deadline, want one bounded by the settle timeout", c.msg.ID)
				}
			}

			decodes, settles := rec.snapshot()
			if len(decodes) != tt.wantDecodeHooks {
				t.Errorf("decode hook fired %d times, want %d", len(decodes), tt.wantDecodeHooks)
			}
			for _, d := range decodes {
				if d.msg.ID != tt.msg.ID || string(d.msg.Payload) != string(tt.msg.Payload) {
					t.Errorf("decode hook got message %s %q, want the raw message %s %q",
						d.msg.ID, d.msg.Payload, tt.msg.ID, tt.msg.Payload)
				}
				if d.err == nil {
					t.Error("decode hook got a nil error")
				}
				if d.ctxErr != nil {
					t.Errorf("decode hook ctx.Err() = %v, want nil", d.ctxErr)
				}
			}

			gotOps := make([]string, 0, len(settles))
			for _, s := range settles {
				gotOps = append(gotOps, s.op)
				if !errors.Is(s.err, tt.wantSettleErr) {
					t.Errorf("settle hook (%s) got error %v, want %v", s.op, s.err, tt.wantSettleErr)
				}
				if s.msg.ID != tt.msg.ID {
					t.Errorf("settle hook (%s) got message %s, want %s", s.op, s.msg.ID, tt.msg.ID)
				}
				if s.ctxErr != nil {
					t.Errorf("settle hook (%s) ctx.Err() = %v, want nil", s.op, s.ctxErr)
				}
			}
			if !slices.Equal(gotOps, tt.wantSettleOps) {
				t.Errorf("settle hook ops = %v, want %v", gotOps, tt.wantSettleOps)
			}
		})
	}
}

// TestPool_AckDetachedFromRunCancellation verifies a handler that finishes
// after Run's ctx is canceled is still Acked on a live, bounded ctx, and
// that Run waits for that Ack before returning. Before settles were
// detached, this Ack ran on the canceled ctx and failed on pgbroker and
// redisbroker, so the task was redelivered after its lease expired.
func TestPool_AckDetachedFromRunCancellation(t *testing.T) {
	t.Log("an in-flight task that finishes during shutdown should be Acked on a live ctx before Run returns")

	b := newStubBroker(stubMessage("in-flight", `{"n":1}`))
	var ackDone atomic.Bool
	b.ackFn = func(ctx context.Context, _ taskq.Message) error {
		time.Sleep(20 * time.Millisecond) // a slow Ack that Run must wait out
		ackDone.Store(true)
		return ctx.Err() // what a real broker's query would surface
	}

	started := make(chan struct{})
	release := make(chan struct{})
	handler := func(_ context.Context, _ taskq.Task[job]) error {
		close(started)
		<-release
		return nil
	}

	rec := &hookRecorder{}
	pool := taskq.NewPool[job](b, "stub", handler, rec.options()...)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- pool.Run(ctx) }()

	<-started
	cancel()       // shutdown begins while the handler is still running
	close(release) // the handler then completes successfully

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after ctx cancellation and handler completion")
	}

	// No waitFor: Run has returned, so the Ack must already have finished.
	if !ackDone.Load() {
		t.Fatal("Run returned before the in-flight Ack completed")
	}
	acks := b.ackCalls()
	if len(acks) != 1 {
		t.Fatalf("broker got %d Ack calls, want 1", len(acks))
	}
	if acks[0].ctxErr != nil {
		t.Errorf("Ack ctx.Err() = %v, want nil (settle ctx must be detached from Run's cancellation)", acks[0].ctxErr)
	}
	if !acks[0].hasDeadline {
		t.Error("Ack ctx had no deadline, want one bounded by the settle timeout")
	}
	if _, settles := rec.snapshot(); len(settles) != 0 {
		t.Errorf("settle hook fired %d times, want 0: %+v", len(settles), settles)
	}
}

// TestPool_ScheduledNackOutlivesSettleTimeout verifies the settle timeout
// for a scheduled retry starts when the backoff timer fires, not when the
// retry is scheduled. With a backoff longer than the settle timeout, a
// ctx created at scheduling time would already be expired when Nack ran,
// and every retry would be lost.
func TestPool_ScheduledNackOutlivesSettleTimeout(t *testing.T) {
	t.Log("a Nack scheduled with a backoff longer than the settle timeout should still run on a live ctx and be redelivered")

	b := newStubBroker(stubMessage("flaky", `{"n":1}`))
	b.nackFn = func(ctx context.Context, msg taskq.Message, _ error) error {
		if err := ctx.Err(); err != nil {
			return err // a real broker's query would fail on an expired ctx
		}
		return b.Enqueue(ctx, msg)
	}

	var (
		mu       sync.Mutex
		attempts []int
	)
	handler := func(_ context.Context, task taskq.Task[job]) error {
		mu.Lock()
		attempts = append(attempts, task.Attempts)
		mu.Unlock()
		if task.Attempts == 0 {
			return errors.New("transient failure")
		}
		return nil
	}

	rec := &hookRecorder{}
	opts := append([]taskq.PoolOption{
		taskq.WithBackoffStrategy(taskq.ConstantBackoff{Delay: 100 * time.Millisecond}),
		taskq.WithSettleTimeout(20 * time.Millisecond),
	}, rec.options()...)
	stop := runPool(t, taskq.NewPool[job](b, "stub", handler, opts...))

	// Stop on the retry's Ack, or on a reported Nack failure, so a
	// regression fails on the ctx assertion below rather than a timeout.
	waitFor(t, func() bool {
		_, settles := rec.snapshot()
		return len(b.ackCalls()) == 1 || len(settles) > 0
	}, 2*time.Second)
	stop()

	nacks := b.nackCalls()
	if len(nacks) != 1 {
		t.Fatalf("broker got %d Nack calls, want 1", len(nacks))
	}
	if nacks[0].ctxErr != nil {
		t.Errorf("Nack ctx.Err() = %v, want nil (settle timeout must start when the backoff timer fires)", nacks[0].ctxErr)
	}
	if !nacks[0].hasDeadline {
		t.Error("Nack ctx had no deadline, want one bounded by the settle timeout")
	}

	mu.Lock()
	defer mu.Unlock()
	if want := []int{0, 1}; !slices.Equal(attempts, want) {
		t.Errorf("handler saw attempts %v, want %v (the retry should have been delivered)", attempts, want)
	}
	if acks := b.ackCalls(); len(acks) != 1 {
		t.Errorf("broker got %d Ack calls, want 1", len(acks))
	} else if acks[0].msg.Attempts != 1 {
		t.Errorf("acked message has Attempts %d, want 1", acks[0].msg.Attempts)
	}
	if _, settles := rec.snapshot(); len(settles) != 0 {
		t.Errorf("settle hook fired %d times, want 0: %+v", len(settles), settles)
	}
}
