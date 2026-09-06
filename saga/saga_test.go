package saga

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/membroker"
)

// orderState is the test payload threaded through every step. Log records
// which Do/Compensate calls actually ran, and in what order — the
// end-to-end tests assert on this to prove steps run and roll back in the
// right sequence, not just that the right hook eventually fires.
type orderState struct {
	Log []string
}

// captureBroker is a minimal taskq.Broker that only records what's
// Enqueue'd — enough to test Handler() in isolation, one call at a time,
// without needing a real queue or a live Pool driving it. Mirrors the
// project's existing captureBroker pattern in queue_test.go.
type captureBroker struct {
	mu       sync.Mutex
	enqueued []taskq.Message
}

func (c *captureBroker) Enqueue(ctx context.Context, msg taskq.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enqueued = append(c.enqueued, msg)
	return nil
}

func (c *captureBroker) Dequeue(ctx context.Context, queue string) (*taskq.Message, error) {
	return nil, errors.New("captureBroker: Dequeue not supported")
}

func (c *captureBroker) Ack(ctx context.Context, msg taskq.Message) error { return nil }

func (c *captureBroker) Nack(ctx context.Context, msg taskq.Message, cause error) error {
	return nil
}

func (c *captureBroker) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.enqueued)
}

func (c *captureBroker) last(t *testing.T) taskq.Message {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.enqueued) == 0 {
		t.Fatal("nothing was enqueued")
	}
	return c.enqueued[len(c.enqueued)-1]
}

func decodeEnvelope(t *testing.T, msg taskq.Message) Envelope[orderState] {
	t.Helper()
	var env Envelope[orderState]
	if err := json.Unmarshal(msg.Payload, &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	return env
}

func TestDirection_String(t *testing.T) {
	if got := Forward.String(); got != "forward" {
		t.Errorf("Forward.String() = %q, want %q", got, "forward")
	}
	if got := Compensating.String(); got != "compensating" {
		t.Errorf("Compensating.String() = %q, want %q", got, "compensating")
	}
}

func TestNew_PanicsOnEmptySteps(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("New did not panic with zero steps")
		}
	}()
	New[orderState](&captureBroker{}, "queue", nil)
}

// TestHandler_Forward covers every branch of handleForward: success
// mid-saga, success on the last step, a retryable failure, a permanent
// failure with nothing to compensate, and a permanent failure that must
// pivot into compensation.
func TestHandler_Forward(t *testing.T) {
	sentinel := errors.New("boom")

	tests := []struct {
		name           string
		stepIndex      int // which of 3 steps this envelope is for
		attempts       int
		maxRetry       int
		doErr          error
		wantErr        error
		wantEnqueued   bool
		wantNextIndex  int
		wantNextDir    Direction
		wantOnComplete bool
		wantOnFailed   bool
		wantFailedStep string
	}{
		{
			name:          "success, not last step, enqueues next forward hop",
			stepIndex:     0,
			wantEnqueued:  true,
			wantNextIndex: 1,
			wantNextDir:   Forward,
		},
		{
			name:           "success, last step, fires OnComplete",
			stepIndex:      2,
			wantOnComplete: true,
		},
		{
			name:      "failure, retries remaining, returns error for Pool to retry",
			stepIndex: 1,
			attempts:  0,
			maxRetry:  2,
			doErr:     sentinel,
			wantErr:   sentinel,
		},
		{
			name:           "failure, retries exhausted, first step, fires OnFailed (nothing to compensate)",
			stepIndex:      0,
			attempts:       2,
			maxRetry:       2,
			doErr:          sentinel,
			wantOnFailed:   true,
			wantFailedStep: "step0",
		},
		{
			name:          "failure, retries exhausted, later step, pivots to compensation",
			stepIndex:     2,
			attempts:      2,
			maxRetry:      2,
			doErr:         sentinel,
			wantEnqueued:  true,
			wantNextIndex: 1,
			wantNextDir:   Compensating,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			broker := &captureBroker{}
			var gotComplete, gotFailed bool
			var gotFailedStep string
			var gotCause error

			mkDo := func(idx int) func(context.Context, *orderState) error {
				return func(ctx context.Context, s *orderState) error {
					if idx == tt.stepIndex {
						return tt.doErr
					}
					return nil
				}
			}
			steps := []Step[orderState]{
				{Name: "step0", Do: mkDo(0)},
				{Name: "step1", Do: mkDo(1)},
				{Name: "step2", Do: mkDo(2)},
			}

			sg := New[orderState](broker, "test-queue", steps,
				WithOnComplete(func(ctx context.Context, sagaID string, state orderState) {
					gotComplete = true
				}),
				WithOnFailed(func(ctx context.Context, sagaID string, state orderState, failedStep string, cause error) {
					gotFailed = true
					gotFailedStep = failedStep
					gotCause = cause
				}),
			)

			task := taskq.Task[Envelope[orderState]]{
				Payload: Envelope[orderState]{
					SagaID:    "saga-1",
					StepIndex: tt.stepIndex,
					Direction: Forward,
				},
				Attempts: tt.attempts,
				MaxRetry: tt.maxRetry,
			}

			err := sg.Handler()(context.Background(), task)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("err = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.wantEnqueued {
				if broker.count() != 1 {
					t.Fatalf("enqueued %d messages, want 1", broker.count())
				}
				env := decodeEnvelope(t, broker.last(t))
				if env.StepIndex != tt.wantNextIndex {
					t.Errorf("next StepIndex = %d, want %d", env.StepIndex, tt.wantNextIndex)
				}
				if env.Direction != tt.wantNextDir {
					t.Errorf("next Direction = %v, want %v", env.Direction, tt.wantNextDir)
				}
			} else if broker.count() != 0 {
				t.Errorf("enqueued %d messages, want 0", broker.count())
			}

			if gotComplete != tt.wantOnComplete {
				t.Errorf("OnComplete fired = %v, want %v", gotComplete, tt.wantOnComplete)
			}
			if gotFailed != tt.wantOnFailed {
				t.Errorf("OnFailed fired = %v, want %v", gotFailed, tt.wantOnFailed)
			}
			if tt.wantOnFailed {
				if gotFailedStep != tt.wantFailedStep {
					t.Errorf("failedStep = %q, want %q", gotFailedStep, tt.wantFailedStep)
				}
				if gotCause == nil {
					t.Error("cause = nil, want the step's error")
				}
			}
		})
	}
}

// TestHandler_Compensating covers every branch of handleCompensating,
// including the bug this test suite was written to catch: once rollback
// reaches step 0, OnFailed must report the step that originally failed
// (carried in the envelope since the pivot), not whichever step's
// Compensate merely happened to run last.
func TestHandler_Compensating(t *testing.T) {
	sentinel := errors.New("rollback boom")

	tests := []struct {
		name             string
		stepIndex        int
		attempts         int
		maxRetry         int
		compensateErr    error
		nilCompensate    bool
		wantErr          error
		wantEnqueued     bool
		wantNextIndex    int
		wantOnFailed     bool
		wantOnCompFailed bool
	}{
		{
			name:          "success, not first step, enqueues next compensating hop",
			stepIndex:     2,
			wantEnqueued:  true,
			wantNextIndex: 1,
		},
		{
			name:         "success, first step, fires OnFailed with the ORIGINAL failure",
			stepIndex:    0,
			wantOnFailed: true,
		},
		{
			name:          "nil Compensate treated as automatic success",
			stepIndex:     1,
			nilCompensate: true,
			wantEnqueued:  true,
			wantNextIndex: 0,
		},
		{
			name:          "failure, retries remaining, returns error for Pool to retry",
			stepIndex:     1,
			attempts:      0,
			maxRetry:      2,
			compensateErr: sentinel,
			wantErr:       sentinel,
		},
		{
			name:             "failure, retries exhausted, fires OnCompensationFailed",
			stepIndex:        1,
			attempts:         2,
			maxRetry:         2,
			compensateErr:    sentinel,
			wantOnCompFailed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			broker := &captureBroker{}
			var gotFailed, gotCompFailed bool
			var gotFailedStep string
			var gotCause error

			mkStep := func(name string, idx int) Step[orderState] {
				s := Step[orderState]{Name: name, Do: func(ctx context.Context, s *orderState) error { return nil }}
				if tt.nilCompensate && idx == tt.stepIndex {
					return s // Compensate left nil
				}
				s.Compensate = func(ctx context.Context, s *orderState) error {
					if idx == tt.stepIndex {
						return tt.compensateErr
					}
					return nil
				}
				return s
			}
			steps := []Step[orderState]{mkStep("step0", 0), mkStep("step1", 1), mkStep("step2", 2)}

			sg := New[orderState](broker, "test-queue", steps,
				WithOnFailed(func(ctx context.Context, sagaID string, state orderState, failedStep string, cause error) {
					gotFailed = true
					gotFailedStep = failedStep
					gotCause = cause
				}),
				WithOnCompensationFailed(func(ctx context.Context, sagaID string, state orderState, step string, cause error) {
					gotCompFailed = true
				}),
			)

			// FailedStep/FailureReason simulate what a real pivot (see
			// TestHandler_Forward) would have already stamped onto the
			// envelope before compensation started — "step2" originally
			// failed, even though this test drives compensation starting
			// at various stepIndex values below step2.
			task := taskq.Task[Envelope[orderState]]{
				Payload: Envelope[orderState]{
					SagaID:        "saga-1",
					StepIndex:     tt.stepIndex,
					Direction:     Compensating,
					FailedStep:    "step2",
					FailureReason: "original failure",
				},
				Attempts: tt.attempts,
				MaxRetry: tt.maxRetry,
			}

			err := sg.Handler()(context.Background(), task)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("err = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.wantEnqueued {
				if broker.count() != 1 {
					t.Fatalf("enqueued %d messages, want 1", broker.count())
				}
				env := decodeEnvelope(t, broker.last(t))
				if env.StepIndex != tt.wantNextIndex {
					t.Errorf("next StepIndex = %d, want %d", env.StepIndex, tt.wantNextIndex)
				}
				if env.Direction != Compensating {
					t.Errorf("next Direction = %v, want Compensating", env.Direction)
				}
			} else if broker.count() != 0 {
				t.Errorf("enqueued %d messages, want 0", broker.count())
			}

			if gotFailed != tt.wantOnFailed {
				t.Errorf("OnFailed fired = %v, want %v", gotFailed, tt.wantOnFailed)
			}
			if tt.wantOnFailed {
				if gotFailedStep != "step2" {
					t.Errorf("failedStep = %q, want %q (the step that ORIGINALLY failed, not step0 — this is the bug this test guards against)", gotFailedStep, "step2")
				}
				if gotCause == nil || gotCause.Error() != "original failure" {
					t.Errorf("cause = %v, want the original failure reason carried from the pivot", gotCause)
				}
			}
			if gotCompFailed != tt.wantOnCompFailed {
				t.Errorf("OnCompensationFailed fired = %v, want %v", gotCompFailed, tt.wantOnCompFailed)
			}
		})
	}
}

func TestHandler_OutOfRangeStepIndex_FiresOnFailed(t *testing.T) {
	broker := &captureBroker{}
	var gotFailed bool
	steps := []Step[orderState]{{Name: "only", Do: func(ctx context.Context, s *orderState) error { return nil }}}
	sg := New[orderState](broker, "q", steps,
		WithOnFailed(func(ctx context.Context, sagaID string, state orderState, failedStep string, cause error) {
			gotFailed = true
		}),
	)

	task := taskq.Task[Envelope[orderState]]{
		Payload: Envelope[orderState]{StepIndex: 5, Direction: Forward},
	}
	if err := sg.Handler()(context.Background(), task); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !gotFailed {
		t.Error("OnFailed did not fire for an out-of-range StepIndex")
	}
	if broker.count() != 0 {
		t.Errorf("enqueued %d messages, want 0", broker.count())
	}
}

// The tests below drive a Saga end-to-end through a real Pool[Envelope[S]]
// on membroker — proving the hop-by-hop transitions tested in isolation
// above actually compose into correct multi-hop behavior when a real
// queue and worker are involved, not just that one call at a time is
// correct.

func TestSaga_EndToEnd_Success(t *testing.T) {
	broker := membroker.New()

	var mu sync.Mutex
	var log []string
	record := func(s string) {
		mu.Lock()
		log = append(log, s)
		mu.Unlock()
	}

	steps := []Step[orderState]{
		{
			Name:       "reserve",
			Do:         func(ctx context.Context, s *orderState) error { record("reserve.do"); return nil },
			Compensate: func(ctx context.Context, s *orderState) error { record("reserve.compensate"); return nil },
		},
		{
			Name:       "charge",
			Do:         func(ctx context.Context, s *orderState) error { record("charge.do"); return nil },
			Compensate: func(ctx context.Context, s *orderState) error { record("charge.compensate"); return nil },
		},
		{
			Name: "ship",
			Do:   func(ctx context.Context, s *orderState) error { record("ship.do"); return nil },
		},
	}

	done := make(chan struct{}, 1)
	sg := New[orderState](broker, "orders", steps,
		WithOnComplete(func(ctx context.Context, sagaID string, state orderState) { done <- struct{}{} }),
	)

	pool := taskq.NewPool[Envelope[orderState]](broker, "orders", sg.Handler())
	ctx, cancel := context.WithCancel(context.Background())
	poolDone := make(chan struct{})
	go func() {
		if err := pool.Run(ctx); err != nil {
			t.Errorf("pool.Run: %v", err)
		}
		close(poolDone)
	}()

	if _, err := sg.Start(ctx, orderState{}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("saga did not reach OnComplete in time")
	}

	cancel()
	broker.Close()
	<-poolDone

	wantLog := []string{"reserve.do", "charge.do", "ship.do"}
	mu.Lock()
	defer mu.Unlock()
	if len(log) != len(wantLog) {
		t.Fatalf("log = %v, want %v", log, wantLog)
	}
	for i := range wantLog {
		if log[i] != wantLog[i] {
			t.Errorf("log[%d] = %q, want %q (steps must run in order)", i, log[i], wantLog[i])
		}
	}
}

func TestSaga_EndToEnd_FailsAndCompensates(t *testing.T) {
	broker := membroker.New()

	var mu sync.Mutex
	var log []string
	record := func(s string) {
		mu.Lock()
		log = append(log, s)
		mu.Unlock()
	}

	steps := []Step[orderState]{
		{
			Name:       "reserve",
			Do:         func(ctx context.Context, s *orderState) error { record("reserve.do"); return nil },
			Compensate: func(ctx context.Context, s *orderState) error { record("reserve.compensate"); return nil },
		},
		{
			Name:       "charge",
			Do:         func(ctx context.Context, s *orderState) error { record("charge.do"); return nil },
			Compensate: func(ctx context.Context, s *orderState) error { record("charge.compensate"); return nil },
		},
		{
			Name: "ship",
			Do: func(ctx context.Context, s *orderState) error {
				record("ship.do")
				return errors.New("carrier unavailable")
			},
			// No Compensate for ship — it never succeeds in this test, so
			// it should never need undoing, and the log assertion below
			// confirms "ship.compensate" never runs.
		},
	}

	type outcome struct {
		failedStep string
		cause      error
	}
	failed := make(chan outcome, 1)

	sg := New[orderState](broker, "orders-fail", steps,
		WithMaxRetry[orderState](0), // give up on the first attempt — no point waiting through retries in a test
		WithOnFailed(func(ctx context.Context, sagaID string, state orderState, failedStep string, cause error) {
			failed <- outcome{failedStep, cause}
		}),
	)

	pool := taskq.NewPool[Envelope[orderState]](broker, "orders-fail", sg.Handler())
	ctx, cancel := context.WithCancel(context.Background())
	poolDone := make(chan struct{})
	go func() {
		if err := pool.Run(ctx); err != nil {
			t.Errorf("pool.Run: %v", err)
		}
		close(poolDone)
	}()

	if _, err := sg.Start(ctx, orderState{}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case got := <-failed:
		if got.failedStep != "ship" {
			t.Errorf("failedStep = %q, want %q (the step that actually failed, not whichever step's Compensate happened to run last)", got.failedStep, "ship")
		}
		if got.cause == nil || got.cause.Error() != "carrier unavailable" {
			t.Errorf("cause = %v, want the original ship error", got.cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("saga did not reach OnFailed in time")
	}

	cancel()
	broker.Close()
	<-poolDone

	// ship never succeeded, so it's never compensated — rollback only
	// covers the steps that actually completed: charge, then reserve.
	wantLog := []string{"reserve.do", "charge.do", "ship.do", "charge.compensate", "reserve.compensate"}
	mu.Lock()
	defer mu.Unlock()
	if len(log) != len(wantLog) {
		t.Fatalf("log = %v, want %v", log, wantLog)
	}
	for i := range wantLog {
		if log[i] != wantLog[i] {
			t.Errorf("log[%d] = %q, want %q", i, log[i], wantLog[i])
		}
	}
}
