package taskq

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// recordingHandler is a minimal slog.Handler that stores every record it
// receives, so tests assert on level and structured attributes directly
// instead of parsing formatted text output.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}

func (h *recordingHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(name string) slog.Handler       { return h }

func (h *recordingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.records)
}

func (h *recordingHandler) last() slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.records[len(h.records)-1]
}

func (h *recordingHandler) attr(r slog.Record, key string) (slog.Value, bool) {
	var (
		val   slog.Value
		found bool
	)
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			val, found = a.Value, true
			return false
		}
		return true
	})
	return val, found
}

// --- Chain ---

func TestChain_OrdersOutermostFirst(t *testing.T) {
	var order []string
	mark := func(name string) Middleware[int] {
		return func(next Handler[int]) Handler[int] {
			return func(ctx context.Context, task Task[int]) error {
				order = append(order, name+":before")
				err := next(ctx, task)
				order = append(order, name+":after")
				return err
			}
		}
	}
	base := func(ctx context.Context, task Task[int]) error {
		order = append(order, "base")
		return nil
	}

	h := Chain(base, mark("A"), mark("B"), mark("C"))
	if err := h(context.Background(), Task[int]{}); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	want := []string{"A:before", "B:before", "C:before", "base", "C:after", "B:after", "A:after"}
	if len(order) != len(want) {
		t.Fatalf("call order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("call order = %v, want %v (Chain(h, A, B, C) should run as A(B(C(h))))", order, want)
		}
	}
}

func TestChain_NoMiddleware_CallsBaseDirectly(t *testing.T) {
	called := false
	base := func(ctx context.Context, task Task[int]) error {
		called = true
		return nil
	}
	if err := Chain[int](base)(context.Background(), Task[int]{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("base handler was not called")
	}
}

// --- RecoveryMiddleware ---

func TestRecoveryMiddleware_CatchesPanic(t *testing.T) {
	h := RecoveryMiddleware[int]()(func(ctx context.Context, task Task[int]) error {
		panic("boom")
	})

	err := h(context.Background(), Task[int]{ID: "1"})
	if err == nil {
		t.Fatal("expected error after panic, got nil")
	}

	var panicErr *PanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("error = %v (%T), want *PanicError", err, err)
	}
	if panicErr.Value != "boom" {
		t.Errorf("PanicError.Value = %v, want %q", panicErr.Value, "boom")
	}
	if len(panicErr.Stack()) == 0 {
		t.Error("PanicError.Stack() is empty, want a captured stack trace")
	}
}

func TestRecoveryMiddleware_PassesThroughNormalError(t *testing.T) {
	wantErr := errors.New("ordinary failure")
	h := RecoveryMiddleware[int]()(func(ctx context.Context, task Task[int]) error {
		return wantErr
	})

	if err := h(context.Background(), Task[int]{}); !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want %v (non-panic errors must pass through unchanged)", err, wantErr)
	}
}

func TestRecoveryMiddleware_PassesThroughSuccess(t *testing.T) {
	h := RecoveryMiddleware[int]()(func(ctx context.Context, task Task[int]) error {
		return nil
	})
	if err := h(context.Background(), Task[int]{}); err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestPanicError_Error(t *testing.T) {
	err := &PanicError{Value: "boom"}
	if got, want := err.Error(), "taskq: handler panicked: boom"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

// --- LoggingMiddleware ---

func TestLoggingMiddleware_Success(t *testing.T) {
	rh := &recordingHandler{}
	h := LoggingMiddleware[int](slog.New(rh))(func(ctx context.Context, task Task[int]) error {
		return nil
	})

	task := Task[int]{ID: "t1", Queue: "q1", Attempts: 0, MaxRetry: 3}
	if err := h(context.Background(), task); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rh.count() == 0 {
		t.Fatal("no log record emitted on success")
	}

	rec := rh.last()
	if rec.Level != slog.LevelInfo {
		t.Errorf("level = %v, want Info on success", rec.Level)
	}
	if !strings.Contains(rec.Message, "succeeded") {
		t.Errorf("message = %q, want it to mention success", rec.Message)
	}
}

func TestLoggingMiddleware_RetryableFailure_LogsWarn(t *testing.T) {
	rh := &recordingHandler{}
	wantErr := errors.New("transient")
	h := LoggingMiddleware[int](slog.New(rh))(func(ctx context.Context, task Task[int]) error {
		return wantErr
	})

	// Attempts (1) < MaxRetry (3): Pool.handle will Nack (retry) after
	// this, so it should log at Warn, not Error.
	task := Task[int]{ID: "t1", Queue: "q1", Attempts: 1, MaxRetry: 3}
	if err := h(context.Background(), task); !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}

	if rec := rh.last(); rec.Level != slog.LevelWarn {
		t.Errorf("level = %v, want Warn when Attempts < MaxRetry", rec.Level)
	}
}

func TestLoggingMiddleware_FinalFailure_LogsError(t *testing.T) {
	rh := &recordingHandler{}
	wantErr := errors.New("permanent")
	h := LoggingMiddleware[int](slog.New(rh))(func(ctx context.Context, task Task[int]) error {
		return wantErr
	})

	// Attempts (3) >= MaxRetry (3): per the MaxRetry+1 delivery budget,
	// Pool.handle will Ack (give up) rather than Nack after this call.
	task := Task[int]{ID: "t1", Queue: "q1", Attempts: 3, MaxRetry: 3}
	if err := h(context.Background(), task); !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}

	rec := rh.last()
	if rec.Level != slog.LevelError {
		t.Errorf("level = %v, want Error when Attempts >= MaxRetry", rec.Level)
	}
	if !strings.Contains(rec.Message, "giving up") {
		t.Errorf("message = %q, want it to mention giving up", rec.Message)
	}
}

func TestLoggingMiddleware_NilLogger_UsesDefault(t *testing.T) {
	h := LoggingMiddleware[int](nil)(func(ctx context.Context, task Task[int]) error {
		return nil
	})
	if err := h(context.Background(), Task[int]{ID: "t1", Queue: "q1"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- Composition: this is the test that would have caught the ordering bug ---

func TestChain_RecoveryInnermost_LetsOuterMiddlewareObservePanic(t *testing.T) {
	rh := &recordingHandler{}
	base := func(ctx context.Context, task Task[int]) error {
		panic("kaboom")
	}

	// Logging outer, Recovery inner — the corrected guidance.
	h := Chain(base,
		LoggingMiddleware[int](slog.New(rh)),
		RecoveryMiddleware[int](),
	)

	err := h(context.Background(), Task[int]{ID: "t1", Queue: "q1", MaxRetry: 3})
	var panicErr *PanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("error = %v (%T), want *PanicError", err, err)
	}

	// If Recovery were outer instead, LoggingMiddleware's post-call code
	// would never run — the panic would bypass it during unwind — and
	// count() would be 0 here.
	if rh.count() == 0 {
		t.Fatal("LoggingMiddleware logged nothing; Recovery must sit inside it in the chain")
	}
	if _, found := rh.attr(rh.last(), "stack"); !found {
		t.Error("logged record has no \"stack\" attribute for a *PanicError")
	}
}
