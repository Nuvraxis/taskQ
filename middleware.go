package taskq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"
)

// Middleware wraps a Handler[T], adding cross-cutting behavior — logging,
// panic recovery, tracing — without changing what the handler does. Same
// shape as net/http middleware: a function from Handler to Handler.
type Middleware[T any] func(Handler[T]) Handler[T]

// Chain composes middleware around a base Handler[T]. Chain(h, A, B, C)
// runs as A(B(C(h))): the first middleware listed is outermost — it sees
// the task first and the returned error last.
//
// Put RecoveryMiddleware LAST (innermost, directly wrapping h), not first:
// a panic it recovers becomes a normal *PanicError return, and only
// middleware listed *before* it in the chain — i.e. wrapping it from
// outside — ever sees that return value to log or trace. Recovery's job is
// to protect against a panicking Handler; it does not protect against a
// panic inside another middleware's own code, regardless of position.
//
//	h := taskq.Chain(baseHandler,
//	    taskq.LoggingMiddleware[T](logger),  // outermost: logs the outcome, including panics
//	    otelmw.Middleware[T](tracer),        // spans decode+handler, including panics
//	    taskq.RecoveryMiddleware[T](),       // innermost: converts a handler panic to an error
//	)
//	pool := taskq.NewPool(broker, queue, h, opts...)
func Chain[T any](h Handler[T], mws ...Middleware[T]) Handler[T] {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// PanicError wraps a value recovered from a panicking Handler. Error()
// stays short so it doesn't bloat a broker's persisted Nack cause; the full
// stack trace captured at the moment of the panic is available via Stack()
// for middleware (LoggingMiddleware, or an OTel middleware) that wants to
// record it separately.
type PanicError struct {
	Value any
	stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("taskq: handler panicked: %v", e.Value)
}

// Stack returns the stack trace captured at the moment of the panic.
func (e *PanicError) Stack() []byte { return e.stack }

// RecoveryMiddleware recovers from a panic in the wrapped Handler and
// converts it into a *PanicError, so one bad task can't take down a Pool
// worker. Without this, Pool.runWorker has nothing catching a panic: it
// propagates up the goroutine's stack and, per normal Go semantics, crashes
// the process. With it, a panic becomes an ordinary handler error, subject
// to the same Attempts/MaxRetry retry decision as any other failure.
func RecoveryMiddleware[T any]() Middleware[T] {
	return func(next Handler[T]) Handler[T] {
		return func(ctx context.Context, task Task[T]) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = &PanicError{Value: r, stack: debug.Stack()}
				}
			}()
			return next(ctx, task)
		}
	}
}

// LoggingMiddleware logs each task's start, outcome, and duration via
// logger (log/slog; slog.Default() if nil).
//
// A failed attempt is logged at Warn if the Pool will retry it, or Error if
// this was the last allowed attempt. That distinction is derived from
// task.Attempts and task.MaxRetry alone — Task.Attempts is the delivery
// count *before* this attempt, so per the MaxRetry+1 delivery budget
// (README), task.Attempts >= task.MaxRetry means Pool.handle will Ack
// (give up) rather than Nack (retry) after this call returns. No Pool
// changes are needed to know that.
//
// If the error is a *PanicError, its stack trace is attached as a separate
// log attribute rather than folded into the error string.
func LoggingMiddleware[T any](logger *slog.Logger) Middleware[T] {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next Handler[T]) Handler[T] {
		return func(ctx context.Context, task Task[T]) error {
			start := time.Now()
			logger.DebugContext(ctx, "task started",
				"task_id", task.ID, "queue", task.Queue, "attempt", task.Attempts+1)

			err := next(ctx, task)
			dur := time.Since(start)

			if err == nil {
				logger.InfoContext(ctx, "task succeeded",
					"task_id", task.ID, "queue", task.Queue,
					"attempt", task.Attempts+1, "duration", dur)
				return nil
			}

			attrs := []any{
				"task_id", task.ID, "queue", task.Queue,
				"attempt", task.Attempts + 1, "max_retry", task.MaxRetry,
				"duration", dur, "error", err,
			}
			var panicErr *PanicError
			if errors.As(err, &panicErr) {
				attrs = append(attrs, "stack", string(panicErr.Stack()))
			}

			finalAttempt := task.Attempts >= task.MaxRetry
			if finalAttempt {
				logger.ErrorContext(ctx, "task failed, retries exhausted — giving up", attrs...)
			} else {
				logger.WarnContext(ctx, "task failed, will retry", attrs...)
			}
			return err
		}
	}
}
