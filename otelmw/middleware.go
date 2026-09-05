package otelmw

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	taskq "github.com/Nuvraxis/taskQ"
)

// Middleware starts a span around each Handler[T] invocation, named
// "taskq.handle <queue>", tagged with the task ID, queue, attempt number,
// and queue latency (time from EnqueuedAt to now). tracer is typically
// otel.Tracer("github.com/Nuvraxis/taskQ").
//
// On error, the span is marked Error and the error recorded; a *PanicError
// (see the root package's RecoveryMiddleware) additionally gets its stack
// trace attached as a span event, so a panic is visible in traces without
// bloating every span's attributes with a stack on ordinary failures.
func Middleware[T any](tracer trace.Tracer) taskq.Middleware[T] {
	return func(next taskq.Handler[T]) taskq.Handler[T] {
		return func(ctx context.Context, task taskq.Task[T]) error {
			ctx, span := tracer.Start(ctx, "taskq.handle "+task.Queue,
				trace.WithAttributes(
					attribute.String("taskq.task_id", task.ID),
					attribute.String("taskq.queue", task.Queue),
					attribute.Int("taskq.attempt", task.Attempts+1),
					attribute.Int("taskq.max_retry", task.MaxRetry),
					attribute.Int64("taskq.queue_latency_ms", time.Since(task.EnqueuedAt).Milliseconds()),
				),
			)
			defer span.End()

			err := next(ctx, task)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())

				var panicErr *taskq.PanicError
				if errors.As(err, &panicErr) {
					span.AddEvent("panic", trace.WithAttributes(
						attribute.String("taskq.panic.stack", string(panicErr.Stack())),
					))
				}
				return err
			}

			span.SetStatus(codes.Ok, "")
			return nil
		}
	}
}
