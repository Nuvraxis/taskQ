package otelmw

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	taskq "github.com/Nuvraxis/taskQ"
)

// TracingBroker wraps any taskq.Broker with a span around each Enqueue,
// Dequeue, Ack, and Nack call — transport-level latency, as opposed to
// Middleware[T]'s handler-level spans. It implements taskq.Broker directly,
// so it drops into Queue[T], NewPool, or a hand-rolled consumer loop with
// no other code changes:
//
//	broker := otelmw.NewTracingBroker(membroker.New(), tracer)
//	h := taskq.Chain(base, otelmw.Middleware[T](tracer), taskq.RecoveryMiddleware[T]())
//	pool := taskq.NewPool(broker, "payments", h)
type TracingBroker struct {
	next   taskq.Broker
	tracer trace.Tracer
}

// NewTracingBroker wraps next with tracing. tracer is typically
// otel.Tracer("github.com/Nuvraxis/taskQ").
func NewTracingBroker(next taskq.Broker, tracer trace.Tracer) *TracingBroker {
	return &TracingBroker{next: next, tracer: tracer}
}

// Enqueue traces a call to the wrapped Broker's Enqueue.
func (b *TracingBroker) Enqueue(ctx context.Context, msg taskq.Message) error {
	ctx, span := b.tracer.Start(ctx, "taskq.broker.Enqueue "+msg.Queue,
		trace.WithAttributes(attribute.String("taskq.queue", msg.Queue), attribute.String("taskq.task_id", msg.ID)))
	defer span.End()
	err := b.next.Enqueue(ctx, msg)
	setSpanOutcome(span, err)
	return err
}

// Dequeue traces a call to the wrapped Broker's Dequeue.
func (b *TracingBroker) Dequeue(ctx context.Context, queue string) (*taskq.Message, error) {
	ctx, span := b.tracer.Start(ctx, "taskq.broker.Dequeue "+queue,
		trace.WithAttributes(attribute.String("taskq.queue", queue)))
	defer span.End()
	msg, err := b.next.Dequeue(ctx, queue)
	setSpanOutcome(span, err)
	if msg != nil {
		span.SetAttributes(attribute.String("taskq.task_id", msg.ID), attribute.Int("taskq.attempt", msg.Attempts+1))
	}
	return msg, err
}

// Ack traces a call to the wrapped Broker's Ack.
func (b *TracingBroker) Ack(ctx context.Context, msg taskq.Message) error {
	ctx, span := b.tracer.Start(ctx, "taskq.broker.Ack "+msg.Queue,
		trace.WithAttributes(attribute.String("taskq.queue", msg.Queue), attribute.String("taskq.task_id", msg.ID)))
	defer span.End()
	err := b.next.Ack(ctx, msg)
	setSpanOutcome(span, err)
	return err
}

// Nack traces a call to the wrapped Broker's Nack.
func (b *TracingBroker) Nack(ctx context.Context, msg taskq.Message, cause error) error {
	ctx, span := b.tracer.Start(ctx, "taskq.broker.Nack "+msg.Queue,
		trace.WithAttributes(attribute.String("taskq.queue", msg.Queue), attribute.String("taskq.task_id", msg.ID)))
	defer span.End()
	err := b.next.Nack(ctx, msg, cause)
	setSpanOutcome(span, err)
	return err
}

func setSpanOutcome(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	span.SetStatus(codes.Ok, "")
}

var _ taskq.Broker = (*TracingBroker)(nil)
