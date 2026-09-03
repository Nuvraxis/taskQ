package taskq

import "context"

// Broker is the entire backend contract. Deliberately tiny — resist adding
// to it. Backend-specific behavior (delayed delivery, priority, SKIP
// LOCKED, …) is modeled as broker-specific constructor options, not
// interface growth.
//
// Ack always means "done, remove it" — whether the task succeeded or the
// caller gave up on it after exhausting retries. Nack always means "requeue
// for redelivery" — it does not decide retry policy; that's the worker
// pool's job (phase 2). The caller passes whatever Attempts value it wants
// persisted on redelivery; the broker just requeues msg as given, verbatim.
type Broker interface {
	Enqueue(ctx context.Context, msg Message) error
	Dequeue(ctx context.Context, queue string) (*Message, error)
	Ack(ctx context.Context, msg Message) error
	Nack(ctx context.Context, msg Message, cause error) error
}
