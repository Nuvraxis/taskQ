// Package saga implements the Saga pattern — a durable, multi-step
// workflow with automatic compensation (rollback) — on top of taskq's
// existing Broker/Queue[T]/Pool[T]/Handler[T] primitives. It requires no
// changes to Broker and no backend-specific support: a saga is just
// Queue[Envelope[S]] enqueuing to itself, one hop per step, so membroker,
// redisbroker, and pgbroker all support it automatically.
package saga

import "context"

// Direction indicates which way a saga is currently moving through its
// steps: forward (running Do) or unwinding (running Compensate) after a
// step permanently failed.
type Direction int

const (
	// Forward means the saga is executing steps[StepIndex].Do.
	Forward Direction = iota
	// Compensating means the saga is unwinding: executing
	// steps[StepIndex].Compensate for a step that already succeeded,
	// because some later step failed permanently.
	Compensating
)

func (d Direction) String() string {
	if d == Compensating {
		return "compensating"
	}
	return "forward"
}

// Step is one stage of a saga. Do performs the stage's forward action,
// mutating state to record whatever Compensate will need to undo it —
// e.g. storing a reservation ID before confirming a reservation was made.
// Compensate undoes Do's effect; it may be nil for a step with nothing to
// undo (a pure read or validation step), in which case rollback skips it
// as an automatic no-op.
//
// Both Do and Compensate should be idempotent, for the same reason any
// taskq Handler[T] should be: at-least-once delivery means either can run
// more than once for the same step if a worker crashes between completing
// the work and the envelope for the next hop being durably enqueued.
type Step[S any] struct {
	// Name identifies the step in logs and observability hooks. Not used
	// for control flow — steps run in the order given to New, not by name.
	Name       string
	Do         func(ctx context.Context, state *S) error
	Compensate func(ctx context.Context, state *S) error
}

// Envelope is the payload type carried through the saga's queue — what
// Queue[Envelope[S]] enqueues and Pool[Envelope[S]] dequeues on every hop.
// It is taskq's ordinary JSON-marshaled Task[T] payload; nothing about it
// is special-cased by Broker or any backend.
type Envelope[S any] struct {
	// SagaID identifies one saga run across all its hops — every Do and
	// Compensate step for a single Start call shares one SagaID. Useful
	// for correlating logs and observability hook calls; not used for
	// control flow.
	SagaID string `json:"saga_id"`

	// State is the caller-defined, accumulated saga state — the same
	// value threaded through every step's Do/Compensate, mutated in
	// place as the saga progresses. Unlike a plain Handler[T] chain,
	// every step in one saga shares this single type S rather than each
	// having its own independent payload type.
	State S `json:"state"`

	// StepIndex is which entry in the saga's []Step[S] this envelope is
	// for.
	StepIndex int `json:"step_index"`

	// Direction is Forward or Compensating — see Direction.
	Direction Direction `json:"direction"`

	// FailedStep and FailureReason record which step's Do originally
	// failed and why — set once, at the moment a saga pivots from
	// Forward to Compensating, then carried unchanged through every
	// subsequent Compensating hop. Without this, once rollback reaches
	// StepIndex 0 there would be no way to report which step actually
	// triggered the rollback — only which step's Compensate happened to
	// run last, which is a different (and far less useful) thing. Empty
	// on every Forward-direction envelope. FailureReason is a plain
	// string, not an error, so Envelope stays a plain JSON payload.
	FailedStep    string `json:"failed_step,omitempty"`
	FailureReason string `json:"failure_reason,omitempty"`
}
