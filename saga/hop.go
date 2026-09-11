package saga

import (
	"context"
	"strconv"
)

// Hop identifies the unit of work a Step function is currently running as:
// which saga run, which step, and in which direction. The triple is stable
// across retries and crash redeliveries of the same hop, which makes it a
// ready-made idempotency key for side effects inside Do and Compensate.
type Hop struct {
	SagaID    string
	StepIndex int
	Direction Direction
}

// Key returns the hop as a single string, "<sagaID>:<step>:<direction>",
// suitable for a unique column or a provider idempotency-key header.
func (h *Hop) Key() string {
	return h.SagaID + ":" + strconv.Itoa(h.StepIndex) + ":" + h.Direction.String()
}

type hopContextKey struct{}

// withHop attaches h to ctx for the duration of one Do/Compensate call.
func withHop(ctx context.Context, h Hop) context.Context {
	return context.WithValue(ctx, hopContextKey{}, h)
}

// HopFromContext returns the Hop for the step currently executing. ok is
// false when ctx did not come from a Saga handler.
func HopFromContext(ctx context.Context) (h Hop, ok bool) {
	h, ok = ctx.Value(hopContextKey{}).(Hop)
	return h, ok
}
