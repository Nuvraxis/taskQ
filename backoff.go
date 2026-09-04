package taskq

import (
	"math"
	"math/rand"
	"time"
)

// BackoffStrategy computes the delay before the nth redelivery of a failed
// task. attempt is 1-indexed: Next(1) is the delay before the first retry,
// Next(2) before the second, and so on — it mirrors the Attempts value the
// worker pool is about to persist via Nack.
type BackoffStrategy interface {
	Next(attempt int) time.Duration
}

// ConstantBackoff waits a fixed Delay before every retry.
type ConstantBackoff struct {
	Delay time.Duration
}

// Next returns Delay, unaffected by attempt.
func (b ConstantBackoff) Next(attempt int) time.Duration {
	return b.Delay
}

// ExponentialBackoff waits Base * Factor^(attempt-1), capped at Max once
// Max is positive. Factor defaults to 2 if zero. If Jitter is true, the
// computed delay is randomized uniformly over [0, delay] to avoid
// thundering-herd redelivery when many tasks fail together.
type ExponentialBackoff struct {
	Base   time.Duration
	Max    time.Duration
	Factor float64
	Jitter bool
}

// Next returns the exponential delay for attempt, capped and jittered per
// the struct's fields.
func (b ExponentialBackoff) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	factor := b.Factor
	if factor == 0 {
		factor = 2
	}

	delay := float64(b.Base) * math.Pow(factor, float64(attempt-1))
	if b.Max > 0 && delay > float64(b.Max) {
		delay = float64(b.Max)
	}
	if b.Jitter {
		delay *= rand.Float64()
	}
	return time.Duration(delay)
}
