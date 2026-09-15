package taskq

import (
	"context"
	"time"
)

// EnqueueOption configures a single Enqueue call.
type EnqueueOption func(*enqueueConfig)

type enqueueConfig struct {
	maxRetry int
}

// WithMaxRetry overrides, for a single Enqueue call, the maximum number of
// redelivery attempts before the worker pool gives up on a task.
func WithMaxRetry(n int) EnqueueOption {
	return func(c *enqueueConfig) {
		c.maxRetry = n
	}
}

// QueueOption configures a Queue[T] at construction time; these become
// defaults that EnqueueOption can override per call.
type QueueOption func(*queueConfig)

type queueConfig struct {
	defaultMaxRetry int
}

// WithDefaultMaxRetry sets the default MaxRetry applied to every task
// enqueued through this Queue, unless overridden per call with WithMaxRetry.
func WithDefaultMaxRetry(n int) QueueOption {
	return func(c *queueConfig) {
		c.defaultMaxRetry = n
	}
}

// PoolOption configures a Pool[T] at construction time.
type PoolOption func(*poolConfig)

// defaultSettleTimeout is the WithSettleTimeout default.
const defaultSettleTimeout = 5 * time.Second

// Values for the op argument of a WithOnSettleError callback, identifying
// which broker call failed.
const (
	// SettleOpAck identifies a failed Broker.Ack.
	SettleOpAck = "ack"
	// SettleOpNack identifies a failed Broker.Nack.
	SettleOpNack = "nack"
)

type poolConfig struct {
	concurrency   int
	backoff       BackoffStrategy
	settleTimeout time.Duration
	onDecodeError func(ctx context.Context, msg Message, err error)
	onSettleError func(ctx context.Context, op string, msg Message, err error)
}

// WithConcurrency sets the number of worker goroutines a Pool runs
// concurrently. Values less than 1 are treated as 1.
func WithConcurrency(n int) PoolOption {
	return func(c *poolConfig) {
		if n < 1 {
			n = 1
		}
		c.concurrency = n
	}
}

// WithBackoffStrategy overrides the default backoff strategy used to space
// out redeliveries after a failed Handler call.
func WithBackoffStrategy(s BackoffStrategy) PoolOption {
	return func(c *poolConfig) { c.backoff = s }
}

// WithOnDecodeError registers a callback invoked when a dequeued Message's
// Payload cannot be decoded into T. The message is still acknowledged and
// dropped afterwards (it would never decode on redelivery). Default: no-op.
//
// ctx carries the values of the ctx passed to Run but is never canceled,
// so the callback still runs usefully during shutdown. A nil fn is ignored.
func WithOnDecodeError(fn func(ctx context.Context, msg Message, err error)) PoolOption {
	return func(c *poolConfig) {
		if fn != nil {
			c.onDecodeError = fn
		}
	}
}

// WithOnSettleError registers a callback invoked when the Pool fails to
// Ack or Nack a message after handling it. op is SettleOpAck or
// SettleOpNack. A failed Ack means the broker will likely redeliver the
// message once its lease/idle window expires; a failed Nack means the
// scheduled retry was lost. Default: no-op.
//
// ctx carries the values of the ctx passed to Run but is never canceled.
// For a scheduled Nack the callback may fire after Run has returned. A nil
// fn is ignored.
func WithOnSettleError(fn func(ctx context.Context, op string, msg Message, err error)) PoolOption {
	return func(c *poolConfig) {
		if fn != nil {
			c.onSettleError = fn
		}
	}
}

// WithSettleTimeout bounds how long an Ack or Nack may take after the
// handler returns. It applies even during shutdown, because settlement
// runs on a context detached from Run's cancellation. Default: 5s.
//
// Values less than or equal to zero are treated as the default: an
// unbounded settle could hang Run's shutdown on a stuck broker, and a zero
// timeout would fail every settle.
func WithSettleTimeout(d time.Duration) PoolOption {
	return func(c *poolConfig) {
		if d <= 0 {
			d = defaultSettleTimeout
		}
		c.settleTimeout = d
	}
}
