package taskq

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Pool binds a Broker and queue name to a Handler[T] and runs it against
// dequeued tasks with a fixed number of concurrent workers. It's the
// consumer-side counterpart to Queue[T] — one Pool per broker/queue/T.
type Pool[T any] struct {
	broker  Broker
	queue   string
	handler Handler[T]
	cfg     poolConfig
}

// NewPool creates a Pool bound to broker and queue, dispatching decoded
// tasks to handler. opts configure concurrency, backoff, settlement, and
// failure hooks — see WithConcurrency, WithBackoffStrategy,
// WithSettleTimeout, WithOnDecodeError, and WithOnSettleError. The default
// is a single worker with an ExponentialBackoff (1s base, 30s max, factor
// 2, jittered), a 5s settle timeout, and no-op hooks.
func NewPool[T any](broker Broker, queue string, handler Handler[T], opts ...PoolOption) *Pool[T] {
	cfg := poolConfig{
		concurrency: 1,
		backoff: ExponentialBackoff{
			Base:   time.Second,
			Max:    30 * time.Second,
			Factor: 2,
			Jitter: true,
		},
		settleTimeout: defaultSettleTimeout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Pool[T]{broker: broker, queue: queue, handler: handler, cfg: cfg}
}

// Run starts cfg.concurrency worker goroutines and blocks until ctx is
// canceled or the broker reports it's closed, then waits for every
// worker's in-flight Handler call — and the Ack that follows it — to
// return before returning itself. A Handler that ignores ctx can therefore
// block Run's return indefinitely — the same caveat as net/http's graceful
// shutdown. The Ack itself is bounded by WithSettleTimeout.
//
// Acks and Nacks run on a context detached from ctx's cancellation (it
// keeps ctx's values, such as a trace span), bounded by WithSettleTimeout.
// A handler that finishes during shutdown is therefore still Acked, rather
// than failing its Ack on a canceled ctx and being redelivered once the
// broker's lease or idle window expires.
//
// Retry delays use an async timer (see BackoffStrategy) rather than
// blocking a worker: a failed task's Nack is scheduled with time.AfterFunc
// and the worker resumes dequeuing immediately. Run does NOT wait for
// scheduled retries to fire — only for handlers already running when
// shutdown begins. A scheduled Nack still fires even after Run has
// returned; its settle timeout starts when the timer fires, not when the
// retry is scheduled, and a failure is reported via WithOnSettleError.
//
// Run returns nil on expected shutdown (ctx canceled or the broker
// closed). Any other Dequeue error is returned as-is. Ack/Nack failures
// are reported through WithOnSettleError, never returned.
func (p *Pool[T]) Run(ctx context.Context) error {
	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		runErr  error
	)

	for i := 0; i < p.cfg.concurrency; i++ {
		wg.Go(func() {
			if err := p.runWorker(ctx); err != nil {
				errOnce.Do(func() { runErr = err })
			}
		})
	}

	wg.Wait()
	return runErr
}

// runWorker is the per-goroutine dequeue/handle loop. It returns nil on
// expected shutdown (ctx canceled or the broker closed) and a non-nil
// error only for an unexpected Dequeue failure.
func (p *Pool[T]) runWorker(ctx context.Context) error {
	for {
		msg, err := p.broker.Dequeue(ctx, p.queue)
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) ||
				errors.Is(err, ErrQueueClosed) {
				return nil
			}
			return err
		}

		p.handle(ctx, *msg)
	}
}

// handle decodes msg, runs the handler, and either Acks (success, retries
// exhausted, or a malformed payload that retrying won't fix) or schedules
// a backoff Nack with the incremented Attempts count. It runs synchronously
// on a worker goroutine, so Run's WaitGroup covers the Ack as well as the
// handler; the scheduled Nack is deliberately outside it.
func (p *Pool[T]) handle(ctx context.Context, msg Message) {
	task, decodeErr := decode[T](msg)
	if decodeErr != nil {
		if p.cfg.onDecodeError != nil {
			p.cfg.onDecodeError(context.WithoutCancel(ctx), msg, decodeErr)
		}
		p.ack(ctx, msg) // it would never decode on redelivery — drop it
		return
	}

	handlerErr := p.handler(ctx, task)
	if handlerErr == nil {
		p.ack(ctx, msg)
		return
	}

	msg.Attempts++
	if msg.Attempts > msg.MaxRetry {
		p.ack(ctx, msg) // exhausted retries — done, remove it
		return
	}

	delay := p.cfg.backoff.Next(msg.Attempts)
	time.AfterFunc(delay, func() {
		// settleCtx is created here, when the timer fires, so a backoff
		// longer than the settle timeout doesn't Nack on an expired ctx.
		p.nack(ctx, msg, handlerErr)
	})
}

// ack Acks msg on a settle context and reports any failure.
func (p *Pool[T]) ack(ctx context.Context, msg Message) {
	settleCtx, cancel := p.settleContext(ctx)
	defer cancel()
	if err := p.broker.Ack(settleCtx, msg); err != nil {
		p.reportSettleError(ctx, SettleOpAck, msg, err)
	}
}

// nack Nacks msg on a settle context and reports any failure.
func (p *Pool[T]) nack(ctx context.Context, msg Message, cause error) {
	settleCtx, cancel := p.settleContext(ctx)
	defer cancel()
	if err := p.broker.Nack(settleCtx, msg, cause); err != nil {
		p.reportSettleError(ctx, SettleOpNack, msg, err)
	}
}

// settleContext returns a context for one Ack or Nack: ctx's values
// (e.g. an otelmw span) without its cancellation, bounded by the
// configured settle timeout.
func (p *Pool[T]) settleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), p.cfg.settleTimeout)
}

func (p *Pool[T]) reportSettleError(ctx context.Context, op string, msg Message, err error) {
	if p.cfg.onSettleError != nil {
		p.cfg.onSettleError(context.WithoutCancel(ctx), op, msg, err)
	}
}
