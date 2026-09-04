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
// tasks to handler. opts configure concurrency and backoff — see
// WithConcurrency and WithBackoffStrategy. The default is a single worker
// with an ExponentialBackoff (1s base, 30s max, factor 2, jittered).
func NewPool[T any](broker Broker, queue string, handler Handler[T], opts ...PoolOption) *Pool[T] {
	cfg := poolConfig{
		concurrency: 1,
		backoff: ExponentialBackoff{
			Base:   time.Second,
			Max:    30 * time.Second,
			Factor: 2,
			Jitter: true,
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Pool[T]{broker: broker, queue: queue, handler: handler, cfg: cfg}
}

// Run starts cfg.concurrency worker goroutines and blocks until ctx is
// canceled or the broker reports it's closed, then waits for every
// worker's in-flight Handler call to return before returning itself. A
// Handler that ignores ctx can therefore block Run's return indefinitely —
// the same caveat as net/http's graceful shutdown.
//
// Retry delays use an async timer (see BackoffStrategy) rather than
// blocking a worker: a failed task's Nack is scheduled with time.AfterFunc
// and the worker resumes dequeuing immediately. Run does NOT wait for
// scheduled retries to fire — only for handlers already running when
// shutdown begins. A scheduled Nack uses context.Background(), so it still
// fires — and any error it returns is silently dropped — even after Run
// has returned. Observing that error is left to Phase 5 middleware.
//
// Run returns nil on expected shutdown (ctx canceled or the broker
// closed). Any other Dequeue error is returned as-is.
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

// handle decodes msg, runs the handler, and either Acks (success, or a
// malformed payload that retrying won't fix) or schedules a backoff Nack
// with the incremented Attempts count. Ack/Nack errors are currently
// swallowed — there's no observability hook yet; that's Phase 5.
func (p *Pool[T]) handle(ctx context.Context, msg Message) {
	task, decodeErr := decode[T](msg)
	if decodeErr != nil {
		_ = p.broker.Ack(ctx, msg)
		return
	}

	handlerErr := p.handler(ctx, task)
	if handlerErr == nil {
		_ = p.broker.Ack(ctx, msg)
		return
	}

	msg.Attempts++
	if msg.Attempts > msg.MaxRetry {
		_ = p.broker.Ack(ctx, msg) // exhausted retries — done, remove it
		return
	}

	delay := p.cfg.backoff.Next(msg.Attempts)
	time.AfterFunc(delay, func() {
		_ = p.broker.Nack(context.Background(), msg, handlerErr)
	})
}
