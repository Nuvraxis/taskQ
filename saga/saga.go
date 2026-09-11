package saga

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	taskq "github.com/Nuvraxis/taskQ"
)

// Saga Saga[S] drives a durable, multi-step workflow with automatic
// compensation over a series of Step[S]. Construct one with New, start a
// run with Start, and hand Handler() to a Pool[Envelope[S]] to actually
// execute steps.
type Saga[S any] struct {
	queue *taskq.Queue[Envelope[S]]
	steps []Step[S]
	cfg   sagaConfig[S]
}

// New constructs a Saga over queueName, backed by broker, running steps
// in the given order. It panics if steps is empty — a saga with nothing
// to do is a programmer error to catch at construction, not a runtime
// condition to handle gracefully.
func New[S any](broker taskq.Broker, queueName string, steps []Step[S], opts ...Option[S]) *Saga[S] {
	if len(steps) == 0 {
		panic("saga: New requires at least one step")
	}
	cfg := defaultConfig[S]()
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Saga[S]{
		queue: taskq.NewQueue[Envelope[S]](broker, queueName),
		steps: steps,
		cfg:   cfg,
	}
}

// Start begins a new saga run: it assigns a fresh SagaID, builds the
// initial Envelope at StepIndex 0 pointing Forward, and enqueues it.
// Start only durably records that the saga should begin — the actual
// steps[0].Do call happens later, whenever a Pool[Envelope[S]] running
// Handler() dequeues that envelope.
func (s *Saga[S]) Start(ctx context.Context, initialState S) (sagaID string, err error) {
	sagaID = uuid.NewString()
	env := Envelope[S]{
		SagaID:    sagaID,
		State:     initialState,
		StepIndex: 0,
		Direction: Forward,
	}
	if err := s.enqueueHop(ctx, env); err != nil {
		return "", fmt.Errorf("saga: start: %w", err)
	}
	return sagaID, nil
}

// Handler returns the taskq.Handler[Envelope[S]] that drives this saga.
// Hand it to a Pool[Envelope[S]] constructed on the same queue name Saga
// was built with:
//
//	pool := taskq.NewPool(broker, queueName, mySaga.Handler())
//	go pool.Run(ctx)
//
// Each call processes exactly one hop — one step, one direction — not
// the whole saga: on success it enqueues the next hop and returns, it
// never loops through remaining steps in-process. That's what makes a
// saga durable across a worker restart: which hop runs next always lives
// in the queue, never only in memory.
func (s *Saga[S]) Handler() taskq.Handler[Envelope[S]] {
	return func(ctx context.Context, task taskq.Task[Envelope[S]]) error {
		env := task.Payload

		if env.StepIndex < 0 || env.StepIndex >= len(s.steps) {
			// Malformed/corrupted envelope, not a transient failure —
			// retrying won't fix an out-of-range index. Surface it rather
			// than retrying forever or letting a bad index panic a
			// worker goroutine.
			s.cfg.onFailed(ctx, env.SagaID, env.State, "",
				fmt.Errorf("saga: step index %d out of range (%d steps)", env.StepIndex, len(s.steps)))
			return nil
		}
		step := s.steps[env.StepIndex]
		ctx = withHop(ctx, Hop{
			SagaID:    env.SagaID,
			StepIndex: env.StepIndex,
			Direction: env.Direction,
		})
		switch env.Direction {
		case Forward:
			return s.handleForward(ctx, task, env, step)
		case Compensating:
			return s.handleCompensating(ctx, task, env, step)
		default:
			s.cfg.onFailed(ctx, env.SagaID, env.State, step.Name,
				fmt.Errorf("saga: unknown direction %v", env.Direction))
			return nil
		}
	}
}

func (s *Saga[S]) handleForward(ctx context.Context, task taskq.Task[Envelope[S]], env Envelope[S], step Step[S]) error {
	err := step.Do(ctx, &env.State)
	if err == nil {
		if env.StepIndex == len(s.steps)-1 {
			s.cfg.onComplete(ctx, env.SagaID, env.State)
			return nil
		}
		next := env
		next.StepIndex++
		return s.enqueueHop(ctx, next)
	}

	// step.Do failed. This is retried exactly like any ordinary
	// Handler[T] failure, subject to the same Attempts/MaxRetry budget —
	// no saga-specific retry logic here.
	if task.Attempts < task.MaxRetry {
		return err
	}

	// Retries exhausted. This step never succeeded, so there's nothing of
	// its own to compensate — pivot to unwinding whatever came before it
	// instead. Returning nil here (rather than err) is deliberate: it
	// stops Pool from applying its own give-up behavior (silently Acking
	// the task and dropping it) before the saga has had a chance to
	// schedule its rollback.
	if env.StepIndex == 0 {
		// Nothing succeeded before this one either — the saga failed at
		// its very first step, with nothing to compensate. Terminal.
		s.cfg.onFailed(ctx, env.SagaID, env.State, step.Name, err)
		return nil
	}

	prev := env
	prev.StepIndex--
	prev.Direction = Compensating
	prev.FailedStep = step.Name
	prev.FailureReason = err.Error()
	if enqueueErr := s.enqueueHop(ctx, prev); enqueueErr != nil {
		// Couldn't even schedule the rollback. Returning this error lets
		// Pool retry this forward hop again — step.Do will fail the same
		// way again, and this pivot will be re-attempted — rather than
		// silently losing the saga's need to roll back.
		return fmt.Errorf("saga: pivot to compensation: %w", enqueueErr)
	}
	return nil
}

func (s *Saga[S]) handleCompensating(ctx context.Context, task taskq.Task[Envelope[S]], env Envelope[S], step Step[S]) error {
	var err error
	if step.Compensate != nil {
		err = step.Compensate(ctx, &env.State)
	}
	// A nil Compensate is treated as an automatic no-op success — a step
	// with nothing to undo (a pure read or validation step) shouldn't
	// block or fail the rollback of everything around it.

	if err == nil {
		if env.StepIndex == 0 {
			// Every step that had succeeded has now been undone — fully
			// rolled back. Report the step and error that originally
			// caused the rollback (carried in the envelope since the
			// pivot), not step.Name here — step.Name is just whichever
			// step's Compensate happened to finish last, which for any
			// saga longer than one step is uninformative: it would be
			// the same value regardless of what actually failed.
			var cause error
			if env.FailureReason != "" {
				cause = errors.New(env.FailureReason)
			}
			s.cfg.onFailed(ctx, env.SagaID, env.State, env.FailedStep, cause)
			return nil
		}
		prev := env
		prev.StepIndex--
		return s.enqueueHop(ctx, prev)
	}

	if task.Attempts < task.MaxRetry {
		return err
	}

	// Compensation itself has now exhausted its retries. This is the one
	// outcome the saga cannot resolve automatically: resuming forward
	// isn't correct (the original failure that triggered rollback still
	// holds), and skipping ahead to compensate earlier steps risks
	// leaving *this* step's side effect permanently in place with no
	// record beyond this callback. Surface it loudly rather than
	// silently Acking it away — this is the case that should page
	// someone, not just log.
	s.cfg.onCompensationFailed(ctx, env.SagaID, env.State, step.Name, err)
	return nil
}

func (s *Saga[S]) enqueueHop(ctx context.Context, env Envelope[S]) error {
	return s.queue.Enqueue(ctx, env, taskq.WithMaxRetry(s.cfg.maxRetry))
}

// sagaConfig holds Saga's tunables and hooks. Unexported — configure only
// through the functional options below.
type sagaConfig[S any] struct {
	maxRetry             int
	onComplete           func(ctx context.Context, sagaID string, state S)
	onFailed             func(ctx context.Context, sagaID string, state S, failedStep string, cause error)
	onCompensationFailed func(ctx context.Context, sagaID string, state S, step string, cause error)
}

func defaultConfig[S any]() sagaConfig[S] {
	return sagaConfig[S]{
		maxRetry:             3,
		onComplete:           func(ctx context.Context, sagaID string, state S) {},
		onFailed:             func(ctx context.Context, sagaID string, state S, failedStep string, cause error) {},
		onCompensationFailed: func(ctx context.Context, sagaID string, state S, step string, cause error) {},
	}
}

// Option configures a Saga[S] at construction time.
type Option[S any] func(*sagaConfig[S])

// WithMaxRetry sets how many redeliveries are allowed for each individual
// hop — one Do or one Compensate call — before that hop's retries are
// considered exhausted and the saga pivots (on a Do failure, to
// compensation) or escalates (on a Compensate failure, via
// OnCompensationFailed). Same semantics as taskq.WithMaxRetry:
// MaxRetry+1 total attempts per hop. Default 3.
func WithMaxRetry[S any](n int) Option[S] {
	return func(c *sagaConfig[S]) { c.maxRetry = n }
}

// WithOnComplete registers a callback fired once, when every step's Do
// has succeeded — the saga finished normally.
func WithOnComplete[S any](fn func(ctx context.Context, sagaID string, state S)) Option[S] {
	return func(c *sagaConfig[S]) { c.onComplete = fn }
}

// WithOnFailed registers a callback fired when the saga does not
// complete successfully. cause is the error from whichever step's Do
// ultimately failed, and failedStep names that step — regardless of how
// many earlier steps had to be compensated as a result, and regardless of
// whether any compensation was needed at all (a step-0 failure has
// nothing to compensate, so this fires immediately with that step's own
// error).
func WithOnFailed[S any](fn func(ctx context.Context, sagaID string, state S, failedStep string, cause error)) Option[S] {
	return func(c *sagaConfig[S]) { c.onFailed = fn }
}

// WithOnCompensationFailed registers a callback fired when a step's own
// Compensate exhausts its retries. This is the one outcome the saga
// cannot resolve automatically — state may be left partially rolled back
// with no further automatic recovery — so unlike WithOnFailed, this
// firing should generally page a human rather than just log.
func WithOnCompensationFailed[S any](fn func(ctx context.Context, sagaID string, state S, step string, cause error)) Option[S] {
	return func(c *sagaConfig[S]) { c.onCompensationFailed = fn }
}
