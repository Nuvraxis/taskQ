package pgbroker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	taskq "github.com/Nuvraxis/taskQ"
	sqlcgen "github.com/Nuvraxis/taskQ/pgbroker/sqlcgen"
)

// Broker implements taskq.Broker on Postgres. The caller owns pool's
// lifecycle — Broker never closes it — matching redisbroker's caller-owned
// *redis.Client (and unlike membroker, which owns its own state). There is
// deliberately no Close method for the same reason.
type Broker struct {
	pool *pgxpool.Pool
	q    *sqlcgen.Queries
	cfg  config
}

// New creates a Broker backed by pool. Run schema.sql against your
// database once before first use — pgbroker has no migration tooling.
func New(pool *pgxpool.Pool, opts ...Option) *Broker {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Broker{pool: pool, q: sqlcgen.New(pool), cfg: cfg}
}

var _ taskq.Broker = (*Broker)(nil)

// Enqueue inserts msg and notifies any consumer blocked in Dequeue for
// this queue. If msg.EnqueuedAt is zero, it's stamped with the current
// time first — queue.go may already set this above the Broker boundary,
// but pgbroker doesn't assume it has.
func (b *Broker) Enqueue(ctx context.Context, msg taskq.Message) error {
	enqueuedAt := msg.EnqueuedAt
	if enqueuedAt.IsZero() {
		enqueuedAt = time.Now().UTC()
	}

	if err := b.q.Enqueue(ctx, sqlcgen.EnqueueParams{
		ID:         msg.ID,
		Queue:      msg.Queue,
		Payload:    msg.Payload,
		Attempts:   int32(msg.Attempts),
		MaxRetry:   int32(msg.MaxRetry),
		EnqueuedAt: pgtype.Timestamptz{Time: enqueuedAt, Valid: true},
	}); err != nil {
		return fmt.Errorf("pgbroker: enqueue: %w", err)
	}

	// Best-effort wake-up. If this fails, or fires with nobody listening
	// yet, Dequeue's poll fallback still finds the message within
	// cfg.pollInterval — this is a latency optimization, not a
	// correctness dependency.
	if err := b.q.NotifyQueue(ctx, msg.Queue); err != nil {
		return fmt.Errorf("pgbroker: notify after enqueue: %w", err)
	}
	return nil
}

// Dequeue returns the oldest available message on queue — one that's
// never been claimed, or whose lease has expired — claiming it with a
// fresh lease and ReceiptHandle. If none is available, it waits on
// LISTEN/NOTIFY (bounded by cfg.pollInterval) and retries, until ctx is
// done.
func (b *Broker) Dequeue(ctx context.Context, queue string) (*taskq.Message, error) {
	for {
		msg, err := b.tryDequeue(ctx, queue)
		if err == nil {
			return msg, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}

		if err := b.waitForActivity(ctx, queue); err != nil {
			return nil, err
		}
		// Loop and re-check the table regardless of why waitForActivity
		// returned nil — a real notification and a poll-interval timeout
		// both just mean "try again".
	}
}

func (b *Broker) tryDequeue(ctx context.Context, queue string) (*taskq.Message, error) {
	receipt := pgtype.Text{String: uuid.NewString()}
	leaseUntil := time.Now().UTC().Add(b.cfg.leaseDuration)

	row, err := b.q.DequeueMessage(ctx, sqlcgen.DequeueMessageParams{
		NewLockedUntil:   pgtype.Timestamptz{Time: leaseUntil, Valid: true},
		NewReceiptHandle: receipt,
		TargetQueue:      queue,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, err // sentinel: caller distinguishes "empty" from real failure
		}
		return nil, fmt.Errorf("pgbroker: dequeue: %w", err)
	}

	return &taskq.Message{
		ID:            row.ID,
		Queue:         row.Queue,
		Payload:       row.Payload,
		Attempts:      int(row.Attempts),
		MaxRetry:      int(row.MaxRetry),
		EnqueuedAt:    row.EnqueuedAt.Time,
		ReceiptHandle: receiptHandleValue(row.ReceiptHandle),
	}, nil
}

// waitForActivity blocks until either a NOTIFY arrives for queue, or
// cfg.pollInterval elapses (the fallback for a missed or never-sent
// notification), or ctx is done. It acquires a dedicated connection for
// the wait — LISTEN is per-session state, so it can't be issued through
// the shared pool — and releases it before returning either way.
//
// LISTEN is issued before the caller's next dequeue attempt (Dequeue calls
// this only after tryDequeue already came back empty, then loops back to
// try again after this returns) — the poll-interval bound is what closes
// the narrow window between "checked, found nothing" and "started
// listening", not perfect ordering.
func (b *Broker) waitForActivity(ctx context.Context, queue string) error {
	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{b.cfg.notifyChannel}.Sanitize()); err != nil {
		return fmt.Errorf("pgbroker: listen: %w", err)
	}
	defer func() {
		// Best-effort cleanup so a released connection doesn't linger in
		// LISTEN state for whichever unrelated caller the pool hands it
		// to next. LISTEN is idempotent, so skipping this on error isn't
		// a correctness bug — just a minor resource wrinkle.
		_, _ = conn.Exec(context.Background(), "UNLISTEN "+pgx.Identifier{b.cfg.notifyChannel}.Sanitize())
	}()

	waitCtx, cancel := context.WithTimeout(ctx, b.cfg.pollInterval)
	defer cancel()

	for {
		notification, err := conn.Conn().WaitForNotification(waitCtx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err() // real cancellation/deadline, not our poll timeout
			}
			return nil // poll interval elapsed — fall through to a re-check
		}
		if notification.Payload == queue {
			return nil
		}
		// A different queue's notification (channel is shared cluster-
		// wide) — keep waiting until waitCtx itself expires.
	}
}

// Ack removes the message this specific delivery refers to. Matching on
// both ID and ReceiptHandle means a stale Ack — one whose lease already
// expired and was redelivered to someone else — deletes nothing rather
// than removing a message a different worker now owns; that's treated as
// a no-op, not an error.
func (b *Broker) Ack(ctx context.Context, msg taskq.Message) error {
	message := msg.ReceiptHandle
	_, err := b.q.AckMessage(ctx, sqlcgen.AckMessageParams{
		TargetID:            msg.ID,
		TargetReceiptHandle: receiptHandleParam(message),
	})
	if err != nil {
		return fmt.Errorf("pgbroker: ack: %w", err)
	}
	return nil
}

// Nack requeues msg for redelivery, persisting msg's fields — notably
// Attempts — verbatim, per Broker.Nack's contract; pgbroker computes no
// retry policy of its own. It's implemented as delete-then-reinsert rather
// than an in-place UPDATE because sort_key (FIFO position) only advances
// on INSERT, and requeuing to the back requires a fresh one. Both steps,
// plus the wake-up NOTIFY, run in one transaction: NOTIFY is delivered by
// Postgres only if its transaction actually commits, so a stale Nack
// (delete matches nothing — see Ack's doc comment) rolls back cleanly
// without waking anyone about a requeue that didn't happen.
func (b *Broker) Nack(ctx context.Context, msg taskq.Message, cause error) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgbroker: nack: begin: %w", err)
	}
	defer func(tx pgx.Tx, ctx context.Context) {
		err := tx.Rollback(ctx)
		if err != nil {
			return
		}
	}(tx, ctx) // no-op once Commit succeeds

	qtx := b.q.WithTx(tx)

	rows, err := qtx.NackDelete(ctx, sqlcgen.NackDeleteParams{
		TargetID:            msg.ID,
		TargetReceiptHandle: receiptHandleParam(msg.ReceiptHandle),
	})
	if err != nil {
		return fmt.Errorf("pgbroker: nack: delete: %w", err)
	}
	if rows == 0 {
		return nil // stale receipt — nothing to requeue, same reasoning as Ack
	}

	if err := qtx.NackReinsert(ctx, sqlcgen.NackReinsertParams{
		ID:         msg.ID,
		Queue:      msg.Queue,
		Payload:    msg.Payload,
		Attempts:   int32(msg.Attempts),
		MaxRetry:   int32(msg.MaxRetry),
		EnqueuedAt: pgtype.Timestamptz{Time: msg.EnqueuedAt, Valid: true},
	}); err != nil {
		return fmt.Errorf("pgbroker: nack: reinsert: %w", err)
	}

	if err := qtx.NotifyQueue(ctx, msg.Queue); err != nil {
		return fmt.Errorf("pgbroker: nack: notify: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgbroker: nack: commit: %w", err)
	}
	return nil
}

// receiptHandleParam converts msg.ReceiptHandle into pgtype.Text for a
// query parameter. An empty string maps to {Valid: false} — never
// {String: "", Valid: true} — because `receipt_handle = NULL` never
// matches in SQL regardless of the column's actual value, so an
// accidentally-empty ReceiptHandle can't accidentally match an unclaimed
// row.
func receiptHandleParam(s string) pgtype.Text {
	if s == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: s, Valid: true}
}

// receiptHandleValue converts a pgtype.Text column value back to a plain
// string, treating NULL (Valid: false) as "".
func receiptHandleValue(t pgtype.Text) string {
	if !t.Valid {
		return ""
	}
	return t.String
}
