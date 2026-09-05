package pgbroker

import (
	"context"
	"fmt"

	taskq "github.com/Nuvraxis/taskQ"
	sqlcgen "github.com/Nuvraxis/taskQ/pgbroker/sqlcgen"
)

// The methods in this file are pgbroker-specific tooling — monitoring,
// debugging, and cleanup — not part of taskq.Broker. Nothing in taskq,
// Pool, or taskqtest calls them; they exist for admin scripts, dashboards,
// and tests that need to reach past the Broker interface.

// QueueDepth returns the total number of messages waiting on queue,
// claimed or not.
func (b *Broker) QueueDepth(ctx context.Context, queue string) (int64, error) {
	n, err := b.q.QueueDepth(ctx, b.cfg.queuePrefix+queue)
	if err != nil {
		return 0, fmt.Errorf("pgbroker: queue depth: %w", err)
	}
	return n, nil
}

// QueueDepthAvailable returns the number of messages on queue that are
// actually claimable right now — unleased, or their lease expired —
// excluding ones a consumer currently holds.
func (b *Broker) QueueDepthAvailable(ctx context.Context, queue string) (int64, error) {
	n, err := b.q.QueueDepthAvailable(ctx, b.cfg.queuePrefix+queue)
	if err != nil {
		return 0, fmt.Errorf("pgbroker: queue depth available: %w", err)
	}
	return n, nil
}

// PeekMessages returns the oldest limit messages on queue without
// claiming them — no lease is taken, no receipt is issued, and repeated
// calls can return the same messages. For inspecting what's stuck in a
// queue; use Dequeue to actually consume.
func (b *Broker) PeekMessages(ctx context.Context, queue string, limit int) ([]taskq.Message, error) {
	rows, err := b.q.PeekMessages(ctx, sqlcgen.PeekMessagesParams{
		TargetQueue: b.cfg.queuePrefix + queue,
		RowLimit:    int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("pgbroker: peek messages: %w", err)
	}

	msgs := make([]taskq.Message, len(rows))
	for i, row := range rows {
		msgs[i] = taskq.Message{
			ID:            row.ID,
			Queue:         queue, // logical name, not row.Queue (carries the prefix)
			Payload:       row.Payload,
			Attempts:      int(row.Attempts),
			MaxRetry:      int(row.MaxRetry),
			EnqueuedAt:    row.EnqueuedAt.Time,
			ReceiptHandle: receiptHandleValue(row.ReceiptHandle),
		}
	}
	return msgs, nil
}

// ReapExpiredLease manually clears queue's expired leases, making any
// crashed consumer's message immediately visible again instead of waiting
// for the next Dequeue to happen to find it. Not required for
// correctness — Dequeue's own WHERE clause already treats an expired
// lease as available — this exists for an admin tool that wants to force
// it, e.g. to inspect via PeekMessages what's about to become claimable.
func (b *Broker) ReapExpiredLease(ctx context.Context, queue string) (int64, error) {
	n, err := b.q.ReapExpiredLease(ctx, b.cfg.queuePrefix+queue)
	if err != nil {
		return 0, fmt.Errorf("pgbroker: reap expired lease: %w", err)
	}
	return n, nil
}

// PurgeQueue deletes every message on queue, claimed or not. Destructive
// — for test cleanup or a deliberate admin action, never called from
// Broker's own code paths.
func (b *Broker) PurgeQueue(ctx context.Context, queue string) (int64, error) {
	n, err := b.q.PurgeQueue(ctx, b.cfg.queuePrefix+queue)
	if err != nil {
		return 0, fmt.Errorf("pgbroker: purge queue: %w", err)
	}
	return n, nil
}

// ListQueues returns the distinct queue names with at least one message
// in the table.
//
// It refuses to run on a Broker constructed with WithQueuePrefix, rather
// than silently listing every queue in the table regardless of prefix:
// the prefix exists specifically to give a Broker instance its own
// namespace (e.g. one per taskqtest subtest sharing a single Postgres),
// and a table-wide list would leak queue names — including their raw,
// unstripped prefixes — belonging to other Broker instances entirely.
// Construct a Broker with no WithQueuePrefix to use this, or query
// taskq_messages directly.
func (b *Broker) ListQueues(ctx context.Context) ([]string, error) {
	if b.cfg.queuePrefix != "" {
		return nil, fmt.Errorf("pgbroker: ListQueues is table-wide and not queue-prefix-aware; construct a Broker with no WithQueuePrefix to use it")
	}
	queues, err := b.q.ListQueues(ctx)
	if err != nil {
		return nil, fmt.Errorf("pgbroker: list queues: %w", err)
	}
	return queues, nil
}

// PurgeAll deletes every message in the table, across all queues. Same
// prefix guard as ListQueues, for the same reason — table-wide on a
// prefixed Broker would delete rows outside that Broker's own namespace,
// which is exactly the collision WithQueuePrefix exists to prevent. Use
// PurgeQueue for a single queue, or construct a Broker with no
// WithQueuePrefix.
func (b *Broker) PurgeAll(ctx context.Context) (int64, error) {
	if b.cfg.queuePrefix != "" {
		return 0, fmt.Errorf("pgbroker: PurgeAll is table-wide and not queue-prefix-aware; use PurgeQueue for a single queue, or construct a Broker with no WithQueuePrefix")
	}
	n, err := b.q.PurgeAll(ctx)
	if err != nil {
		return 0, fmt.Errorf("pgbroker: purge all: %w", err)
	}
	return n, nil
}
