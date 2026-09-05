package redisbroker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	taskq "github.com/Nuvraxis/taskQ"
)

// Broker implements taskq.Broker on Redis Streams via client. Broker does
// not own client's lifecycle — the caller creates and closes it.
type Broker struct {
	client *redis.Client
	cfg    config

	mu     sync.Mutex
	groups map[string]struct{} // stream keys whose consumer group is confirmed to exist
}

var _ taskq.Broker = (*Broker)(nil)

// New creates a Broker backed by client. The consumer group used for
// reading (see WithConsumerGroup) is created lazily, per stream, on first
// Dequeue for that queue.
func New(client *redis.Client, opts ...Option) *Broker {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Broker{
		client: client,
		cfg:    cfg,
		groups: make(map[string]struct{}),
	}
}

func (b *Broker) streamKey(queue string) string {
	return b.cfg.keyPrefix + queue
}

// Enqueue appends msg to the stream for msg.Queue via XADD. The stream is
// created automatically if it doesn't exist yet.
func (b *Broker) Enqueue(ctx context.Context, msg taskq.Message) error {
	key := b.streamKey(msg.Queue)
	if _, err := b.client.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		Values: messageToFields(msg),
	}).Result(); err != nil {
		return fmt.Errorf("redisbroker: enqueue: %w", err)
	}
	return nil
}

// Dequeue reads one message for queue via the configured consumer group,
// creating the group (and stream, if needed) on first use for that queue.
// Each loop iteration first attempts to reclaim one stale pending entry
// (see tryClaimStale) before falling back to XREADGROUP for a genuinely
// new one; it blocks in cfg.blockTimeout increments, re-checking ctx
// between reads, until a message arrives or ctx is done.
func (b *Broker) Dequeue(ctx context.Context, queue string) (*taskq.Message, error) {
	key := b.streamKey(queue)
	if err := b.ensureGroup(ctx, key); err != nil {
		return nil, err
	}

	for {
		claimed, err := b.tryClaimStale(ctx, key)
		if err != nil {
			return nil, err
		}
		if claimed != nil {
			return claimed, nil
		}

		res, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    b.cfg.consumerGroup,
			Consumer: b.cfg.consumerName,
			Streams:  []string{key, ">"},
			Count:    1,
			Block:    b.cfg.blockTimeout,
		}).Result()

		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if errors.Is(err, redis.Nil) {
				continue // BLOCK elapsed with nothing new — loop, re-check ctx (and re-attempt a claim)
			}
			return nil, fmt.Errorf("redisbroker: dequeue: %w", err)
		}

		if len(res) == 0 || len(res[0].Messages) == 0 {
			continue
		}

		entry := res[0].Messages[0]
		msg, err := fieldsToMessage(entry.ID, entry.Values)
		if err != nil {
			return nil, fmt.Errorf("redisbroker: dequeue: decode entry %s: %w", entry.ID, err)
		}
		return &msg, nil
	}
}

// tryClaimStale attempts to reclaim one stale pending entry from key's
// consumer group via XAUTOCLAIM — one that's sat unacknowledged in some
// consumer's Pending Entries List for at least cfg.claimMinIdle, meaning
// that consumer most likely crashed (or hung) before Ack'ing or Nack'ing
// it. It returns nil, nil if nothing was eligible to claim.
//
// It scans from the start of the PEL ("0-0") on every call rather than
// tracking a cursor between calls, and claims at most one entry per call.
// Both are deliberate: Dequeue's own loop already re-runs on a
// cfg.blockTimeout cadence (see the BLOCK/redis.Nil branch above), so each
// iteration gets its own fresh, cheap sweep for free — there's no need for
// a stateful background reaper goroutine, which would also need something
// to stop it. That fits this broker having no Close() and a caller-owned,
// ctx-only lifecycle, the same way pgbroker's lease-expiry check runs
// inline in Dequeue rather than as a separate process. A COUNT of 1 is
// enough because XAUTOCLAIM scans the PEL in ID order (oldest-delivered
// first), so the single oldest pending entry is exactly the one most
// likely to be overdue — and Dequeue only wants one message per call
// anyway.
func (b *Broker) tryClaimStale(ctx context.Context, key string) (*taskq.Message, error) {
	messages, _, err := b.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   key,
		Group:    b.cfg.consumerGroup,
		Consumer: b.cfg.consumerName,
		MinIdle:  b.cfg.claimMinIdle,
		Start:    "0-0",
		Count:    1,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("redisbroker: claim stale: %w", err)
	}
	if len(messages) == 0 {
		return nil, nil
	}

	msg, err := fieldsToMessage(messages[0].ID, messages[0].Values)
	if err != nil {
		return nil, fmt.Errorf("redisbroker: claim stale: decode entry %s: %w", messages[0].ID, err)
	}
	return &msg, nil
}

// Ack acknowledges and removes msg's stream entry. msg.ReceiptHandle must
// be the stream entry ID Dequeue set on the message it returned.
func (b *Broker) Ack(ctx context.Context, msg taskq.Message) error {
	key := b.streamKey(msg.Queue)
	pipe := b.client.Pipeline()
	pipe.XAck(ctx, key, b.cfg.consumerGroup, msg.ReceiptHandle)
	pipe.XDel(ctx, key, msg.ReceiptHandle)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redisbroker: ack: %w", err)
	}
	return nil
}

// Nack requeues msg for redelivery: it writes a new stream entry with
// msg's current field values (Attempts, as already incremented by the
// caller — see taskq.Broker) and then acknowledges the old entry.
func (b *Broker) Nack(ctx context.Context, msg taskq.Message, cause error) error {
	_ = cause

	key := b.streamKey(msg.Queue)
	oldReceipt := msg.ReceiptHandle

	if _, err := b.client.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		Values: messageToFields(msg),
	}).Result(); err != nil {
		return fmt.Errorf("redisbroker: nack: requeue: %w", err)
	}

	pipe := b.client.Pipeline()
	pipe.XAck(ctx, key, b.cfg.consumerGroup, oldReceipt)
	pipe.XDel(ctx, key, oldReceipt)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redisbroker: nack: ack old entry: %w", err)
	}
	return nil
}

func (b *Broker) ensureGroup(ctx context.Context, key string) error {
	b.mu.Lock()
	_, ok := b.groups[key]
	b.mu.Unlock()
	if ok {
		return nil
	}

	// "0" means the group's cursor starts at the beginning of the stream,
	// so entries enqueued before the group existed are still delivered via
	// '>' reads instead of being silently skipped.
	err := b.client.XGroupCreateMkStream(ctx, key, b.cfg.consumerGroup, "0").Err()
	if err != nil && !isBusyGroupErr(err) {
		return fmt.Errorf("redisbroker: create group: %w", err)
	}

	b.mu.Lock()
	b.groups[key] = struct{}{}
	b.mu.Unlock()
	return nil
}

func isBusyGroupErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

func messageToFields(msg taskq.Message) map[string]any {
	return map[string]any{
		"id":         msg.ID,
		"queue":      msg.Queue,
		"payload":    msg.Payload,
		"attempts":   strconv.Itoa(msg.Attempts),
		"maxRetry":   strconv.Itoa(msg.MaxRetry),
		"enqueuedAt": msg.EnqueuedAt.Format(time.RFC3339Nano),
	}
}

func fieldsToMessage(entryID string, values map[string]interface{}) (taskq.Message, error) {
	id, _ := values["id"].(string)
	queue, _ := values["queue"].(string)
	payload, _ := values["payload"].(string)

	attempts, err := strconv.Atoi(fmt.Sprint(values["attempts"]))
	if err != nil {
		return taskq.Message{}, fmt.Errorf("attempts field: %w", err)
	}
	maxRetry, err := strconv.Atoi(fmt.Sprint(values["maxRetry"]))
	if err != nil {
		return taskq.Message{}, fmt.Errorf("maxRetry field: %w", err)
	}
	enqueuedAt, err := time.Parse(time.RFC3339Nano, fmt.Sprint(values["enqueuedAt"]))
	if err != nil {
		return taskq.Message{}, fmt.Errorf("enqueuedAt field: %w", err)
	}

	return taskq.Message{
		ID:            id,
		Queue:         queue,
		Payload:       []byte(payload),
		Attempts:      attempts,
		MaxRetry:      maxRetry,
		EnqueuedAt:    enqueuedAt,
		ReceiptHandle: entryID,
	}, nil
}
