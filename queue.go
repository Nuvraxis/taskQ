package taskq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Queue binds a Broker to a queue name and a payload type T.
type Queue[T any] struct {
	broker Broker
	name   string
	cfg    queueConfig
}

// NewQueue creates a Queue bound to broker and the given queue name. opts
// set defaults — such as WithDefaultMaxRetry — applied to every task
// enqueued through it.
func NewQueue[T any](broker Broker, name string, opts ...QueueOption) *Queue[T] {
	cfg := queueConfig{defaultMaxRetry: 0}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Queue[T]{broker: broker, name: name, cfg: cfg}
}

// Enqueue marshals payload to JSON and hands it to the broker.
func (q *Queue[T]) Enqueue(ctx context.Context, payload T, opts ...EnqueueOption) error {
	ec := enqueueConfig{maxRetry: q.cfg.defaultMaxRetry}
	for _, opt := range opts {
		opt(&ec)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("taskQ: marshal payload: %w", err)
	}

	return q.broker.Enqueue(ctx, Message{
		ID:         uuid.NewString(),
		Queue:      q.name,
		Payload:    body,
		MaxRetry:   ec.maxRetry,
		EnqueuedAt: time.Now(),
	})
}

func decode[T any](msg Message) (Task[T], error) {
	var payload T
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return Task[T]{}, fmt.Errorf("taskQ: unmarshal payload: %w", err)
	}
	return Task[T]{
		ID:         msg.ID,
		Queue:      msg.Queue,
		Payload:    payload,
		Attempts:   msg.Attempts,
		MaxRetry:   msg.MaxRetry,
		EnqueuedAt: msg.EnqueuedAt,
	}, nil
}
