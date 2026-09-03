package taskq

import "time"

// Task is what Handler[T] actually receives: a Message with Payload
// decoded into T.
type Task[T any] struct {
	ID         string    `json:"id"`
	Queue      string    `json:"queue"`
	Payload    T         `json:"payload"`
	Attempts   int       `json:"attempts"`
	MaxRetry   int       `json:"max_retry"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}
