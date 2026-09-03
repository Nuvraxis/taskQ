package taskq

import "time"

type Task[T any] struct {
	ID         string    `json:"id"`
	Queue      string    `json:"queue"`
	Payload    T         `json:"payload"`
	Attempts   int       `json:"attempts"`
	MaxRetry   int       `json:"max_retry"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}
