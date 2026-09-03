package taskq

import "time"

type Message struct {
	ID         string    `json:"id"`
	Queue      string    `json:"queue"`
	Payload    []byte    `json:"payload"`
	Attempts   int       `json:"attempts"`
	MaxRetry   int       `json:"max_retry"`
	EnqueuedAt time.Time `json:"enqueued_at"`

	// Ack/Nack. membroker leaves it empty; redisbroker/pgbroker will use it
	ReceiptHandle string `json:"receipt_handle"`
}
