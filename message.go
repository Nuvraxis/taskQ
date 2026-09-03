package taskq

import "time"

// Message is the broker-level envelope. Brokers only ever see this — they
// don't know about T. Task[T] is decoded from Message above the Broker
// boundary.
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
