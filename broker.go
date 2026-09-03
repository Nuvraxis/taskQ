package taskq

import "context"

type Broker interface {
	Enqueue(ctx context.Context, msg Message) error
	Dequeue(ctx context.Context, queue string) (*Message, error)
	Ack(ctx context.Context, msg Message) error
	Nack(ctx context.Context, msg Message, cause error) error
}
