package membroker

import (
	"context"
	"sync"

	taskq "github.com/Nuvraxis/taskQ"
)

// Broker is an in-memory taskq.Broker. It's for tests and local dev — no
// persistence, no visibility timeout: Dequeue removes a message immediately,
// so a crash between Dequeue and Ack loses it.
type Broker struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queues map[string][]taskq.Message
	closed bool
}

var _ taskq.Broker = (*Broker)(nil)

func New() *Broker {
	b := &Broker{queues: make(map[string][]taskq.Message)}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *Broker) Enqueue(ctx context.Context, msg taskq.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return taskq.ErrQueueClosed
	}
	b.queues[msg.Queue] = append(b.queues[msg.Queue], msg)
	b.cond.Broadcast()
	return nil
}

// Dequeue blocks until a message is available, ctx is done, or the broker
// is closed with no messages left for this queue.
func (b *Broker) Dequeue(ctx context.Context, queue string) (*taskq.Message, error) {

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			b.mu.Lock()
			b.cond.Broadcast()
			b.mu.Unlock()
		case <-done:
		}
	}()

	b.mu.Lock()
	defer b.mu.Unlock()
	for {
		if q := b.queues[queue]; len(q) > 0 {
			msg := &q[0]
			b.queues[queue] = q[1:]
			return msg, nil
		}
		if b.closed {
			return &taskq.Message{}, taskq.ErrQueueClosed
		}
		if err := ctx.Err(); err != nil {
			return &taskq.Message{}, err
		}
		b.cond.Wait()
	}
}

// Ack is a no-op: the message was already removed from the queue at
// Dequeue time.
func (b *Broker) Ack(ctx context.Context, msg taskq.Message) error {
	return nil
}

// Nack requeues msg unchanged at the back of its queue.
func (b *Broker) Nack(ctx context.Context, msg taskq.Message, cause error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return taskq.ErrQueueClosed
	}

	b.queues[msg.Queue] = append(b.queues[msg.Queue], msg)
	b.cond.Broadcast()
	return nil
}

// Close stops the broker. Pending and future Dequeue calls on empty queues
// return ErrQueueClosed; queues with buffered messages still drain them
// first.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.cond.Broadcast()
}
