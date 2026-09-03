package taskq

import "errors"

// ErrQueueClosed is returned by Broker methods once the broker has been
// closed — Dequeue returns it after any buffered messages drain, Enqueue
// and Nack return it immediately.
var ErrQueueClosed = errors.New("taskQ: queue closed")
