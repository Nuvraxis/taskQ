package taskq

import "errors"

var ErrQueueClosed = errors.New("taskQ: queue closed")
