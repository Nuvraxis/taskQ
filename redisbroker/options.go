// redisbroker/options.go

package redisbroker

import (
	"time"

	"github.com/google/uuid"
)

// Option configures a Broker at construction time.
type Option func(*config)

type config struct {
	keyPrefix     string
	consumerGroup string
	consumerName  string
	blockTimeout  time.Duration
}

func defaultConfig() config {
	return config{
		keyPrefix:     "taskq:",
		consumerGroup: "taskq-workers",
		consumerName:  uuid.NewString(),
		blockTimeout:  5 * time.Second,
	}
}

// WithKeyPrefix overrides the prefix prepended to a queue name to form its
// Redis stream key. Default "taskq:".
func WithKeyPrefix(p string) Option {
	return func(c *config) { c.keyPrefix = p }
}

// WithConsumerGroup overrides the consumer group name used for Dequeue.
// All Broker instances sharing a group compete for the same stream's
// entries rather than each seeing every message. Default "taskq-workers".
func WithConsumerGroup(name string) Option {
	return func(c *config) { c.consumerGroup = name }
}

// WithConsumerName overrides this Broker's consumer name within its group
// — Redis uses it to track which consumer holds which pending entry.
// Default is a generated UUID; set this explicitly if you want stable
// consumer identity across restarts (relevant once XCLAIM-based recovery
// lands in a later phase).
func WithConsumerName(name string) Option {
	return func(c *config) { c.consumerName = name }
}

// WithBlockTimeout overrides how long a single XREADGROUP call blocks
// waiting for a new message before Dequeue loops and re-checks ctx.
// Default 5s.
func WithBlockTimeout(d time.Duration) Option {
	return func(c *config) { c.blockTimeout = d }
}
