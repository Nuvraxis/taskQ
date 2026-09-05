package pgbroker

import "time"

// config holds pgbroker's tunables. Unexported — configure only through
// the functional options below, per the project's "no config structs
// passed positionally" convention.
type config struct {
	leaseDuration time.Duration
	pollInterval  time.Duration
	notifyChannel string
}

func defaultConfig() config {
	return config{
		leaseDuration: 30 * time.Second,
		pollInterval:  200 * time.Millisecond,
		notifyChannel: "taskq_new_message",
	}
}

// Option configures a Broker at construction time.
type Option func(*config)

// WithLeaseDuration sets how long a Dequeue'd message stays invisible to
// other consumers before its lease expires and it becomes available for
// redelivery — pgbroker's equivalent of a visibility timeout. Default 30s.
// Should comfortably exceed your Handler's expected worst-case runtime;
// too short and a still-in-flight message gets redelivered to a second
// worker.
func WithLeaseDuration(d time.Duration) Option {
	return func(c *config) { c.leaseDuration = d }
}

// WithPollInterval bounds how long Dequeue waits on LISTEN/NOTIFY before
// re-checking the table itself. NOTIFY makes the common case near-
// instant; this is the fallback for a missed or never-delivered
// notification (e.g. a lease expiring with no new Enqueue to trigger a
// NOTIFY) — so the ceiling on how stale a wakeup can be. Default 200ms.
func WithPollInterval(d time.Duration) Option {
	return func(c *config) { c.pollInterval = d }
}

// WithNotifyChannel sets the Postgres NOTIFY channel name pgbroker uses.
// Unlike Redis keys (see redisbroker.WithKeyPrefix), a channel name is
// shared cluster-wide within a database, not scoped by table — set this
// if multiple independent taskQ deployments share one Postgres instance
// and you want to avoid one waking the other's idle consumers
// unnecessarily. Default "taskq_new_message".
func WithNotifyChannel(name string) Option {
	return func(c *config) { c.notifyChannel = name }
}
