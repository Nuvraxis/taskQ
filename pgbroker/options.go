package pgbroker

import "time"

// config holds pgbroker's tunables. Unexported — configure only through
// the functional options below, per the project's "no config structs
// passed positionally" convention.
type config struct {
	leaseDuration time.Duration
	pollInterval  time.Duration
	notifyChannel string
	queuePrefix   string
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
// NOTIFY), or for a LISTEN/NOTIFY setup that never reaches the database at
// all (see WithNotifyChannel's PgBouncer note) — so it's the ceiling on
// how stale a wakeup can be. Default 200ms.
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

// WithQueuePrefix namespaces every queue name this Broker touches with
// prefix at the storage layer — all rows it writes carry prefix+queue in
// the queue column, and Dequeue only ever looks for that same prefixed
// value. A Message.Queue returned to the caller is still the plain,
// unprefixed name passed in; the prefix is purely a storage-layer
// namespace, invisible above the Broker boundary — the same role
// redisbroker.WithKeyPrefix plays for stream keys.
//
// pgbroker needs this where redisbroker doesn't: Redis keys are already
// isolated per queue by construction (one stream key each), so a prefix
// there just avoids colliding with unrelated keys on a shared instance.
// Every pgbroker queue instead lives in the same taskq_messages table, so
// two Broker instances using bare queue names on one Postgres — e.g. two
// taskqtest subtests both enqueuing to "roundtrip" — collide outright
// without something to separate them. Chiefly useful for tests against a
// shared database; most production deployments have one dedicated
// database and don't need it.
func WithQueuePrefix(prefix string) Option {
	return func(c *config) { c.queuePrefix = prefix }
}
