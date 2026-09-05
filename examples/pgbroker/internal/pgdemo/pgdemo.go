// Package pgdemo holds the setup the pgbroker examples share: connecting to
// Postgres and applying pgbroker's schema. It's example-only glue, not part
// of taskQ.
package pgdemo

import (
	"context"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaDDL mirrors pgbroker/schema.sql (the source of truth). It's all
// CREATE ... IF NOT EXISTS, so applying it on every run is safe. pgbroker has
// no migration tooling yet, so the application is responsible for creating the
// table before first use.
const schemaDDL = `
CREATE TABLE IF NOT EXISTS taskq_messages (
    sort_key       BIGINT GENERATED ALWAYS AS IDENTITY,
    id             TEXT NOT NULL,
    queue          TEXT NOT NULL,
    payload        BYTEA NOT NULL,
    attempts       INT NOT NULL DEFAULT 0,
    max_retry      INT NOT NULL DEFAULT 0,
    enqueued_at    TIMESTAMPTZ NOT NULL,
    locked_until   TIMESTAMPTZ,
    receipt_handle TEXT,
    PRIMARY KEY (sort_key)
);
CREATE INDEX IF NOT EXISTS taskq_messages_dequeue_idx
    ON taskq_messages (queue, locked_until, sort_key);
`

// Connect builds a pool from TASKQ_POSTGRES_DSN (default a local dev DSN),
// pings it, and applies the schema. It returns ok=false with actionable
// guidance when Postgres is unreachable, so an example can skip cleanly
// instead of crashing. The caller owns the returned pool and must Close it.
func Connect(ctx context.Context) (*pgxpool.Pool, bool) {
	dsn := os.Getenv("TASKQ_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(connectCtx, dsn)
	if err != nil {
		guidance(dsn, err)
		return nil, false
	}
	if err := pool.Ping(connectCtx); err != nil {
		pool.Close()
		guidance(dsn, err)
		return nil, false
	}
	// schema.sql is all CREATE ... IF NOT EXISTS, so applying it every run is
	// safe and keeps the example self-contained (no manual setup step).
	if _, err := pool.Exec(connectCtx, schemaDDL); err != nil {
		pool.Close()
		log.Printf("applying schema: %v", err)
		return nil, false
	}

	log.Printf("connected to postgres at %s", redact(dsn))
	return pool, true
}

func guidance(dsn string, err error) {
	log.Printf("postgres not reachable at %s: %v", redact(dsn), err)
	log.Printf("start one with:  docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16")
	log.Printf("then set TASKQ_POSTGRES_DSN if it differs from the default")
}

// redact masks the password in a DSN so it doesn't end up in logs.
func redact(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return dsn
	}
	if _, hasPassword := u.User.Password(); hasPassword {
		u.User = url.UserPassword(u.User.Username(), "redacted")
	}
	return u.String()
}
