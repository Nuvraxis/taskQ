-- pgbroker/schema.sql
--
-- DDL for pgbroker's backing table. Run once per database before using
-- pgbroker.Broker against it — there's no migration tooling yet.
--
-- id/receipt_handle are TEXT, not UUID: Message.ID is already a plain
-- string (assigned via google/uuid.NewString() above the Broker boundary),
-- and a UUID column would require registering pgx's uuid type on every
-- caller's pool. TEXT keeps setup to "hand pgbroker a *pgxpool.Pool" with
-- nothing extra to configure.
--
-- payload is BYTEA, not JSONB: the Broker doesn't know or care that the
-- bytes happen to be JSON right now (Queue[T] does the marshaling above
-- this boundary) — treating it as opaque bytes keeps that layering honest.

CREATE TABLE IF NOT EXISTS taskq_messages (
    -- FIFO order lives here, not in enqueued_at: Nack must be able to move
    -- a message to the back of the queue without touching the caller-
    -- supplied EnqueuedAt (Broker.Nack persists the given Message
    -- verbatim, per broker.go's doc comment) — so ordering needs a column
    -- of its own. A fresh (larger) value is assigned both on initial
    -- Enqueue and again every time Nack re-inserts the message.
                                              sort_key       BIGINT GENERATED ALWAYS AS IDENTITY,

                                              id             TEXT NOT NULL,
                                              queue          TEXT NOT NULL,
                                              payload        BYTEA NOT NULL,
                                              attempts       INT NOT NULL DEFAULT 0,
                                              max_retry      INT NOT NULL DEFAULT 0,
                                              enqueued_at    TIMESTAMPTZ NOT NULL,

    -- Lease / visibility-timeout state. NULL means "available for
    -- Dequeue". SELECT ... FOR UPDATE SKIP LOCKED only skips rows another
    -- *transaction* currently holds — that lock disappears the instant
    -- Dequeue's own short transaction commits, so locked_until is what
    -- actually keeps a checked-out message invisible to other consumers
    -- until its lease expires. A crashed consumer's lease simply times
    -- out; the same WHERE clause that finds fresh messages finds
    -- expired-lease ones too, so redelivery needs no separate sweep.
                                              locked_until   TIMESTAMPTZ,

    -- Identifies one specific delivery (checkout) of this message, same
    -- role as redisbroker's stream entry ID: Ack/Nack must target the
    -- exact delivery instance, so a stale checkout (lease already expired
    -- and redelivered to someone else) can't interfere with the message's
    -- *next* delivery.
                                              receipt_handle TEXT,

                                              PRIMARY KEY (sort_key)
);

-- Every Dequeue scans for the oldest unleased row in a given queue —
-- this index turns that scan into an index-only lookup instead of a
-- full table scan.
CREATE INDEX IF NOT EXISTS taskq_messages_dequeue_idx
    ON taskq_messages (queue, locked_until, sort_key);