-- pgbroker/queries.sql
--
-- Full query set for pgbroker. Grouped in three tiers:
--   1. Broker interface ops — Enqueue/Dequeue/Ack/Nack. These are the only
--      ones broker.go calls to satisfy taskq.Broker; everything else here
--      is optional tooling around it.
--   2. Notify — the LISTEN/NOTIFY wake-up used by blocking Dequeue.
--   3. Operational — queue depth, inspection, purge. Not part of the
--      Broker contract; useful for admin scripts, debugging, and tests
--      (e.g. taskqtest cleanup between subtests).

-- =============================================================================
-- 1. Broker interface ops
-- =============================================================================

-- name: Enqueue :exec
INSERT INTO taskq_messages (id, queue, payload, attempts, max_retry, enqueued_at)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: DequeueMessage :one
-- Finds the oldest available row for queue (unleased, or its lease
-- expired), then atomically claims it by stamping locked_until and a
-- fresh receipt_handle in the same statement. FOR UPDATE SKIP LOCKED lets
-- concurrent Dequeue calls on other rows proceed instead of blocking on
-- this one; the candidate CTE, not the outer UPDATE, is what SKIP LOCKED
-- applies to.
--
-- Named args deliberately don't reuse the column names they populate
-- (target_queue vs. queue, new_locked_until vs. locked_until, …) — sqlc's
-- parser treats an sqlc.arg() name that collides with an in-scope column
-- as an ambiguous column reference, not a parameter. Columns inside the
-- CTE are fully qualified for the same reason: once the outer statement is
-- UPDATE ... FROM candidate, sqlc's analyzer treats bare column names in
-- the CTE as ambiguous too.
WITH candidate AS (
    SELECT taskq_messages.sort_key
    FROM taskq_messages
    WHERE taskq_messages.queue = sqlc.arg(target_queue)
      AND (taskq_messages.locked_until IS NULL OR taskq_messages.locked_until < now())
    ORDER BY taskq_messages.sort_key
        FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE taskq_messages m
SET locked_until = sqlc.arg(new_locked_until),
    receipt_handle = sqlc.arg(new_receipt_handle)
FROM candidate
WHERE m.sort_key = candidate.sort_key
RETURNING m.id, m.queue, m.payload, m.attempts, m.max_retry, m.enqueued_at, m.receipt_handle;

-- name: AckMessage :execrows
-- Removes the message this specific delivery refers to. Matching on both
-- id and receipt_handle means a stale Ack from an expired, already-
-- redelivered lease can't delete the row out from under whoever holds the
-- current receipt — it deletes zero rows instead, and the caller can tell
-- from the returned count.
DELETE FROM taskq_messages
WHERE id = sqlc.arg(target_id) AND receipt_handle = sqlc.arg(target_receipt_handle);

-- name: NackDelete :execrows
-- Same targeting as AckMessage. Nack is delete-then-reinsert (see
-- NackReinsert) rather than an in-place UPDATE, because sort_key must
-- advance to move the message to the back of the queue, and
-- GENERATED ALWAYS AS IDENTITY only assigns a fresh value on INSERT. Run
-- in the same transaction as NackReinsert.
DELETE FROM taskq_messages
WHERE id = sqlc.arg(target_id) AND receipt_handle = sqlc.arg(target_receipt_handle);

-- name: NackReinsert :exec
-- Re-inserts the message with the caller-supplied Attempts (and every
-- other field) persisted verbatim, per Broker.Nack's contract — pgbroker
-- does not compute its own retry count. locked_until/receipt_handle are
-- left NULL (unclaimed), and the fresh sort_key this INSERT generates is
-- what puts it behind every message already waiting.
INSERT INTO taskq_messages (id, queue, payload, attempts, max_retry, enqueued_at)
VALUES ($1, $2, $3, $4, $5, $6);

-- =============================================================================
-- 2. Notify — powers blocking Dequeue without a hot-loop
-- =============================================================================

-- name: NotifyQueue :exec
-- Wakes any Dequeue call currently blocked in WaitForNotification for this
-- queue. Called after Enqueue and after NackReinsert commit (same
-- transaction for Nack — Postgres only delivers NOTIFY if the transaction
-- actually commits, so a rolled-back Nack never wakes anyone about a
-- requeue that didn't happen).
SELECT pg_notify('taskq_new_message', sqlc.arg(target_queue)::text);

-- =============================================================================
-- 3. Operational — not part of taskq.Broker; admin/debugging/test tooling
-- =============================================================================

-- name: QueueDepth :one
-- Total messages waiting on queue, claimed or not. Useful for monitoring
-- and for tests asserting a queue drained to zero.
SELECT count(*) FROM taskq_messages WHERE queue = sqlc.arg(target_queue);

-- name: QueueDepthAvailable :one
-- Messages on queue that are actually claimable right now (unleased, or
-- lease expired) — excludes in-flight ones a consumer currently holds.
SELECT count(*)
FROM taskq_messages
WHERE queue = sqlc.arg(target_queue)
  AND (locked_until IS NULL OR locked_until < now());

-- name: ListQueues :many
-- Distinct queue names with at least one message. Handy for an admin
-- dashboard or CLI that doesn't otherwise know what queues exist.
SELECT DISTINCT queue FROM taskq_messages ORDER BY queue;

-- name: PeekMessages :many
-- Read-only inspection of the oldest N messages on queue — no FOR UPDATE,
-- no claiming, no side effects. For debugging ("what's stuck in this
-- queue"), not for consumption; use Dequeue for that.
SELECT id, queue, payload, attempts, max_retry, enqueued_at, locked_until, receipt_handle
FROM taskq_messages
WHERE queue = sqlc.arg(target_queue)
ORDER BY sort_key
LIMIT sqlc.arg(row_limit);

-- name: ReapExpiredLease :execrows
-- Manually clears an expired lease without claiming it, i.e. makes a
-- crashed consumer's message immediately visible again instead of waiting
-- for the next Dequeue to happen to find it. Not required for correctness
-- — Dequeue's own WHERE clause already treats an expired lease as
-- available — this is purely an operational nicety for an admin tool that
-- wants to force it.
UPDATE taskq_messages
SET locked_until = NULL, receipt_handle = NULL
WHERE queue = sqlc.arg(target_queue) AND locked_until < now();

-- name: PurgeQueue :execrows
-- Deletes every message on queue, claimed or not. Destructive — for test
-- cleanup (e.g. taskqtest resetting state between subtests) or a deliberate
-- admin action, never called from Broker's own code paths.
DELETE FROM taskq_messages WHERE queue = sqlc.arg(target_queue);

-- name: PurgeAll :execrows
-- Deletes every message in the table, across all queues. Same caveats as
-- PurgeQueue, one level more destructive — intended for wiping a test
-- database between full test runs, not for any runtime code path.
DELETE FROM taskq_messages;