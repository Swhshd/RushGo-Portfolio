# Payment financial recovery

Payment PostgreSQL owns the obligation and the worker's operation identity. Stripe
TEST credential/account verification must succeed before managed writers start.
The worker scans on startup and every second, claims at most four rows for 30
seconds, commits, then performs provider calls within ten seconds. No provider
request runs inside a Payment transaction.

Create, cancel and refund use the keys persisted on the original Payment. An
expired lease can cause overlapping provider requests; both use the same key and
immutable target. A claim token/fence and live lease guard local work completion.
Verified late evidence survives a stale claim without clearing a newer lease.

Unknown refunds replay the same full request inside 20 hours. Known refunds are
retrieved and cumulative succeeded amounts are checked against the original full
amount and currency. Pending, failed, canceled and requires-action amounts do not
count toward completion. An HTTP success or a created refund object alone is
`provider_pending`. A partial total retains `attention`. No replacement or partial
refund operation is issued. Cumulative full succeeded evidence resolves the
original obligation, including a full total from existing partial provider objects.

Cancel first retrieves the original intent. When its create response was lost it
correlates by original Payment metadata; absence is uncertainty. It never creates
a replacement intent. Collection discovered during cancel wakes reconciliation
and becomes a full refund obligation for a terminal nonconfirmable Booking.

Transient cancel/refund failures use persisted exponential backoff from one second
up to 256 seconds plus at most 25% jitter. The attempt count resets on the first
cancel/refund claim and excludes routine reconciliation polls. Reaching 32 mutation
attempts, or operation ambiguity reaching 20 hours, escalates to `attention` or
`cancel_attention`. Known provider-pending refunds poll every minute. Trusted new
receipts still wake reconciliation. Attention retains financial responsibility;
it proves neither a completed refund nor permanent noncollection.

The restricted Payment-owned query below excludes frozen parameters, email,
client secrets and raw provider responses. Run it against the intended Payment
database; review account/mode and immutable operation IDs against authenticated
provider evidence. Never delete or rewrite history, generate a fresh operation
key, infer failure from a timeout, or use a manual refund as automatic acceptance
evidence. Escalate ambiguity using the approved controlled resolution process.

```sql
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '2s';
SELECT current_database(), current_user, clock_timestamp() AS observed_at;
SELECT id, booking_id, payment_generation, provider_account, livemode,
       price, currency, payment_intent_id, provider_outcome, provider_status,
       booking_disposition, create_state, cancel_state, reconciliation_state,
       refund_state, refund_amount_cents, refund_currency, refund_provider_id,
       refund_provider_status, refund_requested_at, refund_completed_at,
       operation_kind, claim_fence, lease_until, due_at, attempts
FROM payments
WHERE managed AND (reconciliation_state NOT IN ('sale_verified', 'resolved')
                   OR cancel_state = 'cancel_attention'
                   OR refund_state = 'attention')
ORDER BY COALESCE(refund_requested_at, created_at), id
LIMIT 200;
COMMIT;
```

Metrics use no Booking/Payment/provider IDs: `rushgo.payment.financial.due`,
`attention`, `oldest_age` (seconds), and `attempts` (operation only). Backlog age
includes blocked financial work. A failed summary query is an error, never zero
backlog. External Stripe TEST crash/restart acceptance is recorded in T22.
