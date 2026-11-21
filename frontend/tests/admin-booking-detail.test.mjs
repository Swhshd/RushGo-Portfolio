import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

const { parseAdminBookingDetail, adminBookingSignals } = loadTypeScript("lib/admin-booking-detail.ts");

const BOOKING = "00000000-0000-4000-8000-000000000001";
const USER = "00000000-0000-4000-8000-000000000002";
const EVENT = "00000000-0000-4000-8000-000000000003";
const TIER = "00000000-0000-4000-8000-000000000004";
const ITEM = "00000000-0000-4000-8000-000000000005";
const SEAT = "00000000-0000-4000-8000-000000000006";
const TICKET = "00000000-0000-4000-8000-000000000007";
const REQUEST = "00000000-0000-4000-8000-000000000008";
const NOW = "2026-11-10T12:00:00Z";

function detail(extra = {}) {
  return {
    booking_id: BOOKING, user_id: USER, event_id: EVENT, booking_status: "CONFIRMED",
    total_amount_cents: 1000, currency: "USD", booking_mode: "pessimistic",
    lifecycle_generation: 1, lifecycle_phase: "TERMINAL", created_at: NOW, observed_at: NOW,
    items: [{ id: ITEM, ticket_tier_id: TIER, tier_name: "Floor", quantity: 1, unit_price_cents: 1000, seat_ids: [SEAT] }],
    tickets: [{ ticket_id: TICKET, status: "ISSUED", seat_id: SEAT, ticket_tier_id: TIER, issued_at: NOW, checked_in_at: null, revoked_at: null }],
    receipt: { outcome: "issued", reason_code: "eligible", ticket_count: 1, receipt_count: 1, observed_at: NOW, created_at: NOW },
    refund: null, source_service: "booking", coverage: "booking_owned_exact",
    ...extra,
  };
}

function refund(extra = {}) {
  return {
    request_id: REQUEST, status: "REQUESTED", reason: "Changed plans", decision_reason: "",
    refund_decision_id: "", amount_cents: 1000, currency: "USD", requested_at: NOW, decided_at: null,
    payment_refund_state: "unknown", ticket_revocation_state: "NOT_STARTED", can_withdraw: true,
    approval_enabled: true, ...extra,
  };
}

test("admin booking detail parser keeps exact safe fields and rejects identity/currency drift", () => {
  const parsed = parseAdminBookingDetail(detail(), BOOKING);
  assert.equal(parsed.booking_id, BOOKING);
  assert.equal(parsed.currency, "USD");
  assert.equal(parsed.tickets[0].ticket_id, TICKET);
  assert.equal(parsed.items[0].seat_ids[0], SEAT);

  for (const invalid of [
    detail({ booking_id: EVENT }),
    detail({ currency: "EUR" }),
    detail({ booking_status: "" }),
    detail({ created_at: "not-a-time" }),
    detail({ receipt: { ...detail().receipt, outcome: "future" } }),
    detail({ refund: refund({ currency: "EUR" }) }),
  ]) {
    assert.throws(() => parseAdminBookingDetail(invalid, BOOKING), /Booking detail response was invalid/);
  }
});

test("admin booking signals explain only recorded facts", () => {
  assert.deepEqual(adminBookingSignals(parseAdminBookingDetail(detail({ tickets: [] }), BOOKING)), ["receipt_without_ticket"]);
  assert.deepEqual(adminBookingSignals(parseAdminBookingDetail(detail({
    tickets: [], receipt: { outcome: "denied", reason_code: "not_eligible", ticket_count: 0, receipt_count: 1, observed_at: NOW, created_at: NOW },
  }), BOOKING)), ["issuance_denied"]);
  assert.deepEqual(adminBookingSignals(parseAdminBookingDetail(detail({ tickets: [], receipt: null }), BOOKING)), ["tickets_not_recorded"]);
  assert.deepEqual(adminBookingSignals(parseAdminBookingDetail(detail({ refund: refund() }), BOOKING)), []);
  assert.deepEqual(adminBookingSignals(parseAdminBookingDetail(detail({ booking_status: "PENDING", tickets: [], receipt: null }), BOOKING)), []);
  assert.deepEqual(adminBookingSignals(parseAdminBookingDetail(detail({
    refund: refund({ status: "APPROVED", payment_refund_state: "pending" }),
  }), BOOKING)), ["refund_payment_pending"]);
  assert.deepEqual(adminBookingSignals(parseAdminBookingDetail(detail({
    refund: refund({ status: "NEEDS_ATTENTION", payment_refund_state: "unknown" }),
  }), BOOKING)), ["refund_result_unknown"]);
  assert.deepEqual(adminBookingSignals(parseAdminBookingDetail(detail({
    refund: refund({ status: "APPROVED", payment_refund_state: "failed" }),
  }), BOOKING)), ["refund_result_unknown"]);
  assert.deepEqual(adminBookingSignals(parseAdminBookingDetail(detail({
    refund: refund({ status: "REFUNDED", payment_refund_state: "succeeded", ticket_revocation_state: "PENDING" }),
  }), BOOKING)), ["revocation_pending"]);
  assert.deepEqual(adminBookingSignals(parseAdminBookingDetail(detail({
    refund: refund({ status: "REFUNDED", payment_refund_state: "succeeded", ticket_revocation_state: "NOT_STARTED" }),
  }), BOOKING)), ["revocation_not_synced"]);
});

test("admin booking data never upgrades Booking CONFIRMED into a payment claim", () => {
  const signals = adminBookingSignals(parseAdminBookingDetail(detail({ tickets: [], receipt: null, refund: null }), BOOKING));
  assert.deepEqual(signals, ["tickets_not_recorded"]);
  const text = signals.join(" ").toLowerCase();
  assert.equal(text.includes("paid"), false);
  assert.equal(text.includes("stalled"), false);
});
