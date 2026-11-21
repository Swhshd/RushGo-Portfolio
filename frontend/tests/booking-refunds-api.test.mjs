import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

const bookingId = "00000000-0000-4000-8000-000000000001";
const eventId = "00000000-0000-4000-8000-000000000002";
const requestId = "00000000-0000-4000-8000-000000000003";
const key = "00000000-0000-4000-8000-000000000004";
function refund(extra = {}) {
  return {
    request_id: requestId, booking_id: bookingId, event_id: eventId, status: "REQUESTED",
    reason: "Travel changed", decision_reason: "", refund_decision_id: "", amount_cents: 1000,
    currency: "USD", requested_at: "2026-11-03T10:00:00Z", decided_at: "",
    payment_refund_state: "", ticket_revocation_state: "", can_withdraw: true,
    approval_enabled: false, eligibility_code: "", ...extra,
  };
}

async function browser(fetcher, run) {
  const saved = { window: globalThis.window, localStorage: globalThis.localStorage, fetch: globalThis.fetch };
  Object.assign(globalThis, { window: {}, localStorage: { getItem: () => null, setItem() {}, removeItem() {} }, fetch: fetcher });
  try { await run(); } finally { Object.assign(globalThis, saved); }
}

test("buyer refund reads and requests are scoped, no-store, idempotent, and strip extra fields", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  const seen = [];
  await browser(async (url, options) => {
    seen.push({ url, options });
    return Response.json({ ...refund(), payment_intent_client_secret: "PRIVATE", provider_payload: { key: "PRIVATE" } });
  }, async () => {
    const current = await apiClient.getBookingRefund(bookingId);
    assert.equal(current.booking_id, bookingId);
    assert.equal(JSON.stringify(current).includes("PRIVATE"), false);
    const submitted = await apiClient.requestBookingRefund(bookingId, "Travel changed", key);
    assert.equal(submitted.request_id, requestId);
  });
  assert.equal(seen[0].url, `/api/bookings/${bookingId}/refund`);
  assert.equal(seen[0].options.method, "GET");
  assert.equal(seen[0].options.cache, "no-store");
  assert.equal(seen[1].url, `/api/bookings/${bookingId}/refund`);
  assert.equal(seen[1].options.method, "POST");
  assert.equal(seen[1].options.cache, "no-store");
  assert.equal(seen[1].options.headers["Idempotency-Key"], key);
  assert.deepEqual(JSON.parse(seen[1].options.body), { reason: "Travel changed" });
});

test("refund approval and approver APIs use event-scoped paths and reject invalid input before network", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  const calls = [];
  await browser(async (url, options) => {
    calls.push({ url, options });
    if (String(url).includes("refund-approvers")) return Response.json({ event_id: eventId, staff_user_id: bookingId, active: true, changed_at: "2026-11-03T10:00:00Z" });
    return Response.json(refund({ status: "REJECTED", can_withdraw: false, decided_at: "2026-11-03T10:02:00Z" }));
  }, async () => {
    await apiClient.decideBookingRefund(eventId, requestId, "REJECT", "Check-in recorded", key);
    await apiClient.setEventRefundApprover(eventId, bookingId, true);
    await apiClient.setEventRefundApprover(eventId, bookingId, false);
    await assert.rejects(apiClient.decideBookingRefund(eventId, requestId, "APPROVE", "", key), /invalid_request/);
  });
  assert.equal(calls.length, 3);
  assert.equal(calls[0].url, `/api/events/${eventId}/refund-requests/${requestId}/decision`);
  assert.equal(calls[0].options.headers["Idempotency-Key"], key);
  assert.deepEqual(JSON.parse(calls[0].options.body), { decision: "REJECT", reason: "Check-in recorded" });
  assert.equal(calls[1].url, `/api/admin/events/${eventId}/refund-approvers/${bookingId}`);
  assert.equal(calls[1].options.method, "PUT");
  assert.equal(calls[2].options.method, "DELETE");
});

test("event refund queue reads are bounded, scoped, and no-store", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  let observed;
  await browser(async (url, options) => {
    observed = { url, options };
    return Response.json({ refunds: [refund()], next_cursor: "", approval_enabled: false, approval_block_reason: "event_time_authority_unavailable" });
  }, async () => {
    const result = await apiClient.listEventBookingRefunds(eventId, "cursor_1", 25);
    assert.equal(result.refunds[0].request_id, requestId);
  });
  assert.equal(observed.url, `/api/events/${eventId}/refund-requests?page_size=25&cursor=cursor_1`);
  assert.equal(observed.options.method, "GET");
  assert.equal(observed.options.cache, "no-store");
});

test("refund response parsers enforce event scope and expose only buyer safe fields", () => {
  const { parseBookingRefund, parseEventBookingRefunds, canSubmitBuyerRefundRequest, canApproveBookingRefund, canRejectBookingRefund } = loadTypeScript("lib/booking-refunds.ts");
  const sanitized = parseBookingRefund({ ...refund(), stripe_secret: "PRIVATE" }, bookingId);
  assert.equal(JSON.stringify(sanitized).includes("PRIVATE"), false);
  assert.deepEqual(parseEventBookingRefunds({ refunds: [refund()], next_cursor: "", approval_enabled: false, approval_block_reason: "time_authority_unavailable" }, eventId).refunds.length, 1);
  assert.throws(() => parseEventBookingRefunds({ refunds: [refund({ event_id: bookingId })], next_cursor: "", approval_enabled: true, approval_block_reason: "" }, eventId), /invalid/);
  assert.throws(() => parseBookingRefund(refund({ amount_cents: -1 }), bookingId), /invalid/);
  assert.throws(() => parseBookingRefund(refund({ currency: "EUR" }), bookingId), /invalid/);
  assert.equal(parseBookingRefund(refund({ currency: "usd" }), bookingId).currency, "USD");
  const buyerRefund = parseBookingRefund(refund(), bookingId);
  assert.equal(canSubmitBuyerRefundRequest(false, null, "CONFIRMED", true, false), false);
  assert.equal(canSubmitBuyerRefundRequest(true, null, "CONFIRMED", true, false), true);
  assert.equal(canSubmitBuyerRefundRequest(true, buyerRefund, "CONFIRMED", true, false), false);
  assert.equal(canSubmitBuyerRefundRequest(true, null, "CONFIRMED", true, true), false);
  assert.equal(canApproveBookingRefund("REQUESTED", false), false);
  assert.equal(canApproveBookingRefund("WAITING_CHECKIN_RESOLUTION", true), true);
  assert.equal(canApproveBookingRefund("WAITING_CHECKIN_RESOLUTION", true, "USD"), true);
  assert.equal(canApproveBookingRefund("WAITING_CHECKIN_RESOLUTION", false, "USD"), false);
  assert.equal(canApproveBookingRefund("WAITING_CHECKIN_RESOLUTION", true, "EUR"), false);
  assert.equal(canApproveBookingRefund("REQUESTED", true), true);
  assert.equal(canApproveBookingRefund("REQUESTED", true, "EUR"), false);
  assert.equal(canRejectBookingRefund("WAITING_CHECKIN_RESOLUTION"), true);
});

test("buyer ticket access blocks confirmed refund obligations but not unapproved requests", () => {
  const { buyerRefundEntryAccess } = loadTypeScript("lib/booking-refunds.ts");
  for (const status of ["APPROVED", "REFUND_PROCESSING", "REFUNDED", "NEEDS_ATTENTION"]) {
    assert.equal(buyerRefundEntryAccess(true, refund({ status })), "BLOCKED", `${status} must suppress ticket entry access`);
  }
  for (const status of ["REQUESTED", "WAITING_CHECKIN_RESOLUTION", "REJECTED", "WITHDRAWN"]) {
    assert.equal(buyerRefundEntryAccess(true, refund({ status })), "ALLOWED", `${status} must not suppress ticket entry access`);
  }
  assert.equal(buyerRefundEntryAccess(true, null), "ALLOWED");
  assert.equal(buyerRefundEntryAccess(false, { status: "APPROVED" }), "UNKNOWN");
  assert.equal(buyerRefundEntryAccess(true, refund({ status: "FUTURE_APPROVAL_STATE" })), "UNKNOWN");
});

test("refund labels are present in both supported locales", () => {
  const { adminEn, adminZh } = loadTypeScript("lib/i18n/admin.ts");
  const { customerEn, customerZh } = loadTypeScript("lib/i18n/customer.ts");
  for (const key of ["admin.refunds.title", "admin.refunds.approvalDisabled", "admin.refunds.approve", "admin.refunds.retryApproval", "admin.refunds.waitingCheckinNotice", "admin.refunds.revocationNeedsAttention", "admin.refunds.reject"]) {
    assert.equal(typeof adminEn[key], "string");
    assert.equal(typeof adminZh[key], "string");
  }
  for (const key of ["customer.refund.title", "customer.refund.request", "customer.refund.statusRefunded", "customer.refund.revocationPending", "customer.refund.revocationNeedsAttention"]) {
    assert.equal(typeof customerEn[key], "string");
    assert.equal(typeof customerZh[key], "string");
  }
});

test("lagging refund status cannot reopen entry while rejected decisions remain usable", () => {
  const { buyerRefundEntryAccess: access } = loadTypeScript("lib/booking-refunds.ts");
  for (const payment_refund_state of ["pending", "SUCCEEDED", "unknown"]) {
    assert.equal(access(true, refund({ payment_refund_state })), "BLOCKED");
  }
  for (const ticket_revocation_state of ["PENDING", "REVOKED", "NEEDS_ATTENTION"]) {
    assert.equal(access(true, refund({ ticket_revocation_state })), "BLOCKED");
  }
  assert.equal(access(true, refund({ payment_refund_state: "future_state" })), "UNKNOWN");
  assert.equal(access(true, refund({ status: "REJECTED", refund_decision_id: key, payment_refund_state: "not_required", ticket_revocation_state: "NOT_STARTED" })), "ALLOWED");
  assert.equal(access(true, refund({ status: "WAITING_CHECKIN_RESOLUTION" })), "ALLOWED");
});

test("fallback refund access belongs to the current query cycle", () => {
  const { currentBuyerRefundEntryAccess: current } = loadTypeScript("lib/booking-refunds.ts");
  const firstCycle = ["booking-1"];
  const sameIdsNewCycle = ["booking-1"];
  const firstAllowed = { access: "ALLOWED", fallbackCycle: firstCycle };

  assert.equal(current(firstAllowed, true, firstCycle), "ALLOWED");
  // Re-entering fallback with the same user and order creates a new cycle:
  // the old ALLOWED must display as unknown until the new query resolves.
  assert.equal(current(firstAllowed, true, sameIdsNewCycle), "UNKNOWN");
  // A late old response cannot re-authorize the current cycle.
  assert.equal(current(firstAllowed, true, sameIdsNewCycle), "UNKNOWN");
  // The new cycle's own result is current.
  assert.equal(current({ access: "BLOCKED", fallbackCycle: sameIdsNewCycle }, true, sameIdsNewCycle), "BLOCKED");
  // Leaving fallback keeps the last known value for that order.
  assert.equal(current(firstAllowed, false, sameIdsNewCycle), "ALLOWED");
  // A panel result is not fallback-query evidence.
  assert.equal(current({ access: "ALLOWED", fallbackCycle: null }, true, sameIdsNewCycle), "UNKNOWN");
  assert.equal(current(undefined, true, sameIdsNewCycle), "UNKNOWN");
});
