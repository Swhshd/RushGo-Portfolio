import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

function readyBooking(overrides = {}) {
  return {
    id: "booking-1",
    user_id: "user-1",
    event_id: "event-1",
    status: "PENDING",
    total_amount_cents: 2500,
    items: [],
    created_at: "2026-01-01T00:00:00Z",
    checkout_state: "ready",
    reservation_deadline: "2026-11-13T12:00:00Z",
    can_cancel: true,
    paymentIntentClientSecret: "pi_original_secret_original",
    ...overrides,
  };
}

test("resume payment is accepted only for the original pending booking with a validated ready secret", () => {
  const { parseBookingResume } = loadTypeScript("lib/booking-resume.ts");
  const result = parseBookingResume(readyBooking(), "booking-1");
  assert.equal(result.checkout_state, "ready");
  assert.equal(result.booking.id, "booking-1");
  assert.equal(result.booking.paymentIntentClientSecret, "pi_original_secret_original");
  assert.equal(parseBookingResume(readyBooking({ id: "another-booking" }), "booking-1").checkout_state, "unavailable");
  assert.equal(parseBookingResume(readyBooking({ status: "CONFIRMED" }), "booking-1").checkout_state, "unavailable");
  assert.equal(parseBookingResume(readyBooking({ paymentIntentClientSecret: "" }), "booking-1").checkout_state, "unavailable");
  assert.equal(parseBookingResume(readyBooking({ reservation_deadline: "tomorrow" }), "booking-1").checkout_state, "unavailable");
});

test("non-ready resume responses never expose a payment secret to checkout", () => {
  const { parseBookingResume } = loadTypeScript("lib/booking-resume.ts");
  for (const checkout_state of ["processing", "expired", "completed", "unavailable"]) {
    const result = parseBookingResume(readyBooking({ checkout_state }), "booking-1");
    assert.equal(result.checkout_state, checkout_state);
    assert.equal(result.booking.paymentIntentClientSecret, undefined);
  }
});

test("resume endpoint reuses the original booking ID without an idempotency identity", async (t) => {
  const bookingId = "11111111-1111-4111-8111-111111111111";
  const calls = [];
  const nativeFetch = globalThis.fetch;
  t.after(() => { globalThis.fetch = nativeFetch; });
  globalThis.window = {};
  t.after(() => { delete globalThis.window; });
  globalThis.localStorage = { getItem: () => "session-fixture", setItem() {}, removeItem() {} };
  globalThis.fetch = async (url, options) => {
    calls.push({ url, options });
    return new Response(JSON.stringify(readyBooking()), { status: 200 });
  };
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  await apiClient.resumeBooking(bookingId);
  assert.equal(calls[0].url, `/api/bookings/${bookingId}/resume`);
  assert.equal(calls[0].options.method, "POST");
  assert.equal(calls[0].options.headers["Idempotency-Key"], undefined);
  assert.equal(calls[0].options.body, undefined);
});
