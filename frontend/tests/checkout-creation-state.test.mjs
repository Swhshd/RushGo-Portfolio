import assert from "node:assert/strict";
import { test } from "node:test";
import { booking, cart, provider } from "./booking-provider.mjs";

function storage(t) {
  const rows = new Map();
  const previous = Object.getOwnPropertyDescriptor(globalThis, "sessionStorage");
  Object.defineProperty(globalThis, "sessionStorage", { configurable: true, value: {
    getItem: (key) => rows.get(key) ?? null,
    setItem: (key, value) => rows.set(key, value),
    removeItem: (key) => rows.delete(key),
  } });
  t.after(() => { if (previous) Object.defineProperty(globalThis, "sessionStorage", previous); else delete globalThis.sessionStorage; });
  return rows;
}

async function renderWith(response) {
  const render = provider(async () => response, { createBooking: async () => response });
  await render().ensureEvent(cart.eventId);
  render().setCart(cart);
  return render;
}

test("terminal CONFIRMED is an already-complete purchase, never a payable failure", async (t) => {
  storage(t);
  const render = provider(async () => booking("CONFIRMED"), {
    createBooking: async () => ({ ...booking("CONFIRMED"), creation_state: "terminal", paymentIntentClientSecret: "" }),
  });
  await render().ensureEvent(cart.eventId);
  render().setCart(cart);

  const result = await render().initiateBooking();

  assert.equal(result, null);
  assert.equal(render().creationState, "confirmed");
  assert.equal(render().paymentClientSecret, null);
  assert.equal(render().pendingBookingTotalCents, null);
  assert.equal(render().pendingBookingId, null);
  assert.match(render().error, /already confirmed/i);
});

test("terminal non-success remains ended and never exposes a payment session", async (t) => {
  storage(t);
  const render = provider(async () => booking("FAILED"), {
    createBooking: async () => ({ ...booking("FAILED"), creation_state: "terminal", paymentIntentClientSecret: "must-not-leak" }),
  });
  await render().ensureEvent(cart.eventId);
  render().setCart(cart);

  const result = await render().initiateBooking();

  assert.equal(result, null);
  assert.equal(render().creationState, "terminal");
  assert.equal(render().paymentClientSecret, null);
  assert.equal(render().pendingBookingTotalCents, null);
  assert.match(render().error, /has ended/i);
});

for (const state of ["", "unknown", undefined]) {
  test(`creation_state ${JSON.stringify(state)} cannot become ready`, async (t) => {
    storage(t);
    const response = { ...booking("PENDING"), creation_state: state, paymentIntentClientSecret: "must-not-leak" };
    const render = await renderWith(response);

    await assert.rejects(() => render().initiateBooking());

    assert.notEqual(render().creationState, "ready");
    assert.equal(render().paymentClientSecret, null);
    assert.equal(render().pendingBookingTotalCents, null);
    assert.equal(render().lastPurchaseSuccess, false);
    assert.deepEqual(render().purchasedTickets, []);
  });
}

test("ready is the only state that exposes the authoritative payment session", async (t) => {
  storage(t);
  const render = await renderWith({ ...booking("PENDING"), creation_state: "ready" });

  const result = await render().initiateBooking();

  assert.deepEqual(result, { clientSecret: "test-placeholder", bookingId: "booking" });
  assert.equal(render().creationState, "ready");
  assert.equal(render().paymentClientSecret, "test-placeholder");
  assert.equal(render().pendingBookingTotalCents, 1000);
});
