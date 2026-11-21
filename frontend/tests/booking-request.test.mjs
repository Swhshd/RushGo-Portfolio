import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";
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

test("request identity survives module reload and is scoped to user and semantic cart", (t) => {
  storage(t);
  let ids = 0;
  t.mock.method(crypto, "randomUUID", () => `deterministic_${++ids}`);
  const first = loadTypeScript("lib/booking-request.ts");
  const key = first.getOrCreateBookingRequest("user", "canonical-cart");
  assert.equal(first.getOrCreateBookingRequest("user", "canonical-cart"), key);
  const reloaded = loadTypeScript("lib/booking-request.ts");
  assert.equal(reloaded.getOrCreateBookingRequest("user", "canonical-cart"), key);
  assert.notEqual(reloaded.getOrCreateBookingRequest("other-user", "canonical-cart"), key);
  assert.notEqual(reloaded.getOrCreateBookingRequest("user", "changed-cart"), key);
  const changed = reloaded.getOrCreateBookingRequest("user", "changed-cart");
  reloaded.clearBookingRequest("user");
  assert.notEqual(reloaded.getOrCreateBookingRequest("user", "changed-cart"), changed);
});

test("canonical cart ordering and display prices cannot create a new request", (t) => {
  storage(t);
  const helpers = loadTypeScript("lib/booking-request.ts");
  const a = { eventId: "event", seats: [{ seatId: "b", tierId: "z", price: 900 }, { seatId: "a", tierId: "a", price: 1 }] };
  const b = { ...a, seats: [...a.seats].reverse().map((seat) => ({ ...seat, price: 500 })) };
  assert.equal(helpers.bookingCartFingerprint(a), helpers.bookingCartFingerprint(b));
  assert.notEqual(helpers.bookingCartFingerprint(a), helpers.bookingCartFingerprint({ ...a, seats: [a.seats[0]] }));
  helpers.saveBookingCart("user", a);
  assert.deepEqual(helpers.restoreBookingCart("user"), a);
  assert.equal(helpers.restoreBookingCart("other-user"), null);
});

test("provider effects restore the cart on reload and isolate auth user changes", async (t) => {
  storage(t);
  const helpers = loadTypeScript("lib/booking-request.ts");
  helpers.saveBookingCart("user", cart);
  const originalKey = helpers.getOrCreateBookingRequest("user", helpers.bookingCartFingerprint(cart));
  const options = { effects: true, user: { id: "user" }, createBooking: async (_event, _items, key) => { assert.equal(key, originalKey); return booking("PENDING"); } };
  const render = provider(async () => booking("PENDING"), options);
  await render().ensureEvent(cart.eventId);
  render();
  assert.deepEqual(render().cart, cart);
  await render().initiateBooking();
  assert.equal(render().paymentClientSecret, "test-placeholder");
  options.user = { id: "other-user" };
  render();
  assert.equal(render().cart, null);
  assert.equal(render().paymentClientSecret, null);
  render.unmount();
});

test("storage failure prevents a POST whose key could not be recovered", async (t) => {
  storage(t);
  t.mock.method(sessionStorage, "setItem", () => { throw new Error("storage unavailable"); });
  let calls = 0;
  const render = provider(async () => booking("PENDING"), { createBooking: async () => { calls++; return booking("PENDING"); } });
  await render().ensureEvent(cart.eventId);
  await assert.rejects(render().initiateBooking(cart));
  assert.equal(calls, 0);
});

test("storage recovery releases a rejected single flight for explicit retry", async (t) => {
  storage(t);
  const save = sessionStorage.setItem;
  t.mock.method(sessionStorage, "setItem", () => { throw new Error("storage unavailable"); });
  let calls = 0;
  const render = provider(async () => booking("PENDING"), { createBooking: async () => { calls++; return booking("PENDING"); } });
  await render().ensureEvent(cart.eventId);
  await assert.rejects(render().initiateBooking(cart));
  sessionStorage.setItem = save;
  assert.ok(await render().initiateBooking(cart));
  assert.equal(calls, 1);
});

test("late confirmation cannot erase a newer cart and request", async (t) => {
  storage(t);
  let release;
  const barrier = new Promise((resolve) => { release = resolve; });
  const render = provider(async () => { await barrier; return booking("CONFIRMED"); });
  await render().ensureEvent(cart.eventId);
  render().setCart(cart);
  await render().initiateBooking(cart);
  const pending = render().confirmPayment("intent");
  const next = { ...cart, seats: [{ ...cart.seats[0], seatId: "new-seat" }] };
  render().setCart(next);
  release();
  assert.equal(await pending, false);
  assert.deepEqual(render().cart, next);
  assert.equal(render().lastPurchaseSuccess, false);
  assert.deepEqual(render().purchasedTickets, []);
});

test("cart change and session change discard a late ready result", async (t) => {
  storage(t);
  let release;
  const barrier = new Promise((resolve) => { release = resolve; });
  const options = { sessionVersion: 0, createBooking: async () => { await barrier; return booking("PENDING"); } };
  const render = provider(async () => booking("PENDING"), options);
  await render().ensureEvent(cart.eventId);
  const pending = render().initiateBooking(cart);
  render().setCart({ ...cart, seats: [{ ...cart.seats[0], seatId: "different-seat" }] });
  options.sessionVersion++;
  release();
  assert.equal(await pending, null);
  assert.equal(render().paymentClientSecret, null);
  assert.equal(render().lastPurchaseSuccess, false);
});

test("lost response and provider reload reuse the identical POST key", async (t) => {
  const rows = storage(t);
  const keys = [];
  let lose = true;
  const options = { createBooking: async (_event, _items, key) => {
    keys.push(key);
    if (lose) { lose = false; throw new Error("response lost"); }
    return { ...booking("PENDING"), creation_state: "ready" };
  } };
  const render = provider(async () => booking("PENDING"), options);
  await render().ensureEvent(cart.eventId);
  await assert.rejects(render().initiateBooking(cart));
  await render().initiateBooking(cart);
  const reloaded = provider(async () => booking("PENDING"), options);
  await reloaded().ensureEvent(cart.eventId);
  await reloaded().initiateBooking(cart);
  assert.equal(typeof keys[0], "string");
  assert.ok(keys[0].length > 0);
  assert.deepEqual(keys, [keys[0], keys[0], keys[0]]);
  assert.doesNotMatch([...rows.values()].join(""), /test-placeholder|access_token|clientSecret/);
});

test("double invocation single-flights the request and waits through starting", async (t) => {
  storage(t);
  t.mock.method(globalThis, "setTimeout", (callback) => { queueMicrotask(callback); return 0; });
  let release;
  const barrier = new Promise((resolve) => { release = resolve; });
  let calls = 0;
  const keys = [];
  const render = provider(async () => booking("PENDING"), { createBooking: async (_event, _items, key) => {
    keys.push(key); calls++;
    if (calls === 1) { await barrier; return { ...booking("PENDING"), creation_state: "starting", paymentIntentClientSecret: "must_not_expose" }; }
    return { ...booking("PENDING"), creation_state: "ready" };
  } });
  await render().ensureEvent(cart.eventId);
  const first = render().initiateBooking(cart);
  const second = render().initiateBooking(cart);
  assert.equal(calls, 1);
  assert.equal(render().paymentClientSecret, null);
  release();
  const [a, b] = await Promise.all([first, second]);
  assert.deepEqual(a, b);
  assert.equal(calls, 2);
  assert.equal(keys[0], keys[1]);
  assert.equal(render().paymentClientSecret, "test-placeholder");
});

test("ten starting responses retain the key for explicit retry without payment", async (t) => {
  storage(t);
  const delays = [];
  t.mock.method(globalThis, "setTimeout", (callback, ms) => { delays.push(ms); queueMicrotask(callback); return 0; });
  const keys = [];
  const render = provider(async () => booking("PENDING"), { createBooking: async (_event, _items, key) => {
    keys.push(key); return { ...booking("PENDING"), creation_state: "starting", paymentIntentClientSecret: "must_not_expose" };
  } });
  await render().ensureEvent(cart.eventId);
  assert.equal(await render().initiateBooking(cart), null);
  assert.equal(keys.length, 10);
  assert.deepEqual(delays, Array(9).fill(1000));
  assert.equal(render().paymentClientSecret, null);
  assert.equal(render().lastPurchaseSuccess, false);
  await render().initiateBooking(cart);
  assert.equal(keys.length, 20);
  assert.ok(keys.every((key) => key === keys[0]));
});

test("terminal creation never offers a payment secret or purchase success", async (t) => {
  storage(t);
  const render = provider(async () => booking("FAILED"), { createBooking: async () => ({ ...booking("FAILED"), creation_state: "terminal" }) });
  await render().ensureEvent(cart.eventId);
  assert.equal(await render().initiateBooking(cart), null);
  assert.equal(render().paymentClientSecret, null);
  assert.equal(render().lastPurchaseSuccess, false);
});
