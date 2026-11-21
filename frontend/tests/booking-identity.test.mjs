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

const userBCart = {
  eventId: "event",
  seats: [{ seatId: "b-seat", tierId: "tier", price: 999, sectionName: "Floor", rowLabel: "A", seatLabel: "2" }],
};

const apiEvent = {
  id: "event",
  title: "Demo event",
  date: "2026-10-30T12:00:00Z",
  venue: "Demo venue",
  location: "Demo city",
  category: "concerts",
  description: "Synthetic event",
  image_url: "",
  tiers: [],
};

const getEvent = async (id) => ({ ...apiEvent, id });

test("booking provider skips the event catalog and loads a requested event directly by ID", async () => {
  const requestedIds = [];
  const render = provider(async () => booking("PENDING"), {
    effects: false,
    getEvent: async (id) => { requestedIds.push(id); return { ...apiEvent, id }; },
  });
  const initial = render();
  assert.deepEqual(initial.events, []);
  assert.deepEqual(requestedIds, []);
  await initial.ensureEvent("event");
  const loaded = render();
  assert.equal(loaded.getEvent("event").title, apiEvent.title);
  assert.deepEqual(requestedIds, ["event"]);
});

test("logout and switching owners clears prior booking and success state while restoring only the new owner's cart", async (t) => {
  const rows = storage(t);
  rows.set("ticketbox.booking-cart.v1:user-b", JSON.stringify(userBCart));
  const render = provider(async () => booking("CONFIRMED"), {
    user: { id: "user-a" },
    effects: true,
    getEvent,
    createBooking: async () => booking("PENDING"),
  });

  render().setCart(cart);
  await render().ensureEvent("event");
  await render().initiateBooking(cart);
  const beforeConfirm = render();
  assert.equal(beforeConfirm.paymentClientSecret, "test-placeholder");
  assert.equal(await beforeConfirm.confirmPayment("test-intent"), true);

  const userA = render();
  assert.equal(userA.lastPurchaseSuccess, true);
  assert.equal(userA.lastPurchaseSuccessUserId, "user-a");
  assert.equal(userA.purchasedTickets.length, 1);
  assert.equal(userA.purchasedTickets[0].id, "booking");

  render.setAuth(null, true);
  const loggedOut = render();
  assert.equal(loggedOut.cart, null);
  assert.deepEqual(loggedOut.purchasedTickets, []);
  assert.equal(loggedOut.lastPurchaseSuccess, false);
  assert.equal(loggedOut.lastPurchaseSuccessUserId, null);
  assert.equal(loggedOut.paymentClientSecret, null);
  assert.equal(loggedOut.pendingBookingId, null);
  assert.equal(loggedOut.pendingBookingTotalCents, null);
  assert.equal(loggedOut.creationState, null);
  assert.equal(loggedOut.error, null);

  render.setAuth({ id: "user-b" }, false);
  const userB = render();

  assert.equal(userB.cart.seats[0].seatId, "b-seat");
  assert.deepEqual(userB.purchasedTickets, []);
  assert.equal(userB.lastPurchaseSuccess, false);
  assert.equal(userB.lastPurchaseSuccessUserId, null);
  assert.equal(userB.paymentClientSecret, null);
  assert.equal(userB.pendingBookingId, null);
  assert.equal(userB.pendingBookingTotalCents, null);
  assert.equal(userB.creationState, null);
  assert.equal(userB.error, null);
});

test("same-owner profile refresh and auth loading changes preserve the active booking and success state", async (t) => {
  storage(t);
  const render = provider(async () => booking("CONFIRMED"), {
    user: { id: "user-a", name: "Before refresh" },
    effects: true,
    getEvent,
    createBooking: async () => booking("PENDING"),
  });

  render().setCart(cart);
  await render().ensureEvent("event");
  await render().initiateBooking(cart);
  assert.equal(await render().confirmPayment("test-intent"), true);

  render.setAuth({ id: "user-a", name: "After refresh" }, true);
  const duringProfileRefresh = render();
  assert.equal(duringProfileRefresh.lastPurchaseSuccess, true);
  assert.equal(duringProfileRefresh.lastPurchaseSuccessUserId, "user-a");
  assert.equal(duringProfileRefresh.purchasedTickets.length, 1);
  assert.equal(duringProfileRefresh.cart, null);
  assert.equal(duringProfileRefresh.paymentClientSecret, null);

  render.setAuth({ id: "user-a", name: "After refresh" }, false);
  const afterProfileRefresh = render();
  assert.equal(afterProfileRefresh.lastPurchaseSuccess, true);
  assert.equal(afterProfileRefresh.lastPurchaseSuccessUserId, "user-a");
  assert.equal(afterProfileRefresh.purchasedTickets.length, 1);
  assert.equal(afterProfileRefresh.cart, null);
  assert.equal(afterProfileRefresh.paymentClientSecret, null);
});

test("the authenticated owner's saved seat selection is restorable without restoring payment credentials", (t) => {
  const rows = storage(t);
  rows.set("ticketbox.booking-cart.v1:user-a", JSON.stringify(cart));
  const render = provider(async () => booking("PENDING"), {
    user: { id: "user-a" },
    effects: true,
    getEvent,
  });

  const restored = render();

  assert.equal(restored.cart.seats[0].seatId, "seat");
  assert.equal(restored.paymentClientSecret, null);
  assert.equal(restored.pendingBookingId, null);
  assert.equal(restored.pendingBookingTotalCents, null);
  assert.equal(restored.creationState, null);
  assert.equal(restored.lastPurchaseSuccess, false);
  assert.deepEqual(restored.purchasedTickets, []);
});

test("a restored cart and its automatic booking callback stay stable across booking state updates", async (t) => {
  const rows = storage(t);
  rows.set("ticketbox.booking-cart.v1:user-a", JSON.stringify(cart));
  const render = provider(async () => booking("PENDING"), {
    user: { id: "user-a" },
    effects: true,
    getEvent,
    createBooking: async () => booking("PENDING"),
  });

  await render().ensureEvent("event");
  const beforeBooking = render();
  const restoredCart = beforeBooking.cart;
  const initiateBooking = beforeBooking.initiateBooking;
  assert.equal(restoredCart.seats[0].seatId, "seat");

  await initiateBooking();
  const afterBookingStateUpdate = render();

  assert.equal(afterBookingStateUpdate.cart, restoredCart);
  assert.equal(afterBookingStateUpdate.initiateBooking, initiateBooking);
  assert.equal(afterBookingStateUpdate.paymentClientSecret, "test-placeholder");
  assert.equal(afterBookingStateUpdate.creationState, "ready");
});

test("a late user A booking response cannot replace user B's active booking state", async (t) => {
  const rows = storage(t);
  rows.set("ticketbox.booking-cart.v1:user-b", JSON.stringify(userBCart));
  let resolveUserA;
  let createCount = 0;
  const render = provider(async () => booking("PENDING"), {
    user: { id: "user-a" },
    effects: true,
    getEvent,
    createBooking: async () => {
      createCount++;
      if (createCount === 1) {
        return new Promise((resolve) => { resolveUserA = resolve; });
      }
      return { ...booking("PENDING"), id: "booking-b", paymentIntentClientSecret: "secret-b" };
    },
  });

  render().setCart(cart);
  await render().ensureEvent("event");
  const userARequest = render().initiateBooking(cart);

  render.setUser({ id: "user-b" });
  const userB = render();
  assert.equal(userB.cart.seats[0].seatId, "b-seat");
  await userB.initiateBooking();
  const userBReady = render();
  assert.equal(userBReady.pendingBookingId, "booking-b");
  assert.equal(userBReady.paymentClientSecret, "secret-b");

  resolveUserA({ ...booking("PENDING"), id: "booking-a", paymentIntentClientSecret: "secret-a" });
  assert.equal(await userARequest, null);

  const afterLateResponse = render();
  assert.equal(afterLateResponse.cart.seats[0].seatId, "b-seat");
  assert.equal(afterLateResponse.pendingBookingId, "booking-b");
  assert.equal(afterLateResponse.paymentClientSecret, "secret-b");
  assert.equal(afterLateResponse.creationState, "ready");
  assert.equal(afterLateResponse.lastPurchaseSuccess, false);
  assert.deepEqual(afterLateResponse.purchasedTickets, []);
});
