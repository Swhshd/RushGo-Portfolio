import assert from "node:assert/strict";
import { test } from "node:test";
import { booking, cart, provider } from "./booking-provider.mjs";

for (const status of ["EXPIRED", "FAILED", "CANCELLED", "UNKNOWN"]) {
  test(`context cannot publish a success ticket for ${status}`, async () => {
    const render = provider(async () => booking(status));
  await render().ensureEvent(cart.eventId);
    render().setCart(cart);
    await render().initiateBooking(cart);
    assert.equal(await render().confirmPayment("test-intent"), false);
    assert.equal(render().lastPurchaseSuccess, false);
    assert.deepEqual(render().purchasedTickets, []);
  });
}

test("a pending GET followed by confirmed waits for authoritative confirmation before publishing a ticket", async () => {
  const statuses = ["PENDING", "CONFIRMED"];
  const render = provider(async () => booking(statuses.shift() || "CONFIRMED"));
  await render().ensureEvent(cart.eventId);
  render().setCart(cart);
  await render().initiateBooking(cart);
  assert.equal(await render().confirmPayment("test-intent"), true);
  assert.equal(render().lastPurchaseSuccess, true);
  assert.equal(render().purchasedTickets.length, 1);
  assert.equal(render().purchasedTickets[0].status, "Confirmed");
  assert.equal(render().purchasedTickets[0].totalPrice, 10);
});

test("pending confirmation stops after ten reads without creating a ticket or a success banner", async () => {
  let reads = 0;
  const render = provider(async () => { reads++; return booking("PENDING"); });
  await render().ensureEvent(cart.eventId);
  render().setCart(cart);
  await render().initiateBooking(cart);
  assert.equal(await render().confirmPayment("test-intent"), false);
  assert.equal(reads, 10);
  assert.equal(render().lastPurchaseSuccess, false);
  assert.deepEqual(render().purchasedTickets, []);
});
