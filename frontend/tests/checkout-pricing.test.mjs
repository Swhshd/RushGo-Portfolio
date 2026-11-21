import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { browserOverrides, loadTypeScript } from "./render-typescript.mjs";
import { booking, cart, provider } from "./booking-provider.mjs";

function checkout(totalCents, prices = [10]) {
  const { default: Checkout } = loadTypeScript("app/(customer)/checkout/page.tsx", {
    ...browserOverrides,
    "@/lib/auth-context": { useAuth: () => ({ user: { name: "Demo", email: "demo@example.com" } }) },
    "@/lib/booking-context": { useBooking: () => ({
      cart: { eventId: "event", seats: prices.map((price, index) => ({ seatId: `${index}`, sectionName: "Floor", rowLabel: "A", seatLabel: `${index + 1}`, price })) },
      getEvent: () => ({ title: "Demo", date: "Display date", venue: "Arena", location: "Local", image: "/demo.jpg" }),
      paymentClientSecret: "test-session-placeholder",
      pendingBookingTotalCents: totalCents,
      confirmPayment: async () => true,
    }) },
  });
  return renderToStaticMarkup(React.createElement(Checkout));
}

test("checkout summary and real payment form display the server's 1000 cents without client fees", () => {
  const html = checkout(1000);
  assert.match(html, />Order Summary</);
  assert.match(html, />Pay \$10\.00</);
  assert.match(html, />Total</);
  assert.match(html, />Total<\/span><span[^>]*>\$10\.00</);
  assert.doesNotMatch(html, /Service fee|Handling charge|14\.50/);
});

test("checkout follows a different authoritative total even when multiple cart prices are stale", () => {
  const html = checkout(2750, [999, 999]);
  assert.match(html, />Pay \$27\.50</);
  assert.match(html, />Total<\/span><span[^>]*>\$27\.50</);
  assert.doesNotMatch(html, /Service fee|Handling charge|1998\.00/);
});

test("checkout cannot offer payment without an authoritative booking total", () => {
  assert.doesNotMatch(checkout(null), />Pay \$/);
});

test("context stores the server total and clears it with the payment session", async () => {
  const render = provider(async () => booking("CONFIRMED"));
  await render().ensureEvent(cart.eventId);
  await render().initiateBooking(cart);
  assert.equal(render().pendingBookingTotalCents, 1000);
  render().clearPaymentState();
  assert.equal(render().pendingBookingTotalCents, null);
});
