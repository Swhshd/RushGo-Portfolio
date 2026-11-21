import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { browserOverrides, loadTypeScript } from "./render-typescript.mjs";

function checkout({ loading = false, paymentReady = !loading, authLoading = false, event = true, cart = true, error = null } = {}) {
  const { default: Checkout } = loadTypeScript("app/(customer)/checkout/page.tsx", {
    ...browserOverrides,
    "@/lib/auth-context": { useAuth: () => ({ user: { name: "Buyer", email: "buyer@example.test" }, isLoading: authLoading }) },
    "@/lib/booking-context": { useBooking: () => ({
      cart: cart ? { eventId: "event", seats: [{ seatId: "seat", sectionName: "Floor", rowLabel: "A", seatLabel: "1" }] } : null,
      getEvent: () => event ? { title: "Event", date: "Tomorrow", venue: "Arena", location: "Local", image: "/demo.jpg" } : undefined,
      isLoading: loading,
      paymentClientSecret: paymentReady ? "test-session" : null,
      pendingBookingTotalCents: 100,
      creationState: "ready",
      confirmPayment: async () => false,
      error,
    }) },
  });
  return renderToStaticMarkup(React.createElement(Checkout));
}

test("checkout waits for event data rather than rendering a blank page or payment", () => {
  const html = checkout({ loading: true, event: false });
  assert.match(html, /role="status"/);
  assert.match(html, /Loading/);
  assert.doesNotMatch(html, />Pay \$/);
});

test("checkout waits for identity recovery before declaring the cart empty", () => {
  const html = checkout({ authLoading: true, cart: false });
  assert.match(html, /Loading/);
  assert.doesNotMatch(html, /No items in your cart/);
});

test("an empty cart offers routes to discover events and view existing tickets", () => {
  const html = checkout({ cart: false });
  assert.match(html, /href="\/"/);
  assert.match(html, /Discover all events/);
  assert.match(html, /href="\/my-tickets"/);
  assert.match(html, /View My Tickets/);
});

test("inventory refresh with an existing payment session keeps the payment form visible", () => {
  const html = checkout({ loading: true, paymentReady: true });
  assert.match(html, />Pay \$1\.00</);
  assert.match(html, /Card input/);
  assert.doesNotMatch(html, /Loading/);
});

test("unavailable event offers navigation without offering a payment", () => {
  const html = checkout({ event: false });
  assert.match(html, /Event not found/);
  assert.match(html, /href="\/my-tickets"/);
  assert.match(html, /href="\/"/);
  assert.doesNotMatch(html, />Pay \$/);
});

test("a payable checkout keeps an order lookup route when confirmation is delayed", () => {
  const html = checkout({ error: "Your booking is not confirmed. Please check My Tickets shortly." });
  assert.match(html, /href="\/my-tickets"/);
  assert.match(html, /Your booking is not confirmed/);
  assert.match(html, />Pay \$1\.00</);
});

test("booking recovery explains same-purchase retry and checking tickets before a new order", () => {
  const html = checkout({ paymentReady: false });
  assert.match(html, /Retrying recovers this same purchase/);
  assert.match(html, /Check My Tickets before starting a new order/);
  assert.match(html, /Returning to seat selection does not cancel the previous booking/);
  assert.match(html, /href="\/my-tickets"/);
});
