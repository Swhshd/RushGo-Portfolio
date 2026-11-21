import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { browserOverrides, loadTypeScript } from "./render-typescript.mjs";

const tier = (overrides) => ({
  id: "tier-1",
  event_id: "event",
  name: "Floor",
  price_cents: 2500,
  total_quantity: 12,
  available_quantity: 10,
  version: 3,
  ...overrides,
});

test("transformTier only reports sellable when the server projects readiness", () => {
  const { transformTier } = loadTypeScript("lib/api/transformers.ts");
  assert.equal(transformTier(tier({ accounting_ready: true })).accountingReady, true);
  assert.equal(transformTier(tier({ accounting_ready: false })).accountingReady, false);
  // A missing projection (old or failed API response) must never read as ready.
  assert.equal(transformTier(tier({})).accountingReady, false);
});

test("event cards calculate the starting price from sellable tiers only", () => {
  const { EventCard } = loadTypeScript("components/event-card.tsx", {
    ...browserOverrides,
    "@/components/event-artwork": { EventArtwork: () => null },
  });
  const event = {
    id: "event",
    title: "Mixed readiness",
    date: "Dec 2, 2026",
    time: "8:00 PM",
    venue: "Arena",
    location: "Local",
    category: "Concerts",
    genre: "Live",
    description: "",
    image: "",
    tiers: [
      { id: "floor", name: "Floor", description: "10 of 12 remaining", price: 10, available: 10, accountingReady: false },
      { id: "early-bird", name: "Early Bird", description: "Sold out", price: 15, available: 0, accountingReady: true },
      { id: "courtside", name: "Courtside", description: "4 of 4 remaining", price: 30, available: 4, accountingReady: true },
    ],
  };

  const html = renderToStaticMarkup(React.createElement(EventCard, { event }));
  assert.match(html, /From \$30/);
  assert.doesNotMatch(html, /From \$10/);
});

test("event cards distinguish sold-out events from events with unready tiers", () => {
  const { EventCard } = loadTypeScript("components/event-card.tsx", {
    ...browserOverrides,
    "@/components/event-artwork": { EventArtwork: () => null },
  });
  const soldOut = eventWith([
    { id: "a", name: "Balcony", description: "Sold out", price: 20, available: 0, accountingReady: true },
  ]);
  const temporarilyUnavailable = eventWith([
    { id: "a", name: "Floor", description: "10 of 12 remaining", price: 10, available: 10, accountingReady: false },
    { id: "b", name: "Balcony", description: "Sold out", price: 20, available: 0, accountingReady: true },
  ]);
  const noTiers = eventWith([]);

  const soldOutHtml = renderToStaticMarkup(React.createElement(EventCard, { event: soldOut }));
  const unavailableHtml = renderToStaticMarkup(React.createElement(EventCard, { event: temporarilyUnavailable }));
  const noTiersHtml = renderToStaticMarkup(React.createElement(EventCard, { event: noTiers }));
  assert.match(soldOutHtml, />Sold out</);
  assert.doesNotMatch(soldOutHtml, /Temporarily unavailable/);
  assert.match(unavailableHtml, /Temporarily unavailable/);
  assert.doesNotMatch(unavailableHtml, /From \$/);
  assert.match(noTiersHtml, />Unavailable</);
  assert.doesNotMatch(noTiersHtml, /Sold out/);
});

test("venue layout carries tier sellability onto its sections", () => {
  const { generateVenueLayout } = loadTypeScript("lib/api/generate-venue-layout.ts");
  const layout = generateVenueLayout(
    [tier({ id: "ready", name: "Ready", accounting_ready: true }), tier({ id: "unready", name: "Unready", accounting_ready: false })],
    "concerts",
  );
  const byTier = new Map(layout.sections.map((section) => [section.id.split(":")[0], section.accountingReady]));
  assert.equal(byTier.get("ready"), true);
  assert.equal(byTier.get("unready"), false);
  assert.ok(layout.sections.every((section) => section.availableSeats > 0), "an unready tier must not be rewritten as sold out");
});

test("venue map labels an unready tier as temporarily unavailable and blocks selection", () => {
  const { VenueMap } = loadTypeScript("components/venue-map.tsx");
  const layout = {
    id: "venue",
    name: "Venue",
    sections: [
      { id: "s1", name: "Ready", tier: "Ready", price: 25, color: "#fff", path: "M0 0", labelPosition: { x: 0, y: 0 }, totalSeats: 4, availableSeats: 2, accountingReady: true, rows: [] },
      { id: "s2", name: "Unready", tier: "Unready", price: 25, color: "#fff", path: "M0 0", labelPosition: { x: 0, y: 0 }, totalSeats: 4, availableSeats: 2, accountingReady: false, rows: [] },
      { id: "s3", name: "Sold", tier: "Sold", price: 25, color: "#fff", path: "M0 0", labelPosition: { x: 0, y: 0 }, totalSeats: 4, availableSeats: 0, accountingReady: true, rows: [] },
    ],
  };
  const html = renderToStaticMarkup(React.createElement(VenueMap, { layout, onSectionClick() {} }));
  assert.match(html, /Temporarily unavailable/);
  assert.match(html, /Sold out/);
  assert.doesNotMatch(html, /Unready, Sold out/);
  assert.match(html, /aria-label="Unready, Temporarily unavailable"[^>]*aria-disabled="true"/);
  assert.match(html, /aria-label="Sold, Sold out"[^>]*aria-disabled="true"/);
  assert.match(html, /aria-label="Select Ready"/);
});

function eventWith(tiers) {
  return { id: "event", title: "Demo", date: "Dec 2, 2026", time: "8:00 PM", venue: "Arena", location: "Local", category: "Concerts", tiers };
}

function seatsPage(event, initialState = {}) {
  const slots = [
    event ? null : "event", null, [], initialState.venueLayout ?? null,
    { eventId: "event", state: initialState.isLoadingSeats ? "loading" : "ready" },
    Object.hasOwn(initialState, "loadedEventId") ? initialState.loadedEventId : "event",
    { eventId: "event", phase: "LIVE" }, { current: "LIVE" }, 0, { current: true }, { current: "event" },
    false, null, { current: false }, { current: [] }, { current: false },
  ];
  let cursor = 0;
  const hooks = {
    ...React,
    useState(initial) { const index = cursor++; if (!(index in slots)) slots[index] = initial; return [slots[index], (value) => { slots[index] = typeof value === "function" ? value(slots[index]) : value; }]; },
    useRef(initial) { const index = cursor++; return (slots[index] ||= { current: initial }); },
    useMemo: (fn) => fn(),
    useCallback: (fn) => fn,
    useEffect() {},
  };
  const { default: Page } = loadTypeScript("app/(customer)/events/[id]/seats/page.tsx", {
    ...browserOverrides,
    react: hooks,
    "next/navigation": { useParams: () => ({ id: "event" }), useRouter: () => ({ push() {} }) },
    "@/lib/auth-context": { useAuth: () => ({ user: { id: "user" }, isLoading: false }) },
    "@/lib/booking-context": { useBooking: () => ({ getEvent: () => event, isLoading: false, setCart() {}, clearPaymentState() {}, initiateBooking: async () => null }) },
  });
  cursor = 0;
  return renderToStaticMarkup(React.createElement(Page));
}

function eventDetailPage(event) {
  const { default: Page } = loadTypeScript("app/(customer)/events/[id]/page.tsx", {
    ...browserOverrides,
    "@/lib/auth-context": { useAuth: () => ({ user: null, isLoading: false }) },
    "next/navigation": { useParams: () => ({ id: "event" }), useRouter: () => ({ push() {} }) },
    "@/lib/booking-context": { useBooking: () => ({ getEvent: () => event, isLoading: false }) },
  });
  return renderToStaticMarkup(React.createElement(Page));
}

test("seats page refuses to offer seat selection when no tier is on sale", () => {
  const html = seatsPage(eventWith([
    { id: "a", name: "Floor", description: "10 of 12 remaining", price: 10, available: 10, accountingReady: false },
  ]), { venueLayout: { sections: [] } });
  assert.match(html, /No ticket tiers are currently on sale for this event/);
  assert.doesNotMatch(html, /Continue to Checkout/);
});

test("seats page shows a sold-out empty state when every ready tier has zero inventory", () => {
  const html = seatsPage(eventWith([
    { id: "a", name: "Balcony", description: "Sold out", price: 20, available: 0, accountingReady: true },
    { id: "b", name: "Floor", description: "Sold out", price: 10, available: 0, accountingReady: true },
  ]), { venueLayout: { sections: [] } });
  assert.match(html, /All ticket tiers are sold out/);
  assert.match(html, /Back to event/);
  assert.doesNotMatch(html, /Continue to Checkout/);
  assert.doesNotMatch(html, /Select seats from the venue map/);
});

test("seats page offers a return path when an event has no ticket tiers", () => {
  const html = seatsPage(eventWith([]), { venueLayout: { sections: [] } });
  assert.match(html, /No ticket tiers are currently available for this event/);
  assert.match(html, /Back to event/);
  assert.doesNotMatch(html, /All ticket tiers are sold out/);
  assert.doesNotMatch(html, /Continue to Checkout/);
});

test("seats page exits loading when the event lookup fails and offers a return path", () => {
  const html = seatsPage(null, { isLoadingSeats: true, loadedEventId: null });
  assert.match(html, /This event is unavailable/);
  assert.match(html, /Back to events/);
  assert.doesNotMatch(html, /animate-spin/);
});

test("seats page still offers a ready tier when a sibling tier is unready", () => {
  const html = seatsPage(eventWith([
    { id: "a", name: "Floor", description: "10 of 12 remaining", price: 10, available: 10, accountingReady: false },
    { id: "b", name: "Courtside", description: "4 of 4 remaining", price: 30, available: 4, accountingReady: true },
  ]), {
    venueLayout: {
      sections: [
        { id: "floor", name: "Floor", tier: "Floor", price: 10, color: "#fff", path: "M0 0", labelPosition: { x: 0, y: 0 }, totalSeats: 12, availableSeats: 10, accountingReady: false, rows: [] },
        { id: "courtside", name: "Courtside", tier: "Courtside", price: 30, color: "#fff", path: "M0 0", labelPosition: { x: 0, y: 0 }, totalSeats: 4, availableSeats: 4, accountingReady: true, rows: [] },
      ],
    },
  });
  assert.doesNotMatch(html, /No ticket tiers are currently on sale for this event/);
  assert.match(html, /Temporarily unavailable/);
  assert.match(html, /aria-label="Select Courtside"/);
  assert.match(html, /aria-label="Floor, Temporarily unavailable"[^>]*aria-disabled="true"/);
});

test("event detail uses both readiness and inventory to report availability", () => {
  const mixed = eventDetailPage(eventWith([
    { id: "a", name: "Floor", description: "10 of 12 remaining", price: 10, available: 10, accountingReady: false },
    { id: "b", name: "Courtside", description: "4 of 4 remaining", price: 30, available: 4, accountingReady: true },
  ]));
  assert.match(mixed, /Temporarily unavailable — accounting verification is pending/);
  assert.match(mixed, /Select Seats/);
  assert.match(mixed, /Available/);

  const blocked = eventDetailPage(eventWith([
    { id: "a", name: "Floor", description: "10 of 12 remaining", price: 10, available: 10, accountingReady: false },
  ]));
  assert.doesNotMatch(blocked, /Select Seats/);
  assert.match(blocked, /No ticket tiers are currently on sale for this event/);
  assert.doesNotMatch(blocked, /10 tickets left/);

  const soldOut = eventDetailPage(eventWith([
    { id: "a", name: "Balcony", description: "Sold out", price: 20, available: 0, accountingReady: true },
  ]));
  assert.match(soldOut, /Sold out/);
  assert.match(soldOut, /All ticket tiers are sold out/);
  assert.doesNotMatch(soldOut, /Select Seats/);
  assert.doesNotMatch(soldOut, />Available</);

  const oneTicketLeft = eventDetailPage(eventWith([
    { id: "single", name: "Last seat", description: "1 of 12 remaining", price: 10, available: 1, accountingReady: true },
  ]));
  assert.match(oneTicketLeft, /1 ticket left/);
  assert.doesNotMatch(oneTicketLeft, /1 tickets left/);
});
