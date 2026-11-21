import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
const frontendRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { loadTypeScript, browserOverrides } from "./render-typescript.mjs";
import { cart } from "./booking-provider.mjs";

function seats(create, persist = () => {}) {
  const pushes = [], mutations = [];
  const authoritativeSeat = {
    id: "seat", event_id: "event", ticket_tier_id: "tier-1", status: "available",
    position: { sectionId: "Floor-center", row: "A", seat: 1, x: 200, y: 200 },
    created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z", seat_revision: "1",
  };
  const selectedSeat = { ...cart.seats[0], sectionId: "Floor-center", tierId: "tier-1", price: 25 };
  const layout = { sections: [{ id: "Floor-center", name: "Floor", tier: "Floor", tierId: "tier-1", price: 25, color: "#E8567F", path: "", labelPosition: { x: 200, y: 200 }, totalSeats: 1, availableSeats: 1, accountingReady: true, rows: [{ id: "Floor-center-A", label: "Row A", seats: [{ id: "seat", label: "1", status: "available", seatRevision: "1" }] }] }] };
  const slots = [null, null, [selectedSeat], layout, { eventId: "event", state: "ready" }, "event", { eventId: "event", phase: "LIVE" }, { current: "LIVE" }, 0,
    { current: true }, { current: "event" }, false, null, { current: false }, { current: [authoritativeSeat] }, { current: false }];
  let cursor = 0;
  const hooks = { ...React,
    useContext(context) { return context._currentValue; },
    useState(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = initial;
      return [slots[index], value => { slots[index] = typeof value === "function" ? value(slots[index]) : value; }];
    },
    useRef(initial) { const index = cursor++; return slots[index] ||= { current: initial }; },
    useMemo: fn => fn(), useCallback: fn => fn, useEffect() {},
  };
  const { default: Page } = loadTypeScript("app/(customer)/events/[id]/seats/page.tsx", {
    ...browserOverrides, react: hooks,
    "next/navigation": { useParams: () => ({ id: "event" }), useRouter: () => ({ push: href => pushes.push(href) }) },
    "@/lib/api/client": { apiClient: { getSeats: async () => ({ seats: [authoritativeSeat] }) } },
    "@/lib/auth-context": { useAuth: () => ({ user: { id: "user" }, isLoading: false }) },
    "@/lib/booking-context": { useBooking: () => ({ getEvent: () => ({ id: "event", title: "Demo", category: "concerts", venue: "Venue", tiers: [{ id: "tier-1", name: "Floor", description: "10 of 12 remaining", price: 25, available: 10, accountingReady: true }] }), isLoading: false,
      setCart: value => { persist(value); mutations.push(value); }, clearPaymentState() {}, initiateBooking: create, refreshEvents: async () => {} }) },
  });
  function find(tree) {
    if (!tree || typeof tree !== "object") return null;
    if (tree.type === "button" && JSON.stringify(tree.props.children).includes("Continue to Checkout")) return tree;
    for (const child of React.Children.toArray(tree.props?.children)) { const found = find(child); if (found) return found; }
    return null;
  }
  return { pushes, mutations, render: () => { cursor = 0; return find(Page()); } };
}

test("lost response retains the original cart after reserved SSE and permits retry", async () => {
  let calls = 0;
  const d = seats(async () => { calls++; throw Object.assign(new Error("response lost"), { code: "BOOKING_ERROR" }); });
  await d.render().props.onClick();
  assert.equal(d.mutations.length, 1);
  assert.equal(d.mutations[0].eventId, "event");
  assert.deepEqual(d.mutations[0].seats.map((seat) => seat.seatId), ["seat"]);
  await d.render().props.onClick();
  assert.equal(calls, 2);
  assert.deepEqual(d.pushes, []);
});

test("storage failure during Continue releases submission and can be retried", async () => {
  let unavailable = true, calls = 0;
  const d = seats(async () => { calls++; return { bookingId: "original" }; }, () => { if (unavailable) throw new Error("storage unavailable"); });
  await d.render().props.onClick();
  assert.equal(calls, 0);
  assert.equal(d.render().props.disabled, false);
  unavailable = false;
  await d.render().props.onClick();
  assert.equal(calls, 1);
  assert.deepEqual(d.pushes, ["/checkout"]);
});

test("booking catalog errors do not silently publish mock events", () => {
  const source = fs.readFileSync(path.join(frontendRoot, "lib/booking-context.tsx"), "utf8");
  assert.doesNotMatch(source, /mockEvents|using mock data/i);
});

test("seat layout construction does not create an interactive layout without real seats", () => {
  const { generateVenueLayoutFromSeats } = loadTypeScript("lib/api/transformers.ts");
  const layout = generateVenueLayoutFromSeats([], [], "concerts");
  assert.deepEqual(layout.sections, []);
});

test("seat fetch failure keeps event details but does not create a synthetic seat layout", async () => {
  const { fetchEventWithSeats } = loadTypeScript("lib/api/transformers.ts");
  const originalWarn = console.warn;
  console.warn = () => {};
  try {
    const event = await fetchEventWithSeats({
      id: "event-1", title: "Concert", date: "2026-12-01T19:00:00Z", venue: "Hall", location: "City",
      category: "concerts", description: "", image_url: "", tiers: [],
    }, async () => { throw new Error("inventory unavailable"); });
    assert.equal(event.title, "Concert");
    assert.equal(event.venueLayout, undefined);
  } finally {
    console.warn = originalWarn;
  }
});

test("authoritative seat section availability excludes reserved inventory", () => {
  const { generateVenueLayoutFromSeats } = loadTypeScript("lib/api/transformers.ts");
  const tiers = [{ id: "tier-1", name: "Floor", price_cents: 2500, total_quantity: 3, available_quantity: 3, accounting_ready: true }];
  const makeSeat = (id, status, seatNumber) => ({
    id,
    event_id: "event-1",
    ticket_tier_id: "tier-1",
    status,
    position: { sectionId: "Floor-center", row: "A", seat: seatNumber, x: seatNumber * 20, y: 20 },
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    seat_revision: "1",
  });
  const layout = generateVenueLayoutFromSeats([
    makeSeat("available-seat", "available", 1),
    makeSeat("reserved-seat", "reserved", 2),
    makeSeat("booked-seat", "booked", 3),
  ], tiers, "concerts");
  assert.equal(layout.sections[0].availableSeats, 1);
});
