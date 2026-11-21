import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

function makeStorage() {
  const values = new Map();
  return {
    values,
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
    removeItem: (key) => values.delete(key),
  };
}

test("guest draft stores only the event, real seat and section identifiers, and a safe local return path", () => {
  const draft = loadTypeScript("lib/guest-seat-draft.ts");
  const storage = makeStorage();
  draft.saveGuestSeatDraft({
    eventId: "event-1",
    seats: [{ seatId: "seat-1", sectionId: "floor" }],
    returnTo: "https://evil.example/steal",
    paymentClientSecret: "must-not-persist",
    access_token: "must-not-persist",
  }, storage);

  const saved = JSON.parse([...storage.values.values()][0]);
  assert.deepEqual(Object.keys(saved).sort(), ["eventId", "returnTo", "seats", "version"]);
  assert.deepEqual(saved.seats, [{ seatId: "seat-1", sectionId: "floor" }]);
  assert.equal(saved.returnTo, "/events/event-1/seats");
  assert.doesNotMatch(JSON.stringify(saved), /must-not-persist|paymentClientSecret|access_token/);
});

test("draft restoration retains only selected seats still available in the actual layout", () => {
  const draft = loadTypeScript("lib/guest-seat-draft.ts");
  const layout = { sections: [{ id: "floor", name: "Floor", tier: "Standard", tierId: "tier-1", price: 25, rows: [{ label: "Row A", seats: [
    { id: "seat-1", label: "1", status: "available" },
    { id: "seat-2", label: "2", status: "reserved" },
  ] }] }] };
  const restored = draft.restoreGuestSeatSelection({
    version: 1,
    eventId: "event-1",
    returnTo: "/events/event-1/seats",
    seats: [{ seatId: "seat-1", sectionId: "floor" }, { seatId: "seat-2", sectionId: "floor" }, { seatId: "missing", sectionId: "floor" }],
  }, layout);
  assert.deepEqual(restored.seats.map((seat) => seat.seatId), ["seat-1"]);
  assert.deepEqual(restored.invalidSeatIds, ["seat-2", "missing"]);
  assert.equal(restored.seats[0].tierId, "tier-1");
  assert.equal(restored.seats[0].price, 25);
});