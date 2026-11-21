import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

const drafts = () => loadTypeScript("lib/event-create-draft.ts");

function memoryStorage() {
  const values = new Map();
  return {
    getItem(key) { return values.get(key) ?? null; },
    setItem(key, value) { values.set(key, String(value)); },
    removeItem(key) { values.delete(key); },
  };
}

const payload = {
  title: "Night Show",
  description: "Live",
  category: "concerts",
  venue: "Hall",
  location: "City",
  date: "2026-11-13T12:00:00.000Z",
  image_url: "",
  publication_status: "draft",
  sales_status: "scheduled",
  sales_start_at: "",
  sales_end_at: "",
  tiers: [{ name: "Floor", price_cents: 5000, total_quantity: 50 }],
};

test("an unresolved event create reuses its saved identity and exact payload", () => {
  const { readPendingEventCreate, savePendingEventCreate } = drafts();
  const storage = memoryStorage();
  const first = savePendingEventCreate(payload, storage, "123e4567-e89b-12d3-a456-426614174000");
  const retry = readPendingEventCreate(storage);
  assert.deepEqual(retry, first);
  assert.equal(retry.idempotencyKey, "123e4567-e89b-12d3-a456-426614174000");
  assert.deepEqual(retry.payload, payload);
});

test("event create identity is cleared only after an authoritative success", () => {
  const { clearPendingEventCreate, readPendingEventCreate, savePendingEventCreate } = drafts();
  const storage = memoryStorage();
  savePendingEventCreate(payload, storage, "123e4567-e89b-12d3-a456-426614174000");
  clearPendingEventCreate(storage);
  assert.equal(readPendingEventCreate(storage), null);
});

test("malformed saved event create data is ignored and removed", () => {
  const { PENDING_EVENT_CREATE_KEY, readPendingEventCreate } = drafts();
  const storage = memoryStorage();
  storage.setItem(PENDING_EVENT_CREATE_KEY, JSON.stringify({ idempotencyKey: "bad", payload: { title: "partial" } }));
  assert.equal(readPendingEventCreate(storage), null);
  assert.equal(storage.getItem(PENDING_EVENT_CREATE_KEY), null);
});
