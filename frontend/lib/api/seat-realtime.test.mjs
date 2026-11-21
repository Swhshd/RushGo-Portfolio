import assert from "node:assert/strict";
import { test } from "node:test";

import {
  applySeatUpdates,
  canContinueWithSeatInventory,
  compareSeatRevision,
  createSeatRealtimeSession,
  invalidateUnavailableSelections,
  applyUpdatesToVenueLayout,
  reconcileSeatSnapshot,
} from "./seat-realtime.mjs";

const seat = (id, status, seatRevision, updatedAt = "2026-01-01T00:00:00Z") => ({
  id,
  status,
  seatRevision,
  updatedAt,
});

const update = (seatId, status, seatRevision, updatedAt = "2025-01-01T00:00:00Z") => ({
  seat_id: seatId,
  ticket_tier_id: "tier-1",
  status,
  seat_revision: seatRevision,
  updated_at: updatedAt,
});

test("revision comparisons remain exact above JavaScript's safe integer range", () => {
  assert.equal(compareSeatRevision("9007199254740993", "9007199254740992"), 1);
});

test("newer delta overrides older snapshot despite an older updated_at", () => {
  const result = reconcileSeatSnapshot([seat("a", "available", "5")], [
    { message_id: "m1", seat_updates: [update("a", "reserved", "6")] },
  ]);
  assert.equal(result.seats[0].status, "reserved");
  assert.equal(result.seats[0].seatRevision, "6");
});

test("older buffered delta cannot override newer snapshot despite newer updated_at", () => {
  const result = reconcileSeatSnapshot([seat("a", "booked", "6")], [
    { message_id: "m1", seat_updates: [update("a", "reserved", "5", "2027-01-01T00:00:00Z")] },
  ]);
  assert.equal(result.seats[0].status, "booked");
  assert.equal(result.seats[0].seatRevision, "6");
});

test("equal revision and equal status is a duplicate no-op", () => {
  const result = applySeatUpdates([seat("a", "booked", "6")], [update("a", "booked", "6")]);
  assert.equal(result.changed, false);
  assert.equal(result.conflict, false);
});

test("equal revision and different status requires resync rather than guessing", () => {
  const result = applySeatUpdates([seat("a", "booked", "6")], [update("a", "reserved", "6")]);
  assert.equal(result.changed, false);
  assert.equal(result.conflict, true);
  assert.equal(result.seats[0].status, "booked");
});

test("snapshot reconciliation invalidates selected seats that are no longer available", () => {
  const selected = [{ seatId: "a" }, { seatId: "b" }, { seatId: "missing" }];
  const result = invalidateUnavailableSelections(selected, [
    seat("a", "reserved", "3"),
    seat("b", "available", "2"),
  ]);
  assert.deepEqual(result.remaining.map((item) => item.seatId), ["b"]);
  assert.deepEqual(result.invalidated.map((item) => item.seatId), ["a", "missing"]);
});

test("reservation deltas preserve checkout selections until the booking request resolves", () => {
  const selected = [{ seatId: "a" }];
  const reserved = [seat("a", "reserved", "3")];
  const duringRequest = invalidateUnavailableSelections(selected, reserved, true);
  assert.deepEqual(duringRequest.remaining, selected);
  assert.deepEqual(duringRequest.invalidated, []);

  // A failed attempt must still reconcile the latest state after the request.
  const afterFailure = invalidateUnavailableSelections(selected, reserved, false);
  assert.deepEqual(afterFailure.remaining, []);
  assert.deepEqual(afterFailure.invalidated, selected);
});

class FakeEventSource {
  listeners = new Map();
  closed = false;
  onerror = null;

  addEventListener(type, listener) {
    this.listeners.set(type, listener);
  }

  emit(type, event = {}) {
    this.listeners.get(type)?.(event);
  }

  close() {
    this.closed = true;
  }
}

test("snapshot starts only after ready and buffered delta reconciles", async () => {
  const source = new FakeEventSource();
  const states = [];
  let finishSnapshot;
  let snapshotCalls = 0;
  const session = createSeatRealtimeSession({
    createSource: () => source,
    fetchSnapshot: () => {
      snapshotCalls++;
      return new Promise((resolve) => { finishSnapshot = resolve; });
    },
    onState: (state) => states.push(state),
  });

  assert.equal(snapshotCalls, 0);
  source.emit("ready");
  assert.equal(snapshotCalls, 1);
  source.emit("seat-update", { data: JSON.stringify({ message_id: "r1", seat_updates: [update("a", "reserved", "6")] }) });
  finishSnapshot({ seats: [seat("a", "available", "5")] });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(session.getState().phase, "LIVE");
  assert.equal(session.getState().seats[0].seatRevision, "6");
  assert.equal(states.some((state) => state.phase === "STREAM_READY"), true);
  session.close();
});

test("stale prior generation snapshot is ignored and reconnect starts one fresh fetch", async () => {
  const source = new FakeEventSource();
  const pending = [];
  let snapshotCalls = 0;
  const session = createSeatRealtimeSession({
    createSource: () => source,
    fetchSnapshot: () => {
      snapshotCalls++;
      return new Promise((resolve) => pending.push(resolve));
    },
    onState: () => {},
  });
  source.emit("ready");
  source.onerror();
  source.emit("ready");
  assert.equal(snapshotCalls, 2);
  pending[0]({ seats: [seat("a", "available", "2")] });
  pending[1]({ seats: [seat("a", "booked", "4")] });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(session.getState().seats[0].status, "booked");
  session.close();
});

test("closing session closes its EventSource", () => {
  const source = new FakeEventSource();
  const session = createSeatRealtimeSession({
    createSource: () => source,
    fetchSnapshot: async () => ({ seats: [] }),
    onState: () => {},
  });
  session.close();
  assert.equal(source.closed, true);
});

test("section availability counts only seats whose authoritative status is available", () => {
  const layout = { sections: [{ id: "floor", availableSeats: 3, rows: [{ seats: [
    seat("a", "available", "1"),
    seat("b", "reserved", "1"),
    seat("c", "booked", "1"),
  ] }] }] };
  const result = applyUpdatesToVenueLayout(layout, [update("a", "booked", "2")]);
  assert.equal(result.layout.sections[0].availableSeats, 0);
});

test("a stream disconnect invalidates Continue authority while retaining the last seat snapshot", async () => {
  const source = new FakeEventSource();
  const states = [];
  const session = createSeatRealtimeSession({
    createSource: () => source,
    fetchSnapshot: async () => ({ seats: [seat("a", "available", "1")] }),
    onState: (state) => states.push(state),
  });
  source.emit("ready");
  await new Promise((resolve) => setImmediate(resolve));
  source.onerror();
  const latest = states.at(-1);
  assert.equal(latest.phase, "CONNECTING");
  assert.deepEqual(latest.seats, [seat("a", "available", "1")]);
  assert.equal(canContinueWithSeatInventory(latest.phase), false);
  assert.equal(canContinueWithSeatInventory("LIVE"), true);
  session.close();
});

test("a malformed delta blocks Continue and starts a new authoritative snapshot", async () => {
  const source = new FakeEventSource();
  const states = [];
  let snapshots = 0;
  const session = createSeatRealtimeSession({
    createSource: () => source,
    fetchSnapshot: async () => {
      snapshots++;
      return { seats: [seat("a", "available", "1")] };
    },
    onState: (state) => states.push(state),
  });
  source.emit("ready");
  await new Promise((resolve) => setImmediate(resolve));
  source.emit("seat-update", { data: "{}" });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(snapshots, 2);
  assert.equal(states.some((state) => state.phase === "RESYNC_REQUIRED"), true);
  assert.equal(session.getState().phase, "LIVE");
  session.close();
});
