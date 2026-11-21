import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

test("decision retries retain payload, isolate operators, and permit rejecting a waiting approval", () => {
  const saved = { localStorage: globalThis.localStorage, sessionStorage: globalThis.sessionStorage };
  const values = new Map();
  const storage = { getItem: k => values.get(k) ?? null, setItem: (k, v) => values.set(k, v), removeItem: k => values.delete(k) };
  Object.assign(globalThis, { localStorage: storage, sessionStorage: storage });
  try {
    const { reserveRefundDecision: reserve, readRefundDecision: read, clearRefundDecision: clear } = loadTypeScript("lib/refund-decision-storage.ts");
    const first = reserve("alice", "event", "request", "APPROVE", "Original reason");
    assert.deepEqual(reserve("alice", "event", "request", "APPROVE", "Edited reason"), first);
    assert.equal(read("bob", "event", "request"), null);
    assert.notEqual(reserve("bob", "event", "request", "APPROVE", "Other operator").key, first.key);
    assert.throws(() => reserve("alice", "event", "request", "REJECT", "Not eligible"), /reconciled/);
    const rejection = reserve("alice", "event", "request", "REJECT", "Not eligible", true);
    assert.notEqual(rejection.key, first.key);
    assert.equal(rejection.reason, "Not eligible");
    assert.deepEqual(reserve("alice", "event", "request", "REJECT", "Changed again", true), rejection);
    clear("alice", "event", "request");
    assert.equal(read("alice", "event", "request"), null);
    assert.ok(read("bob", "event", "request"));
    values.set("rushgo.refund-decision.v2.alice.event.request", "{broken");
    assert.throws(() => reserve("alice", "event", "request", "APPROVE", "New"));
  } finally { Object.assign(globalThis, saved); }
});
