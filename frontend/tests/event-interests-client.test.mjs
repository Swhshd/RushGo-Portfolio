import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

function fixture() {
  const tokens = new Map([["access_token", "buyer-fixture"]]);
  globalThis.window = {};
  globalThis.localStorage = { getItem: key => tokens.get(key) ?? null, setItem: (key, value) => tokens.set(key, value), removeItem: key => tokens.delete(key) };
  const client = loadTypeScript("lib/api/client.ts");
  const interests = loadTypeScript("lib/api/event-interests.ts", { "./client": client });
  return { ...client, ...interests, tokens };
}

test("reminder email consent is explicit and withdrawal uses the authenticated user's endpoint", async () => {
  const original = globalThis.fetch;
  const f = fixture();
  const calls = [];
  globalThis.fetch = async (url, options) => { calls.push({ url, options }); return new Response("{}"); };
  try {
    await f.eventInterestsApi.subscribe("event-fixture", "onsale", false);
    await f.eventInterestsApi.subscribe("event-fixture", "onsale", true);
    await f.eventInterestsApi.subscribe("event-fixture", "onsale", false);
    await f.eventInterestsApi.unsubscribe("event-fixture", "onsale");
    assert.deepEqual(calls.slice(0, 3).map(c => JSON.parse(c.options.body)), [
      { emailOptIn: false, consentVersion: "" },
      { emailOptIn: true, consentVersion: "event-alerts-v1" },
      { emailOptIn: false, consentVersion: "" },
    ]);
    assert.equal(calls[3].options.method, "DELETE");
    for (const { url, options } of calls) {
      assert.equal(url, "/api/users/me/events/event-fixture/alerts/onsale");
      assert.equal(options.headers.Authorization, "Bearer buyer-fixture");
      assert.equal(options.cache, "no-store");
      assert.ok(!options.body?.includes("userId"));
    }
  } finally { globalThis.fetch = original; }
});

test("an interests response from an old session cannot complete after logout", async () => {
  const original = globalThis.fetch;
  const f = fixture();
  let resolve;
  globalThis.fetch = () => new Promise(r => { resolve = r; });
  try {
    const pending = f.eventInterestsApi.state("event-fixture");
    f.apiClient.clearLocalSession();
    resolve(new Response(JSON.stringify({ eventId: "event-fixture", favorite: true })));
    await assert.rejects(pending, error => error.code === "REQUEST_ERROR");
  } finally { globalThis.fetch = original; }
});

test("an on-sale state conflict is surfaced without an automatic mutation retry", async () => {
  const original = globalThis.fetch;
  const f = fixture();
  let calls = 0;
  globalThis.fetch = async () => { calls++; return new Response(JSON.stringify({ code: "already_on_sale", error: "Changed" }), { status: 409 }); };
  try {
    await assert.rejects(f.eventInterestsApi.subscribe("event-fixture", "onsale", false), error => error.status === 409);
    assert.equal(calls, 1);
  } finally { globalThis.fetch = original; }
});
