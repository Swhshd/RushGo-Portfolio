import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

function fixture() {
  const tokens = new Map([["access_token", "admin-access-fixture"]]);
  globalThis.window = {};
  globalThis.localStorage = {
    getItem: key => tokens.get(key) || null,
    setItem: (key, value) => tokens.set(key, value),
    removeItem: key => tokens.delete(key),
  };
  return { ...loadTypeScript("lib/api/client.ts"), tokens };
}

test("admin flag client reads and changes an allowlisted flag with optimistic version", async () => {
  const originalFetch = globalThis.fetch;
  const f = fixture();
  const calls = [];
  globalThis.fetch = async (url, options) => {
    calls.push({ url, options });
    if (options.method === "PUT") return new Response(JSON.stringify({ key: "discovery_advanced_filters", enabled: true, version: 5 }));
    return new Response(JSON.stringify({ flags: [], audit: [], audit_limit: 50, source: "booking", observed_at: "2026-11-12T00:00:00Z" }));
  };
  try {
    await f.apiClient.getAdminFeatureFlags();
    const updated = await f.apiClient.updateAdminFeatureFlag("discovery_advanced_filters", true, 4);
    assert.equal(updated.version, 5);
    assert.equal(calls[0].url, "/api/admin/feature-flags");
    assert.equal(calls[0].options.headers.Authorization, "Bearer admin-access-fixture");
    assert.equal(calls[1].url, "/api/admin/feature-flags/discovery_advanced_filters");
    assert.equal(calls[1].options.method, "PUT");
    assert.deepEqual(JSON.parse(calls[1].options.body), { enabled: true, expected_version: 4 });
  } finally { globalThis.fetch = originalFetch; }
});

test("flag version conflict is returned without retry or overwriting the submitted version", async () => {
  const originalFetch = globalThis.fetch;
  const f = fixture();
  const calls = [];
  globalThis.fetch = async (url, options) => {
    calls.push({ url, options });
    return new Response(JSON.stringify({ code: "version_conflict", error: "Reload" }), { status: 409 });
  };
  try {
    await assert.rejects(f.apiClient.updateAdminFeatureFlag("discovery_advanced_filters", false, 7), error => error.status === 409);
    assert.equal(calls.length, 1);
    assert.deepEqual(JSON.parse(calls[0].options.body), { enabled: false, expected_version: 7 });
  } finally { globalThis.fetch = originalFetch; }
});

test("metrics JSON and CSV requests preserve the selected range and CSV download auth", async () => {
  const originalFetch = globalThis.fetch;
  const f = fixture();
  const calls = [];
  globalThis.fetch = async (url, options) => {
    calls.push({ url, options });
    if (new URL(url, "http://local").searchParams.get("format") === "csv") {
      return new Response("metric,value\norders,0\n", { headers: { "Content-Type": "text/csv", "Content-Disposition": 'attachment; filename="rushgo-operations.csv"' } });
    }
    return new Response(JSON.stringify({ from: "2026-11-01T00:00:00Z", to: "2026-11-08T00:00:00Z", booking: {}, payment: {} }));
  };
  try {
    const from = "2026-11-01T00:00:00.000Z";
    const to = "2026-11-08T00:00:00.000Z";
    await f.apiClient.getAdminMetrics(from, to);
    const csv = await f.apiClient.downloadAdminMetricsCsv(from, to);
    const query = new URL(calls[0].url, "http://local").searchParams;
    assert.equal(query.get("from"), from);
    assert.equal(query.get("to"), to);
    assert.equal(calls[1].options.headers.Authorization, "Bearer admin-access-fixture");
    const csvQuery = new URL(calls[1].url, "http://local").searchParams;
    assert.equal(csvQuery.get("format"), "csv");
    assert.equal(csvQuery.get("from"), from);
    assert.equal(csv.filename, "rushgo-operations.csv");
    assert.match(await csv.blob.text(), /orders,0/);
  } finally { globalThis.fetch = originalFetch; }
});
