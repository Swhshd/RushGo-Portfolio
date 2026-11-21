import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

function fixture(initial = {}) {
  const tokens = new Map(Object.entries(initial)), calls = [];
  globalThis.window = {};
  globalThis.localStorage = { getItem: k => tokens.get(k) || null, setItem: (k,v) => tokens.set(k,v), removeItem: k => tokens.delete(k) };
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  const respond = responses => { globalThis.fetch = async (url, options) => { calls.push({ url, options }); const response = responses.shift(); return typeof response === "function" ? response() : new Response(JSON.stringify(response.body), { status: response.status }); }; };
  return { apiClient, tokens, calls, respond };
}
test("missing credentials 401 is AUTH_REQUIRED, not payment failure", async () => {
  const f = fixture(); f.respond([{ status: 401, body: { error: "missing authorization header" } }]);
  await assert.rejects(f.apiClient.createBooking("event", []), e => e.code === "AUTH_REQUIRED" && e.status === 401);
});
test("unrecoverable authenticated 401 clears credentials and notifies AuthContext", async () => {
  const f = fixture({ access_token: "invalid-fixture", refresh_token: "invalid-refresh-fixture" }); let expired = 0;
  f.apiClient.subscribeSessionExpired?.(() => expired++);
  f.respond([{ status: 401, body: { error: "invalid token" } }, { status: 401, body: { error: "token expired or invalid" } }]);
  await assert.rejects(f.apiClient.createBooking("event", []), e => e.code === "SESSION_EXPIRED");
  assert.equal(f.tokens.size, 0); assert.equal(expired, 1);
});
test("successful refresh retries the booking and retains a valid session", async () => {
  const f = fixture({ access_token: "expired-fixture", refresh_token: "valid-refresh-fixture" });
  f.respond([{ status: 401, body: { error: "invalid token" } }, { status: 200, body: { access_token: "new-fixture", refresh_token: "rotated-fixture" } }, { status: 201, body: { id: "booking" } }]);
  assert.equal((await f.apiClient.createBooking("event", [])).id, "booking");
  assert.equal(f.calls[2].options.headers.Authorization, "Bearer new-fixture"); assert.equal(f.tokens.get("access_token"), "new-fixture");
});
test("a second 401 after successful refresh also clears auth immediately", async () => {
  const f = fixture({ access_token: "expired-fixture", refresh_token: "valid-refresh-fixture" });
  f.respond([{ status: 401, body: {} }, { status: 200, body: { access_token: "new-fixture", refresh_token: "rotated-fixture" } }, { status: 401, body: { error: "invalid token" } }]);
  await assert.rejects(f.apiClient.createBooking("event", []), e => e.code === "SESSION_EXPIRED"); assert.equal(f.tokens.size, 0);
});
test("concurrent expired protected reads share one rotating refresh", async () => {
  const f = fixture({ access_token: "expired-fixture", refresh_token: "valid-refresh-fixture" }); let refreshes = 0;
  globalThis.fetch = async (url, options) => {
    if (url === "/api/auth/refresh") { refreshes++; await new Promise(resolve => setImmediate(resolve)); return new Response(JSON.stringify({ access_token: "new-fixture", refresh_token: "rotated-fixture" })); }
    if (options.headers.Authorization === "Bearer expired-fixture") return new Response("{}", { status: 401 });
    return new Response(JSON.stringify({ id: "user", bookings: [] }));
  };
  await Promise.all([f.apiClient.getProfile(), f.apiClient.listBookings()]);
  assert.equal(refreshes, 1); assert.equal(f.tokens.get("access_token"), "new-fixture");
});
test("401 without credentials also reconciles a stale AuthContext user", async () => {
  const f = fixture(); let invalidated = 0; f.apiClient.subscribeSessionExpired(() => invalidated++);
  f.respond([{ status: 401, body: { error: "missing authorization header" } }]);
  await assert.rejects(f.apiClient.getProfile(), e => e.code === "AUTH_REQUIRED");
  assert.equal(invalidated, 1);
});
test("an external storage session change invalidates in-flight API work without changing credentials", async () => {
  const f = fixture({ access_token: "tab-a", refresh_token: "refresh-a" });
  const before = f.apiClient.getSessionVersion();
  f.apiClient.notifyExternalSessionChange();
  assert.equal(f.apiClient.getSessionVersion(), before + 1);
  assert.equal(f.tokens.get("access_token"), "tab-a");
  assert.equal(f.tokens.get("refresh_token"), "refresh-a");
});
test("an older session's delayed retry 401 cannot clear a newer successful login", async () => {
  const f = fixture({ access_token: "expired-fixture", refresh_token: "old-refresh-fixture" });
  let release, entered; const retryStarted = new Promise(resolve => { entered = resolve; }); let expired = 0;
  f.apiClient.subscribeSessionExpired(() => expired++);
  globalThis.fetch = async (url, options) => {
    if (url === "/api/auth/login") return new Response(JSON.stringify({ access_token: "new-login-fixture", refresh_token: "new-login-refresh-fixture" }));
    if (url === "/api/auth/refresh") return new Response(JSON.stringify({ access_token: "old-refreshed-fixture", refresh_token: "old-rotated-fixture" }));
    if (options.headers.Authorization === "Bearer expired-fixture") return new Response("{}", { status: 401 });
    entered(); return new Promise(resolve => { release = () => resolve(new Response("{}", { status: 401 })); });
  };
  const oldRead = f.apiClient.getProfile().catch(error => error);
  await retryStarted; await f.apiClient.login("demo@example.com", "fixture"); release(); await oldRead;
  assert.equal(f.tokens.get("access_token"), "new-login-fixture");
  assert.equal(f.tokens.get("refresh_token"), "new-login-refresh-fixture"); assert.equal(expired, 0);
});
test("a delayed refresh result cannot overwrite a newer login", async () => {
  const f = fixture({ access_token: "expired-fixture", refresh_token: "old-refresh-fixture" });
  let release, entered; const refreshStarted = new Promise(resolve => { entered = resolve; });
  globalThis.fetch = async url => {
    if (url === "/api/auth/login") return new Response(JSON.stringify({ access_token: "new-login-fixture", refresh_token: "new-login-refresh-fixture" }));
    if (url === "/api/auth/refresh") { entered(); return new Promise(resolve => { release = () => resolve(new Response(JSON.stringify({ access_token: "old-refreshed-fixture", refresh_token: "old-rotated-fixture" }))); }); }
    return new Response("{}", { status: 401 });
  };
  const oldRead = f.apiClient.getProfile().catch(error => error);
  await refreshStarted; await f.apiClient.login("demo@example.com", "fixture"); release(); await oldRead;
  assert.equal(f.tokens.get("access_token"), "new-login-fixture"); assert.equal(f.tokens.get("refresh_token"), "new-login-refresh-fixture");
});
test("public catalog 200 remains authoritative when profile expiry clears the session", async () => {
  const f = fixture({ access_token: "invalid-fixture" }); let release;
  globalThis.fetch = async url => {
    if (url.startsWith("/api/events")) return new Promise(resolve => { release = () => resolve(new Response(JSON.stringify({ events: [{ id: "return-event" }], total: 1 }))); });
    return new Response(JSON.stringify({ error: "invalid token" }), { status: 401 });
  };
  const catalog = f.apiClient.listEvents();
  await assert.rejects(f.apiClient.getProfile(), e => e.code === "SESSION_EXPIRED"); release();
  assert.deepEqual((await catalog).events, [{ id: "return-event" }]); assert.equal(f.tokens.size, 0);
});
for (const [status, error, code] of [[409,"seat is no longer available","BOOKING_CONFLICT"],[500,"booking failed rpc error: seat admission conflict","BOOKING_CONFLICT"],[500,"database unavailable","BOOKING_ERROR"],[500,"create payment intent failed","PAYMENT_INIT_ERROR"],[403,"forbidden","BOOKING_ERROR"]]) {
  test(`booking ${status} ${error} is classified ${code}`, async () => {
    const f = fixture({ access_token: "valid-fixture" }); f.respond([{ status, body: { error } }]);
    await assert.rejects(f.apiClient.createBooking("event", []), e => e.code === code && e.status === status);
  });
}
test("an accounting refusal is not reported as a seat conflict", async () => {
  const f = fixture({ access_token: "valid-fixture" });
  f.respond([{ status: 409, body: { code: "accounting_not_ready", error: "ticket accounting is not ready for sale" } }]);
  await assert.rejects(f.apiClient.createBooking("event", []), e => e.code === "ACCOUNTING_NOT_READY" && e.status === 409);
});
test("an idempotency-key 409 still reports a booking conflict", async () => {
  const f = fixture({ access_token: "valid-fixture" });
  f.respond([{ status: 409, body: { code: "booking_request_conflict", error: "request key has a different booking payload" } }]);
  await assert.rejects(f.apiClient.createBooking("event", []), e => e.code === "BOOKING_CONFLICT" && e.status === 409);
});
