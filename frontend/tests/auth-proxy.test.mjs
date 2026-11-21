import assert from "node:assert/strict";
import { test } from "node:test";
import http from "node:http";
import { loadTypeScript } from "./render-typescript.mjs";

const { proxyAuthRequest } = loadTypeScript("lib/auth-proxy.ts");
async function fixture(t, respond) {
  const server = http.createServer(respond);
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  return `http://127.0.0.1:${server.address().port}`;
}
function request(body = "{}", headers = {}) {
  return new Request("http://frontend.test/api/auth/login?token=synthetic-query-canary", {
    method: "POST", headers: { "content-type": "application/json", ...headers }, body,
  });
}
test("auth upstream success and rejection remain uncached without changing response or credentials", async t => {
  let reached = 0;
  const origin = await fixture(t, (req, res) => {
    assert.equal(req.url, "/api/auth/logout");
    assert.equal(req.headers.authorization, "Bearer synthetic-session");
    assert.equal(req.headers.baggage, undefined);
    assert.equal(req.headers["x-forwarded-for"], undefined);
    let body = "";
    req.on("data", chunk => { body += chunk; });
    req.on("end", () => {
      assert.equal(body, '{"refresh_token":"synthetic-refresh"}');
      res.writeHead(++reached === 1 ? 200 : 429, { "Content-Type": "application/json", "Retry-After": "60", "Cache-Control": "public, max-age=100" });
      res.end('{"message":"safe"}');
    });
  });
  for (const status of [200, 429]) {
    const response = await proxyAuthRequest(request('{"refresh_token":"synthetic-refresh"}', { authorization: "Bearer synthetic-session", baggage: "secret=synthetic-canary", "x-forwarded-for": "192.0.2.7" }), "logout", origin);
    assert.equal(response.status, status);
    assert.equal(response.headers.get("cache-control"), "no-store");
    assert.equal(response.headers.get("retry-after"), "60");
    assert.equal(await response.text(), '{"message":"safe"}');
  }
});
test("auth transport failure is a safe uncached 503", async t => {
  const origin = await fixture(t, (_req, res) => res.destroy());
  const response = await proxyAuthRequest(request(), "login", origin);
  assert.equal(response.status, 503);
  assert.equal(response.headers.get("cache-control"), "no-store");
  assert.deepEqual(await response.json(), { code: "TEMPORARY_UNAVAILABLE", error: "Temporarily unavailable. Please try again." });
});
test("public auth never forwards another session's Authorization header", async t => {
  const origin = await fixture(t, (req, res) => {
    assert.equal(req.headers.authorization, undefined);
    res.end("{}");
  });
  assert.equal((await proxyAuthRequest(request("{}", { authorization: "Bearer synthetic-session" }), "forgot-password", origin)).status, 200);
});
test("oversized auth input and unknown operations are rejected before reaching Gateway", async t => {
  let reached = false;
  const origin = await fixture(t, (_req, res) => { reached = true; res.end("{}"); });
  assert.equal((await proxyAuthRequest(request("x".repeat(16 * 1024 + 1)), "login", origin)).status, 413);
  assert.equal((await proxyAuthRequest(request(), "unknown", origin)).status, 404);
  assert.equal(reached, false);
});
