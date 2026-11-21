import assert from "node:assert/strict";
import { createServer } from "node:http";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

test("real HTTP POST preserves the attempt header and payload after response loss", async (t) => {
  const requests = [];
  const server = createServer(async (req, res) => {
    let body = "";
    for await (const chunk of req) body += chunk;
    requests.push({ key: req.headers["idempotency-key"], body: JSON.parse(body) });
    if (requests.length === 1) { req.socket.destroy(); return; }
    res.writeHead(202, { "Content-Type": "application/json", "Retry-After": "1" });
    res.end(JSON.stringify({ id: "original-booking", creation_state: "starting", paymentIntentClientSecret: "", total_amount_cents: 725 }));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const nativeFetch = globalThis.fetch;
  const origin = `http://127.0.0.1:${server.address().port}`;
  t.mock.method(globalThis, "fetch", (url, options) => nativeFetch(origin + url, options));
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  const items = [{ ticket_tier_id: "tier", quantity: 1, seat_ids: ["seat"] }];
  await assert.rejects(apiClient.createBooking("event", items, "original-attempt"));
  const booking = await apiClient.createBooking("event", items, "original-attempt");
  assert.equal(booking.id, "original-booking");
  assert.equal(booking.creation_state, "starting");
  assert.deepEqual(requests, Array(2).fill({ key: "original-attempt", body: { event_id: "event", items } }));
});
