import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

const eventId = "00000000-0000-4000-8000-000000000001";
const ticketId = "00000000-0000-4000-8000-000000000002";
function body(extra = {}) {
  return { event_id: eventId, observed_at: "2026-11-01T12:00:00Z", source_service: "booking", source_status: "available", coverage: "recorded_ticket_snapshot", source_record_limit: 5000, status_filter: "ALL", page_size: 20, has_more: false, next_cursor: null, tickets: [{ ticket_id: ticketId, status: "ISSUED", issued_at: "2026-11-01T10:00:00Z", checked_in_at: null, revoked_at: null }], ...extra };
}
async function browser(fetcher, run) {
  const saved = { window: globalThis.window, localStorage: globalThis.localStorage, fetch: globalThis.fetch };
  Object.assign(globalThis, { window: {}, localStorage: { getItem: () => null, setItem() {}, removeItem() {} }, fetch: fetcher });
  try { await run(); } finally { Object.assign(globalThis, saved); }
}
test("safe ticket page uses bounded GET/no-store/signal and strips unexpected fields", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  const signal = new AbortController().signal;
  await browser(async (url, options) => {
    assert.equal(url, `/api/admin/events/${eventId}/tickets?status=ALL&page_size=20`);
    assert.equal(options.method, "GET"); assert.equal(options.cache, "no-store"); assert.equal(options.signal, signal);
    return Response.json(body({ secret: "PRIVATE", tickets: [{ ...body().tickets[0], digest: "PRIVATE", booking_id: "PRIVATE" }] }));
  }, async () => {
    const result = await apiClient.listAdminEventTickets(eventId, undefined, signal);
    assert.deepEqual(result, body()); assert.equal(JSON.stringify(result).includes("PRIVATE"), false);
  });
});
test("bad input is rejected before fetching, including scope, status, page bounds and cursor control characters", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  let calls = 0;
  await browser(async () => { calls++; return Response.json(body()); }, async () => {
    for (const [id, query] of [["bad", {}], [eventId, { status: "UNKNOWN" }], [eventId, { page_size: 0 }], [eventId, { page_size: 101 }], [eventId, { page_size: 1.5 }], [eventId, { cursor: "a\nb" }], [eventId, { cursor: "x".repeat(2049) }]]) {
      await assert.rejects(apiClient.listAdminEventTickets(id, query), error => error.code === "invalid_request");
    }
  });
  assert.equal(calls, 0);
});
test("page validation rejects unsafe status, wrong event/filter/size, duplicate IDs, invalid time, inconsistent cursor and chronology", () => {
  const { parseAdminEventTickets } = loadTypeScript("lib/admin-event-tickets.ts");
  for (const invalid of [body({ event_id: ticketId }), body({ status_filter: "REVOKED" }), body({ page_size: 100 }), body({ has_more: true }), body({ next_cursor: "unexpected" }), body({ tickets: [body().tickets[0], body().tickets[0]] }), body({ tickets: [{ ...body().tickets[0], status: "UNKNOWN" }] }), body({ tickets: [{ ...body().tickets[0], issued_at: "2026-02-30T10:00:00Z" }] }), body({ tickets: [{ ...body().tickets[0], checked_in_at: "2026-11-01T09:00:00Z" }] })]) {
    assert.throws(() => parseAdminEventTickets(invalid, eventId, { status: "ALL", page_size: 20 }), error => error.code === "invalid_response");
  }
});
test("capacity, source faults and hostile errors keep semantic codes and hide raw details", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  for (const [status, code, expected] of [[503, "capacity_exceeded", "capacity_exceeded"], [503, "source_unavailable", "unavailable"], [403, "forbidden", "forbidden"], [404, "not_found", "not_found"]]) {
    await browser(async () => Response.json({ code, error: "PRIVATE_BACKEND_SQL" }, { status }), async () => {
      await assert.rejects(apiClient.listAdminEventTickets(eventId), error => error.code === expected && !error.message.includes("PRIVATE"));
    });
  }
});
