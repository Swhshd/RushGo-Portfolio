import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

const eventId = "00000000-0000-4000-8000-000000000001";

test("delivery exceptions preserve excess tickets without hiding the operations snapshot", () => {
  const { parseAdminEventTicketOperations } = loadTypeScript("lib/admin-event-operations.ts");
  const row = {
    booking_id: "00000000-0000-4000-8000-000000000002",
    state: "CONFIRMED_TICKET_DELIVERY_PENDING", booking_status: "CONFIRMED",
    expected_ticket_count: 1, recorded_ticket_count: 2,
    issuance_receipt_outcome: "CONFLICT", issuance_reason_code: "ticket_identity_conflict",
    booking_created_at: "2026-11-01T10:00:00Z",
  };
  const result = parseAdminEventTicketOperations(responseBody({ delivery_exceptions: [row] }), eventId);
  assert.equal(result.delivery_exceptions[0].recorded_ticket_count, 2);
  assert.equal(result.total_recorded_count, 4);
  const large = parseAdminEventTicketOperations(responseBody({ delivery_exceptions: [{ ...row, expected_ticket_count: 6000 }] }), eventId);
  assert.equal(large.delivery_exceptions[0].expected_ticket_count, 6000, "expected quantity is not the bounded database row count");
});

test("metadata limits match the Gateway Unicode code-point boundaries", () => {
  const { parseAdminEventTicketOperations } = loadTypeScript("lib/admin-event-operations.ts");
  const event_title = "🎟".repeat(240);
  const event_venue = "🎶".repeat(300);
  const parsed = parseAdminEventTicketOperations(responseBody({ event_title, event_venue }), eventId);
  assert.equal(parsed.event_title, event_title);
  assert.equal(parsed.event_venue, event_venue);
  for (const extra of [{ event_title: event_title + "🎟" }, { event_venue: event_venue + "🎶" }]) {
    assert.throws(() => parseAdminEventTicketOperations(responseBody(extra), eventId), /could not be verified/i);
  }
});

test("event-operations safe error helper supports both locales", () => {
  const { adminEventOperationsErrorMessage } = loadTypeScript("lib/admin-event-operations.ts");
  assert.equal(
    adminEventOperationsErrorMessage("capacity_exceeded", "zh-CN"),
    "已达到查询上限。活动运营数据超出安全读取容量。",
  );
  assert.match(adminEventOperationsErrorMessage("forbidden", "en-US"), /cannot view event operations/i);
});

function responseBody(overrides = {}) {
  return {
    event_id: eventId,
    event_title: "Midnight Arena",
    event_venue: "RushGo Arena",
    event_date: "2026-11-01T19:30:00Z",
    observed_at: "2026-11-01T12:00:00Z",
    source_service: "booking",
    source_status: "available",
    coverage: "recorded_ticket_snapshot",
    total_recorded_count: 4,
    issued_count: 2,
    checked_in_count: 1,
    revoked_count: 1,
    successful_checkin_count: 1,
    latest_checkin_at: "2026-11-01T11:55:00Z",
    recent_checkins: [{ checked_in_at: "2026-11-01T11:55:00Z" }],
    source_record_limit: 5000,
    live_admission_eligibility: "not_evaluated",
    ...overrides,
  };
}

function withBrowserFetch(fetchImpl, run) {
  const previous = {
    window: globalThis.window,
    localStorage: globalThis.localStorage,
    fetch: globalThis.fetch,
  };
  globalThis.window = {};
  globalThis.localStorage = {
    getItem: () => null,
    setItem() {},
    removeItem() {},
  };
  globalThis.fetch = fetchImpl;
  return Promise.resolve()
    .then(run)
    .finally(() => {
      for (const [key, value] of Object.entries(previous)) {
        if (value === undefined) delete globalThis[key];
        else globalThis[key] = value;
      }
    });
}

test("admin ticket operations reads a no-store safe DTO and discards upstream extras", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  let request;
  await withBrowserFetch(async (url, init) => {
    request = { url, init };
    return new Response(JSON.stringify(responseBody({
      upstream_debug: "PRIVATE_DO_NOT_RENDER",
      recent_checkins: [{ checked_in_at: "2026-11-01T11:55:00Z", ticket_id: "PRIVATE_TICKET_ID" }],
    })), { status: 200, headers: { "content-type": "application/json" } });
  }, async () => {
    const result = await apiClient.getAdminEventTicketOperations(eventId);
    assert.equal(request.url, `/api/admin/events/${eventId}/ticket-operations`);
    assert.equal(request.init.method, "GET");
    assert.equal(request.init.cache, "no-store");
    assert.equal(result.event_id, eventId);
    assert.equal("upstream_debug" in result, false);
    assert.deepEqual(result.recent_checkins, [{ checked_in_at: "2026-11-01T11:55:00Z" }]);
    assert.equal(JSON.stringify(result).includes("PRIVATE_TICKET_ID"), false);
  });
});

test("admin ticket operations rejects mismatched identity, count totals, and invalid timestamps", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  for (const invalid of [
    responseBody({ event_id: "00000000-0000-4000-8000-000000000002" }),
    responseBody({ total_recorded_count: 5 }),
    responseBody({ observed_at: "not-a-timestamp" }),
    responseBody({ recent_checkins: [{ checked_in_at: "2026-11-01T12:01:00Z" }] }),
    responseBody({ successful_checkin_count: 0, latest_checkin_at: null, recent_checkins: [] }),
    responseBody({ successful_checkin_count: 3 }),
    responseBody({ successful_checkin_count: 1, recent_checkins: [] }),
  ]) {
    await withBrowserFetch(async () => new Response(JSON.stringify(invalid), {
      status: 200,
      headers: { "content-type": "application/json" },
    }), async () => {
      await assert.rejects(
        apiClient.getAdminEventTicketOperations(eventId),
        (error) => error instanceof Error && !error.message.includes("PRIVATE"),
      );
    });
  }
});

test("a revoked ticket may retain a valid accepted check-in history", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  const history = [
    "2026-11-01T11:55:00Z",
    "2026-11-01T11:54:00Z",
    "2026-11-01T11:53:00Z",
  ];
  const body = responseBody({
    total_recorded_count: 4,
    issued_count: 1,
    checked_in_count: 1,
    revoked_count: 2,
    successful_checkin_count: 3,
    latest_checkin_at: history[0],
    recent_checkins: history.map((checked_in_at) => ({ checked_in_at })),
  });
  await withBrowserFetch(async () => new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
  }), async () => {
    const result = await apiClient.getAdminEventTicketOperations(eventId);
    assert.equal(result.successful_checkin_count, 3);
    assert.equal(result.revoked_count, 2);
  });
});

test("the nil UUID is rejected before a network request", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  let requested = false;
  await withBrowserFetch(async () => {
    requested = true;
    return new Response("{}", { status: 200 });
  }, async () => {
    await assert.rejects(apiClient.getAdminEventTicketOperations("00000000-0000-0000-0000-000000000000"), /request is invalid/i);
  });
  assert.equal(requested, false);
});

test("admin ticket operations maps authorization and capacity failures to fixed copy", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  const cases = [
    { status: 403, body: { code: "forbidden", error: "PRIVATE_AUTH_DETAIL" }, message: /cannot view event operations/i },
    { status: 503, body: { code: "capacity_exceeded", error: "PRIVATE_CAPACITY_DETAIL" }, message: /query limit reached/i },
  ];
  for (const item of cases) {
    await withBrowserFetch(async () => new Response(JSON.stringify(item.body), {
      status: item.status,
      headers: { "content-type": "application/json" },
    }), async () => {
      await assert.rejects(
        apiClient.getAdminEventTicketOperations(eventId),
        (error) => error instanceof Error && item.message.test(error.message) && !error.message.includes("PRIVATE"),
      );
    });
  }
});
