import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

test("Booking Outbox client issues the bounded private GET with validated filters and cancellation", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  assert.equal(
    typeof apiClient.listAdminOutboxEvents,
    "function",
    "the API client should expose the admin Outbox read operation",
  );

  const calls = [];
  const originalFetch = globalThis.fetch;
  const controller = new AbortController();
  const expected = {
    source_service: "booking",
    records: [],
    next_cursor: "next-fixture",
    has_more: true,
    observed_at: "2026-10-31T16:00:00Z",
    window: { from: "2026-10-30T16:00:00Z", to: "2026-10-31T16:00:00Z" },
    supported_event_types: ["BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"],
    source_record_limit: 5000,
    consumer_outcome: "not_verified",
  };

  try {
    globalThis.fetch = async (url, options = {}) => {
      calls.push({ url, options });
      return { ok: true, status: 200, json: async () => expected };
    };

    const result = await apiClient.listAdminOutboxEvents(
      {
        status: "pending",
        event_type: "BookingConfirmed",
        from: "2026-10-30T16:00:00Z",
        to: "2026-10-31T16:00:00Z",
        limit: 20,
        cursor: "opaque-cursor-fixture",
      },
      controller.signal,
    );

    assert.equal(calls.length, 1);
    assert.equal(calls[0].url.split("?")[0], "/api/admin/outbox-events");
    const query = new URLSearchParams(calls[0].url.split("?")[1]);
    assert.equal(query.get("source"), "booking");
    assert.equal(query.get("status"), "pending");
    assert.equal(query.get("event_type"), "BookingConfirmed");
    assert.equal(query.get("from"), "2026-10-30T16:00:00Z");
    assert.equal(query.get("to"), "2026-10-31T16:00:00Z");
    assert.equal(query.get("limit"), "20");
    assert.equal(query.get("cursor"), "opaque-cursor-fixture");
    assert.equal(calls[0].options.method ?? "GET", "GET");
    assert.equal(calls[0].options.body, undefined);
    assert.equal(calls[0].options.cache, "no-store");
    assert.equal(calls[0].options.signal, controller.signal);
    assert.deepEqual(result, expected);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("Overview source clients forward cancellation and keep scheduler reads out of cache", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  const calls = [];
  const originalFetch = globalThis.fetch;
  const controller = new AbortController();

  try {
    globalThis.fetch = async (url, options = {}) => {
      calls.push({ url, options });
      return {
        ok: true,
        status: 200,
        json: async () => url === "/api/events?page=1&page_size=1"
          ? { events: [], total: 0, page: 1, page_size: 1 }
          : { schedulers: [] },
      };
    };

    await apiClient.listEventsPage({ page: 1, page_size: 1 }, controller.signal);
    await apiClient.listSchedulerJobs(controller.signal);

    assert.equal(calls.length, 2);
    assert.equal(calls[0].options.signal, controller.signal);
    assert.equal(calls[1].options.signal, controller.signal);
    assert.equal(calls[1].options.cache, "no-store");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("Booking Outbox capacity errors use a typed code only for the Outbox endpoint", () => {
  const { responseError } = loadTypeScript("lib/api/errors.ts");

  for (const endpoint of ["/admin/outbox-events", "/admin/outbox-events?source=booking"]) {
    for (const backendCode of ["capacity_exceeded", "outbox_capacity_exceeded"]) {
      const error = responseError(
        endpoint,
        503,
        "private backend detail fixture",
        backendCode,
      );
      assert.equal(error.code, "OUTBOX_CAPACITY_EXCEEDED");
      assert.equal(error.status, 503);
    }
  }

  assert.equal(
    responseError("/events", 503, "private backend detail fixture", "capacity_exceeded").code,
    "REQUEST_ERROR",
    "the source-specific capacity code must not change unrelated endpoint errors",
  );
});

test("Notification Outbox client sends source-specific filters and preserves Booking as the default", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  const calls = [];
  const originalFetch = globalThis.fetch;
  const notification = {
    source_service: "notification",
    records: [],
    next_cursor: "notification-next",
    has_more: true,
    observed_at: "2026-10-31T16:00:00Z",
    window: { from: "2026-10-30T16:00:00Z", to: "2026-10-31T16:00:00Z" },
    supported_event_types: ["booking_confirmation", "booking_failure", "booking_cancellation", "welcome_email"],
    source_record_limit: 5000,
    consumer_outcome: "not_verified",
  };

  try {
    globalThis.fetch = async (url, options = {}) => {
      calls.push({ url, options });
      return { ok: true, status: 200, json: async () => notification };
    };

    const result = await apiClient.listAdminOutboxEvents({
      source: "notification",
      status: "sent",
      event_type: "welcome_email",
      from: "2026-10-30T16:00:00Z",
      to: "2026-10-31T16:00:00Z",
      limit: 20,
      cursor: "notification-only-cursor",
    });

    assert.equal(calls.length, 1);
    const query = new URLSearchParams(calls[0].url.split("?")[1]);
    assert.equal(query.get("source"), "notification");
    assert.equal(query.get("status"), "sent");
    assert.equal(query.get("event_type"), "welcome_email");
    assert.equal(query.get("cursor"), "notification-only-cursor");
    assert.equal(calls[0].options.cache, "no-store");
    assert.equal(calls[0].options.body, undefined);
    assert.deepEqual(result, notification);

    await apiClient.listAdminOutboxEvents({
      status: "pending",
      from: "2026-10-30T16:00:00Z",
      to: "2026-10-31T16:00:00Z",
      limit: 20,
    });
    assert.equal(new URLSearchParams(calls[1].url.split("?")[1]).get("source"), "booking");
  } finally {
    globalThis.fetch = originalFetch;
  }
});
