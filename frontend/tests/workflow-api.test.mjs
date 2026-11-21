import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

test("account, inbox, work, and operations calls follow the owner-scoped API contracts", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  const originalFetch = globalThis.fetch;
  const calls = [];
  const responses = [
    { id: "u1", email: "user@example.test", name: "User", role: "customer", locale: "zh", created_at: "2026-01-01T00:00:00Z" },
    { message: "changed", sign_in_required: true },
    { message: "sent", verification_id: "verification-id", expires_at: "2026-11-12T02:00:00Z", next_resend_at: "2026-11-12T01:05:00Z" },
    { message: "changed", sign_in_required: true },
    { notifications: [], total: 0, page: 2, page_size: 20, unread_count: 4 },
    { id: "notification-id", read_at: "2026-11-12T01:00:00Z" },
    { items: [], has_more: false, observed_at: "2026-11-12T01:00:00Z" },
    { items: [], has_more: false, notification_source_state: "unavailable", notification_items: [], notification_has_more: false, notification_observed_at: null },
  ];
  try {
    globalThis.fetch = async (url, options = {}) => {
      calls.push({ url, options });
      return new Response(JSON.stringify(responses.shift()), { status: 200 });
    };
    assert.equal((await apiClient.updateProfile({ name: "User", locale: "zh" })).locale, "zh");
    assert.equal((await apiClient.changePassword("current", "new-password")).sign_in_required, true);
    assert.equal((await apiClient.requestEmailChangeCode("current", "next@example.test")).verification_id, "verification-id");
    assert.equal((await apiClient.completeEmailChange("current", "verification-id", "123456")).sign_in_required, true);
    assert.equal((await apiClient.listMyNotifications(2, 20, true)).unread_count, 4);
    assert.equal((await apiClient.markNotificationRead("notification-id")).id, "notification-id");
    assert.equal((await apiClient.listMyWork(50, 100)).has_more, false);
    assert.equal((await apiClient.listAdminOperations({ from: "2026-11-05T00:00:00Z", to: "2026-11-12T00:00:00Z", limit: 50, offset: 100 })).notification_source_state, "unavailable");

    assert.equal(calls[0].url, "/api/users/me");
    assert.equal(calls[0].options.method, "PUT");
    assert.deepEqual(JSON.parse(calls[0].options.body), { name: "User", locale: "zh" });
    assert.equal(calls[1].url, "/api/users/me/password");
    assert.deepEqual(JSON.parse(calls[1].options.body), { current_password: "current", new_password: "new-password" });
    assert.equal(calls[2].url, "/api/users/me/email/code");
    assert.equal(calls[3].url, "/api/users/me/email/confirm");
    assert.equal(calls[4].url, "/api/users/me/notifications?page=2&page_size=20&unread_only=true");
    assert.equal(calls[5].url, "/api/users/me/notifications/notification-id/read");
    assert.equal(calls[5].options.method, "PATCH");
    assert.equal(calls[6].url, "/api/work?limit=50&offset=100");
    assert.equal(calls[7].url, "/api/admin/operations?from=2026-11-05T00%3A00%3A00Z&to=2026-11-12T00%3A00%3A00Z&limit=50&offset=100");
  } finally {
    globalThis.fetch = originalFetch;
  }
});
