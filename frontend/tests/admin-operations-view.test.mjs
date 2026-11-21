import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

const view = () => loadTypeScript("lib/admin-operations-view.ts");

test("notification source unavailability is not normalized into an empty result", () => {
  const { notificationSourceSnapshot } = view();
  const unavailable = notificationSourceSnapshot({
    notification_source_state: "unavailable",
    notification_items: [],
    notification_has_more: false,
    notification_observed_at: null,
  });
  assert.equal(unavailable.state, "unavailable");
  assert.equal(unavailable.items, null);

  const availableEmpty = notificationSourceSnapshot({
    notification_source_state: "available",
    notification_items: [],
    notification_has_more: false,
    notification_observed_at: "2026-11-12T00:00:00Z",
  });
  assert.equal(availableEmpty.state, "available");
  assert.deepEqual(availableEmpty.items, []);
});
