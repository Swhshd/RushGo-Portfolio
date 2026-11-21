import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

test("only the presentation discovery flag is allowlisted", () => {
  const { isAllowedAdminFeatureFlag } = loadTypeScript("lib/admin-feature-flags.ts");
  assert.equal(isAllowedAdminFeatureFlag("discovery_advanced_filters"), true);
  assert.equal(isAllowedAdminFeatureFlag("payments_enabled"), false);
  assert.equal(isAllowedAdminFeatureFlag("unknown"), false);
});

test("a version conflict reloads current state once and never repeats the toggle", async () => {
  const { ApiError } = loadTypeScript("lib/api/errors.ts");
  const { updateFeatureFlagWithConflictReload } = loadTypeScript("lib/admin-feature-flags.ts", { "./api/errors": { ApiError } });
  const calls = [];
  const current = { key: "discovery_advanced_filters", enabled: false, version: 8 };
  const latest = { flags: [{ ...current, enabled: true, version: 9 }], audit: [], audit_limit: 50, observed_at: "2026-11-12T00:00:00Z", source: "booking" };
  const result = await updateFeatureFlagWithConflictReload(
    current,
    async (...args) => { calls.push(["update", ...args]); throw new ApiError("REQUEST_ERROR", "stale version", 409); },
    async () => { calls.push(["reload"]); return latest; },
  );
  assert.deepEqual(result, { state: "conflict", latest });
  assert.deepEqual(calls, [
    ["update", "discovery_advanced_filters", true, 8],
    ["reload"],
  ]);
});
