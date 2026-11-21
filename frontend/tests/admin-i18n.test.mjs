import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

function placeholders(value) {
  return [...value.matchAll(/\{([a-zA-Z][a-zA-Z0-9_]*)\}/g)]
    .map((match) => match[1])
    .sort();
}

test("Admin English and Chinese resources have matching keys and placeholders", () => {
  const { adminEn, adminZh } = loadTypeScript("lib/i18n/admin.ts");
  const englishKeys = Object.keys(adminEn).sort();

  assert.ok(englishKeys.length > 0, "Admin resources should not be empty");
  assert.deepEqual(Object.keys(adminZh).sort(), englishKeys);

  for (const key of englishKeys) {
    assert.ok(key.startsWith("admin."), `${key} should be namespaced`);
    assert.deepEqual(
      placeholders(adminZh[key]),
      placeholders(adminEn[key]),
      `${key} should preserve the same dynamic placeholders`,
    );
  }
});
