import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { loadTypeScript } from "./render-typescript.mjs";
import { componentDriver, find } from "./recovery-driver.mjs";

// Catch session-expiry navigation destroying the reset component's only token.
// Keep the real provider, API client and reset form; control browser I/O/hooks.
for (const pathname of ["/reset-password", "/forgot-password", "/my-tickets"]) {
  test(`stale session restoration preserves recovery but redirects protected ${pathname}`, async () => {
    const tokens = new Map([["access_token", "expired-fixture"], ["refresh_token", "revoked-fixture"]]);
    const pushes = [], requests = [], slots = [], effects = [];
    let cursor = 0, hash = pathname === "/reset-password" ? "#token=synthetic-reset-token" : "";
    globalThis.localStorage = { getItem: key => tokens.get(key) || null, setItem: (key, value) => tokens.set(key, value), removeItem: key => tokens.delete(key) };
    globalThis.window = { location: { pathname, search: "", get hash() { return hash; } }, history: { replaceState() { hash = ""; } }, addEventListener() {}, removeEventListener() {} };
    const { apiClient } = loadTypeScript("lib/api/client.ts");
    globalThis.fetch = async (url, options) => {
      requests.push({ url, options });
      return new Response(JSON.stringify(url.endsWith("reset-password") ? { message: "Password updated" } : {}), { status: url.endsWith("reset-password") ? 200 : 401 });
    };
    const router = { push: href => pushes.push(href) };
    const hooks = { ...React, useState(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = typeof initial === "function" ? initial() : initial;
      return [slots[index], value => slots[index] = typeof value === "function" ? value(slots[index]) : value];
    }, useCallback: fn => fn, useEffect: fn => effects.push(fn) };
    const { AuthProvider } = loadTypeScript("lib/auth-context.tsx", { react: hooks, "./api/client": { apiClient }, "next/navigation": { useRouter: () => router } });
    const state = () => { cursor = 0; return AuthProvider({ children: null }).props.value; };
    let auth = state();
    const form = pathname === "/reset-password" ? componentDriver("app/(customer)/reset-password/reset-form.tsx", { "next/navigation": { useRouter: () => router }, "@/lib/auth-context": { useAuth: () => auth } }) : null;
    if (form) { form.render(); form.runEffects(); }
    effects.splice(0).forEach(fn => fn());
    await new Promise(resolve => setImmediate(resolve));
    auth = state();
    assert.equal(tokens.size, 0);
    assert.equal(auth.user, null);
    assert.equal(auth.isLoading, false);
    if (pathname === "/my-tickets") {
      assert.equal(new URL(pushes[0], "https://example.com").pathname, "/login");
    } else {
      assert.deepEqual(pushes, [], "stale credentials must not navigate away from recovery");
    }
    if (form) {
      assert.equal(hash, "");
      for (const id of ["reset-password", "reset-confirm"]) find(form.render(), node => node.props?.id === id).props.onChange({ target: { value: "synthetic long input" } });
      await find(form.render(), node => node.type === "form").props.onSubmit({ preventDefault() {} });
      const resets = requests.filter(request => request.url === "/api/auth/reset-password");
      assert.equal(resets.length, 1);
      assert.equal(JSON.parse(resets[0].options.body).token, "synthetic-reset-token");
      assert.deepEqual(pushes, ["/login?success=reset"]);
    }
  });
}
