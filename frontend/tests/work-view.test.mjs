import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { loadTypeScript } from "./render-typescript.mjs";

const EVENT_ID = "00000000-0000-4000-8000-000000000001";
const Icon = () => null;
const { getI18n } = loadTypeScript("lib/i18n/core.ts");

function sameDependencies(left, right) {
  return Array.isArray(left) && Array.isArray(right) && left.length === right.length && left.every((value, index) => Object.is(value, right[index]));
}

function createPageDriver(responses) {
  const slots = [];
  const pendingEffects = [];
  const activeEffects = new Map();
  let cursor = 0;
  const user = { id: "staff-user" };
  const hooks = {
    ...React,
    useState(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = typeof initial === "function" ? initial() : initial;
      return [slots[index], (value) => { slots[index] = typeof value === "function" ? value(slots[index]) : value; }];
    },
    useMemo(factory, dependencies) {
      const index = cursor++;
      const current = slots[index];
      if (!current || !sameDependencies(current.dependencies, dependencies)) slots[index] = { value: factory(), dependencies };
      return slots[index].value;
    },
    useCallback(callback, dependencies) {
      return hooks.useMemo(() => callback, dependencies);
    },
    useEffect(setup, dependencies) {
      const index = cursor++;
      const current = slots[index];
      if (!current || !sameDependencies(current.dependencies, dependencies)) {
        slots[index] = { dependencies };
        pendingEffects.push({ index, setup });
      }
    },
  };
  let calls = 0;
  const { default: Page } = loadTypeScript("app/(customer)/work/page.tsx", {
    react: hooks,
    "next/link": { __esModule: true, default: "a" },
    "next/navigation": { useRouter: () => ({ replace() {} }) },
    "lucide-react": { AlertCircle: Icon, CalendarDays: Icon, Loader2: Icon, RefreshCw: Icon },
    "@/lib/auth-context": { useAuth: () => ({ user, isLoading: false }) },
    "@/lib/api/client": { apiClient: { listMyWork: async () => responses[calls++] } },
    "@/lib/i18n": { useI18n: () => getI18n("en-US") },
  });

  return {
    render() {
      cursor = 0;
      return Page();
    },
    runEffects() {
      for (const effect of pendingEffects.splice(0)) {
        activeEffects.get(effect.index)?.cleanup?.();
        const cleanup = effect.setup();
        activeEffects.set(effect.index, { cleanup: typeof cleanup === "function" ? cleanup : null });
      }
    },
    calls: () => calls,
  };
}

function all(tree, predicate, result = []) {
  if (!tree || typeof tree !== "object") return result;
  if (Array.isArray(tree)) {
    for (const child of tree) all(child, predicate, result);
    return result;
  }
  if (typeof tree.type === "function") return all(tree.type(tree.props), predicate, result);
  if (predicate(tree)) result.push(tree);
  for (const child of React.Children.toArray(tree.props?.children)) all(child, predicate, result);
  return result;
}

function textContent(node) {
  if (node === null || node === undefined || typeof node === "boolean") return "";
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(textContent).join("");
  if (typeof node.type === "function") return textContent(node.type(node.props));
  return node.props ? textContent(node.props.children) : "";
}

function item(access) {
  return {
    event_id: EVENT_ID,
    access,
    pending_refunds: access === "approver" ? 2 : 0,
    title: "Shared event",
    starts_at: "2026-12-01T18:00:00Z",
    event_state: "published",
    event_source_state: "available",
  };
}

function response(items) {
  return { items, has_more: false, observed_at: "2026-11-12T00:00:00Z" };
}

async function settle() {
  await new Promise((resolve) => setImmediate(resolve));
}

test("work view keeps scanner and approver entry points for one event and refresh removes only the revoked role", async () => {
  const driver = createPageDriver([
    response([item("scanner"), item("approver")]),
    response([item("scanner")]),
  ]);

  driver.render();
  driver.runEffects();
  await settle();
  let tree = driver.render();

  const links = all(tree, (node) => node.type === "a");
  assert.ok(links.some((link) => link.props.href === `/events/${EVENT_ID}/check-in`), "scanner assignment should open check-in");
  assert.ok(links.some((link) => link.props.href === `/admin/events/${EVENT_ID}/refunds`), "approver assignment should open refunds");
  const cards = all(tree, (node) => node.type === "article");
  assert.equal(cards.length, 2, "both access rows should render as separate cards");
  assert.equal(new Set(cards.map((card) => card.key)).size, 2, "event access rows should have distinct React keys");

  const refresh = all(tree, (node) => node.type === "button" && textContent(node).includes("Refresh assignments"))[0];
  assert.ok(refresh, "work view should expose refresh");
  refresh.props.onClick();
  driver.render();
  driver.runEffects();
  await settle();
  tree = driver.render();

  assert.equal(driver.calls(), 2, "refresh should request the current server assignments");
  const refreshedLinks = all(tree, (node) => node.type === "a");
  assert.ok(refreshedLinks.some((link) => link.props.href === `/events/${EVENT_ID}/check-in`), "scanner entry remains after the refresh");
  assert.ok(!refreshedLinks.some((link) => link.props.href === `/admin/events/${EVENT_ID}/refunds`), "revoked approver entry disappears after the refresh");
  assert.equal(all(tree, (node) => node.type === "article").length, 1, "only the remaining scanner row should render");
});
