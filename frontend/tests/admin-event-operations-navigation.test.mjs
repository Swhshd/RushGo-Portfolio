import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { loadTypeScript } from "./render-typescript.mjs";

const EVENT_ID = "00000000-0000-4000-8000-000000000001";
const { getI18n } = loadTypeScript("lib/i18n/core.ts");

function sameDependencies(left, right) {
  return left === right || (
    Array.isArray(left) && Array.isArray(right) && left.length === right.length &&
    left.every((value, index) => Object.is(value, right[index]))
  );
}

function hookSet() {
  const slots = [];
  let cursor = 0;
  const pendingEffects = [];
  const activeEffects = new Map();
  const hooks = {
    ...React,
    useState(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = typeof initial === "function" ? initial() : initial;
      return [slots[index], (value) => { slots[index] = typeof value === "function" ? value(slots[index]) : value; }];
    },
    useRef(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = { current: initial };
      return slots[index];
    },
    useMemo(factory, dependencies) {
      const index = cursor++;
      const current = slots[index];
      if (current?.kind === "memo" && sameDependencies(current.dependencies, dependencies)) return current.value;
      const value = factory();
      slots[index] = { kind: "memo", dependencies, value };
      return value;
    },
    useCallback(callback, dependencies) {
      return hooks.useMemo(() => callback, dependencies);
    },
    useEffect(setup, dependencies) {
      const index = cursor++;
      const current = slots[index];
      if (current?.kind !== "effect" || !sameDependencies(current.dependencies, dependencies)) {
        slots[index] = { kind: "effect", dependencies };
        pendingEffects.push({ index, setup });
      }
    },
  };
  return {
    hooks,
    reset() { cursor = 0; },
    runEffects() {
      for (const effect of pendingEffects.splice(0)) {
        activeEffects.get(effect.index)?.cleanup?.();
        const cleanup = effect.setup();
        activeEffects.set(effect.index, { cleanup: typeof cleanup === "function" ? cleanup : null });
      }
    },
    unmount() { for (const effect of activeEffects.values()) effect.cleanup?.(); },
  };
}

function textContent(node) {
  if (node === null || node === undefined || typeof node === "boolean") return "";
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(textContent).join("");
  if (typeof node.type === "function") return textContent(node.type(node.props));
  return node.props ? textContent(node.props.children) : "";
}

function find(tree, predicate) {
  if (!tree || typeof tree !== "object") return null;
  if (typeof tree.type === "function") return find(tree.type(tree.props), predicate);
  if (predicate(tree)) return tree;
  for (const child of React.Children.toArray(tree.props?.children)) {
    const found = find(child, predicate);
    if (found) return found;
  }
  return null;
}

function withLocation(search, run) {
  const previous = globalThis.window;
  const historyCalls = [];
  const listeners = new Map();
  globalThis.window = {
    location: { search, href: `http://localhost/admin/events${search}` },
    history: {
      state: null,
      replaceState(_state, _title, url) {
        historyCalls.push(url);
        const parsed = new URL(url, "http://localhost");
        globalThis.window.location.search = parsed.search;
        globalThis.window.location.href = parsed.href;
      },
    },
    addEventListener(type, callback) { listeners.set(type, callback); },
    removeEventListener(type) { listeners.delete(type); },
    dispatchEvent(event) { listeners.get(event.type)?.(event); },
    setTimeout,
    clearTimeout,
  };
  return Promise.resolve(run(historyCalls)).finally(() => {
    if (previous === undefined) delete globalThis.window;
    else globalThis.window = previous;
  });
}

test("Events opens a UUID eventId query as a read-only detail drawer and clears it on close", async () => {
  await withLocation(`?eventId=${EVENT_ID}`, async (historyCalls) => {
    const reactHooks = hookSet();
    const apiClient = { listAdminEventsPage: () => Promise.resolve({
      events: [{
        id: EVENT_ID, title: "Synthetic event", description: "", category: "concerts",
        venue: "Fixture Hall", location: "Synthetic City", date: "2026-12-15T20:00:00Z",
        image_url: "", status: "active", tiers: [], created_at: "2026-10-31T00:00:00Z",
      }],
      total: 1,
    }) };
    function EventDetailDrawer({ eventId, onClose }) {
      return React.createElement("section", { role: "dialog", "data-event-id": eventId },
        React.createElement("button", { type: "button", onClick: onClose }, "Back to events"));
    }
    function EventFormDrawer() { return null; }
    const { default: Page } = loadTypeScript("app/admin/events/page.tsx", {
      react: reactHooks.hooks,
      "next/link": { __esModule: true, default: "a" },
      "@/lib/api/client": { apiClient },
      "@/components/admin-event-detail-drawer": { EventDetailDrawer },
      "@/components/admin-event-form-drawer": { EventFormDrawer },
      "@/lib/i18n": { useI18n: () => getI18n("en-US") },
    });
    const render = () => { reactHooks.reset(); return Page(); };

    render();
    reactHooks.runEffects();
    await new Promise((resolve) => setImmediate(resolve));
    let tree = render();
    const listEntry = find(tree, (node) => node.type === "a" && node.props.href === `/admin/events/${EVENT_ID}/operations`);
    assert.ok(listEntry, "the Events list should link to the event operations route");
    const drawer = find(tree, (node) => node.props?.role === "dialog");
    assert.equal(drawer?.props["data-event-id"], EVENT_ID);
    window.location.search = "";
    window.dispatchEvent({ type: "popstate" });
    tree = render();
    assert.equal(find(tree, (node) => node.props?.role === "dialog"), null, "removing the deep-link query should close the drawer");
    window.location.search = `?eventId=${EVENT_ID}`;
    window.dispatchEvent({ type: "popstate" });
    tree = render();
    assert.equal(find(tree, (node) => node.props?.role === "dialog")?.props["data-event-id"], EVENT_ID);
    find(drawer, (node) => node.type === "button" && textContent(node.props.children).includes("Back to events"))
      .props.onClick();
    tree = render();
    assert.equal(find(tree, (node) => node.props?.role === "dialog"), null);
    assert.equal(historyCalls.length, 1);
    assert.equal(new URL(historyCalls[0], "http://localhost").searchParams.has("eventId"), false);
    reactHooks.unmount();
  });
});

test("Events detail drawer links back into the event operations route", () => {
  const reactHooks = hookSet();
  const event = {
    id: EVENT_ID,
    title: "Synthetic event",
    description: "",
    category: "concerts",
    venue: "Fixture Hall",
    location: "Synthetic City",
    date: "2026-12-15T20:00:00Z",
    image_url: "",
    status: "active",
    tiers: [],
    created_at: "2026-10-31T00:00:00Z",
  };
  const { EventDetailContent } = loadTypeScript("components/admin-event-detail-drawer.tsx", {
    react: reactHooks.hooks,
    "next/link": { __esModule: true, default: "a" },
    "@/lib/api/client": { apiClient: {} },
    "@/lib/i18n": { useI18n: () => getI18n("en-US") },
  });
  reactHooks.reset();
  const tree = EventDetailContent({ event });
  const link = find(tree, (node) => node.type === "a" && textContent(node.props.children).includes("Event operations"));
  assert.equal(link?.props.href, `/admin/events/${EVENT_ID}/operations`);
  reactHooks.unmount();
});
