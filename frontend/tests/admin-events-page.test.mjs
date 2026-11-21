import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { loadTypeScript } from "./render-typescript.mjs";

const { getI18n } = loadTypeScript("lib/i18n/core.ts");

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function sameDependencies(left, right) {
  return left === right || (
    Array.isArray(left) &&
    Array.isArray(right) &&
    left.length === right.length &&
    left.every((value, index) => Object.is(value, right[index]))
  );
}

// This synthetic hook harness exercises the real page handlers and async
// transitions. The isolated browser checks own actual React/Next lifecycle QA.
function pageDriver(apiClient, localeState = { current: "en-US" }) {
  const slots = [];
  const pendingEffects = [];
  const activeEffects = new Map();
  let cursor = 0;
  let mounted = true;
  let stateWritesAfterUnmount = 0;

  function memoize(factory, dependencies) {
    const index = cursor++;
    const current = slots[index];
    if (current?.kind === "memo" && sameDependencies(current.dependencies, dependencies)) {
      return current.value;
    }
    const value = factory();
    slots[index] = { kind: "memo", dependencies, value };
    return value;
  }

  const hooks = {
    ...React,
    useState(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = typeof initial === "function" ? initial() : initial;
      return [
        slots[index],
        (value) => {
          if (!mounted) stateWritesAfterUnmount += 1;
          slots[index] = typeof value === "function" ? value(slots[index]) : value;
        },
      ];
    },
    useRef(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = { current: initial };
      return slots[index];
    },
    useMemo(factory, dependencies) {
      return memoize(factory, dependencies);
    },
    useCallback(callback, dependencies) {
      return memoize(() => callback, dependencies);
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

  const apiErrors = loadTypeScript("lib/api/errors.ts");
  const { default: Page } = loadTypeScript("app/admin/events/page.tsx", {
    react: hooks,
    "@/lib/api/client": { apiClient },
    "@/lib/api/errors": apiErrors,
    "@/lib/i18n": {
      useI18n: () => ({ ...getI18n(localeState.current), setLocale: (locale) => { localeState.current = locale; } }),
    },
  });

  return {
    render() {
      cursor = 0;
      return Page();
    },
    runEffects() {
      const pending = pendingEffects.splice(0);
      for (const effect of pending) {
        activeEffects.get(effect.index)?.cleanup?.();
        const result = effect.setup();
        activeEffects.set(effect.index, {
          setup: effect.setup,
          cleanup: typeof result === "function" ? result : null,
        });
      }
    },
    unmount() {
      mounted = false;
      for (const effect of activeEffects.values()) effect.cleanup?.();
      activeEffects.clear();
    },
    ApiError: apiErrors.ApiError,
    stateWritesAfterUnmount: () => stateWritesAfterUnmount,
  };
}

function find(tree, predicate) {
  if (!tree || typeof tree !== "object") return null;
  if (predicate(tree)) return tree;
  for (const child of React.Children.toArray(tree.props?.children)) {
    const found = find(child, predicate);
    if (found) return found;
  }
  return null;
}

function textContent(node) {
  if (node === null || node === undefined || typeof node === "boolean") return "";
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(textContent).join("");
  return node.props ? textContent(node.props.children) : "";
}

function hasText(tree, expected) {
  if (tree === expected) return true;
  if (!tree || typeof tree !== "object") return false;
  return React.Children.toArray(tree.props?.children).some((child) => hasText(child, expected));
}

function button(tree, label) {
  return find(
    tree,
    (node) => node.type === "button" && textContent(node.props.children).includes(label),
  );
}

function eventFormDrawer(tree) {
  return find(tree, (node) => node.type?.name === "EventFormDrawer");
}

function event(id, title, category = "concerts") {
  return {
    id,
    title,
    description: "Synthetic event for page state regression.",
    category,
    venue: "Fixture Hall",
    location: "Synthetic City",
    date: "2026-12-15T20:00:00Z",
    image_url: "",
    status: "active",
    tiers: [],
    created_at: "2026-10-31T00:00:00Z",
    date_version: 7,
  };
}

function requestQueue() {
  const requests = [];
  const updateCalls = [];
  const salesCalls = [];
  let updateError = null;
  const apiClient = {
    listAdminEventsPage(args) {
      const request = deferred();
      requests.push({ ...request, args });
      return request.promise;
    },
    async updateEvent(id, input) {
      updateCalls.push({ id, input });
      if (updateError) throw updateError;
      return { ...event(id, "Updated fixture"), ...input };
    },
    async updateEventSales(id, input) {
      salesCalls.push({ id, input });
      return { ...event(id, "Updated fixture"), publication_status: input.publication_status, sales_status: input.sales_status };
    },
  };
  return { apiClient, requests, updateCalls, salesCalls, setUpdateError(error) { updateError = error; } };
}

test("events page distinguishes loading, request failure, retry, and a real empty result", async () => {
  const fixture = requestQueue();
  const driver = pageDriver(fixture.apiClient);

  const initial = driver.render();
  assert(hasText(initial, "Loading events…"));
  driver.runEffects();
  assert.equal(fixture.requests.length, 1);

  fixture.requests[0].reject(new Error("Synthetic events outage"));
  await assert.rejects(fixture.requests[0].promise, /Synthetic events outage/);

  const failed = driver.render();
  assert(hasText(failed, "Events unavailable"));
  assert(hasText(failed, "Events could not be loaded. Use Retry above."));
  assert.equal(button(failed, "Retry") !== null, true);
  assert.equal(hasText(failed, "No events yet. Create the first one."), false);

  button(failed, "Retry").props.onClick();
  driver.render();
  driver.runEffects();
  assert.equal(fixture.requests.length, 2);
  assert(hasText(driver.render(), "Loading events…"));

  fixture.requests[1].resolve({ events: [], total: 0 });
  await fixture.requests[1].promise;

  const empty = driver.render();
  assert(hasText(empty, "No events yet. Create the first one."));
  assert.equal(hasText(empty, "Events unavailable"), false);
  assert.equal(button(empty, "Retry"), null);
});

test("switching locale rerenders the event list without resetting search or issuing another request", async () => {
  const fixture = requestQueue();
  const localeState = { current: "en-US" };
  const driver = pageDriver(fixture.apiClient, localeState);
  driver.render();
  driver.runEffects();
  fixture.requests[0].resolve({ events: [event("locale-event", "Locale fixture")], total: 1 });
  await fixture.requests[0].promise;

  const initial = driver.render();
  const search = find(initial, (node) => node.type === "input" && node.props["aria-label"] === "Search events");
  search.props.onChange({ target: { value: "retained query" } });
  driver.render();

  localeState.current = "zh-CN";
  const localized = driver.render();
  assert.equal(fixture.requests.length, 1);
  assert(hasText(localized, "活动"));
  assert(hasText(localized, "Locale fixture"));
  assert.equal(find(localized, (node) => node.type === "input" && node.props.value === "retained query") !== null, true);
});

test("event tier rows use per-event singular and plural prices while page inventory stays aggregate", async () => {
  const fixture = requestQueue();
  const localeState = { current: "en-US" };
  const driver = pageDriver(fixture.apiClient, localeState);

  driver.render();
  driver.runEffects();
  fixture.requests[0].resolve({
    events: [
      {
        ...event("one-tier", "One tier event"),
        tiers: [{ name: "General", price_cents: 2500, available_quantity: 3, total_quantity: 10 }],
      },
      {
        ...event("many-tiers", "Many tiers event"),
        tiers: [
          { name: "General", price_cents: 3000, available_quantity: 4, total_quantity: 10 },
          { name: "Balcony", price_cents: 4500, available_quantity: 6, total_quantity: 10 },
        ],
      },
    ],
    total: 2,
  });
  await fixture.requests[0].promise;

  const tierCell = (tree, title) => {
    const row = find(tree, (node) => node.type === "tr" && hasText(node, title));
    assert(row, `missing event row for ${title}`);
    return React.Children.toArray(row.props.children)[4];
  };
  const inventoryHint = (tree, label) => {
    const card = find(
      tree,
      (node) => typeof node.type === "function" && node.type.name === "StatCard" && node.props.label === label,
    );
    assert(card, "missing page tier inventory card");
    return textContent(card.props.hint);
  };

  const english = driver.render();
  assert.equal(textContent(tierCell(english, "One tier event")), "1 tier · from $25.00");
  assert.equal(textContent(tierCell(english, "Many tiers event")), "2 tiers · from $30.00");
  assert.equal(
    inventoryHint(english, "Tier availability (this page)"),
    "3 tiers · sum of available quantity on this page; seat holds are not reflected.",
  );

  localeState.current = "zh-CN";
  const chinese = driver.render();
  assert.equal(
    textContent(tierCell(chinese, "One tier event")),
    `1 个票档 · 最低 ${getI18n("zh-CN").usdCents(2500)}`,
  );
  assert.equal(
    textContent(tierCell(chinese, "Many tiers event")),
    `2 个票档 · 最低 ${getI18n("zh-CN").usdCents(3000)}`,
  );
  assert.equal(
    inventoryHint(chinese, "票档余量（当前页）"),
    "3 个票档 · 当前页可用数量之和；不含座位预留。",
  );
  assert.equal(fixture.requests.length, 1, "locale changes do not refetch the event list");
});

test("page inventory uses a singular tier count when exactly one tier is returned", async () => {
  const fixture = requestQueue();
  const localeState = { current: "en-US" };
  const driver = pageDriver(fixture.apiClient, localeState);

  driver.render();
  driver.runEffects();
  fixture.requests[0].resolve({
    events: [{
      ...event("one-tier-page", "One tier page"),
      tiers: [{ name: "General", price_cents: 2500, available_quantity: 3, total_quantity: 10 }],
    }],
    total: 1,
  });
  await fixture.requests[0].promise;

  const inventoryHint = (tree, label) => {
    const card = find(
      tree,
      (node) => typeof node.type === "function" && node.type.name === "StatCard" && node.props.label === label,
    );
    assert(card, "missing page tier inventory card");
    return textContent(card.props.hint);
  };

  const english = driver.render();
  assert.equal(
    inventoryHint(english, "Tier availability (this page)"),
    "1 tier · sum of available quantity on this page; seat holds are not reflected.",
  );

  localeState.current = "zh-CN";
  const chinese = driver.render();
  assert.equal(
    inventoryHint(chinese, "票档余量（当前页）"),
    "1 个票档 · 当前页可用数量之和；不含座位预留。",
  );
  assert.equal(fixture.requests.length, 1, "locale changes do not refetch the event list");
});

test("a slower event filter response cannot replace the latest category page", async () => {
  const fixture = requestQueue();
  const driver = pageDriver(fixture.apiClient);

  driver.render();
  driver.runEffects();
  const filter = find(
    driver.render(),
    (node) => node.type === "select" && node.props["aria-label"] === "Filter by category",
  );
  assert(filter);
  filter.props.onChange({ target: { value: "concerts" } });
  driver.render();
  driver.runEffects();
  assert.equal(fixture.requests.length, 2);
  assert.equal(fixture.requests[0].args.category, undefined);
  assert.equal(fixture.requests[1].args.category, "concerts");

  fixture.requests[1].resolve({ events: [event("new", "Current category result")], total: 1 });
  await fixture.requests[1].promise;
  assert(find(
    driver.render(),
    (node) => node.type === "button" && textContent(node.props.children) === "Current category result",
  ));

  fixture.requests[0].resolve({ events: [event("old", "Stale all-category result")], total: 1 });
  await fixture.requests[0].promise;

  const current = driver.render();
  assert(hasText(current, "Current category result"));
  assert.equal(hasText(current, "Stale all-category result"), false);
});

test("page correction fetches the last valid server page after totals shrink", async () => {
  const fixture = requestQueue();
  const driver = pageDriver(fixture.apiClient);

  driver.render();
  driver.runEffects();
  fixture.requests[0].resolve({ events: [event("page-one", "Page one")], total: 45 });
  await fixture.requests[0].promise;

  button(driver.render(), "Next").props.onClick();
  driver.render();
  driver.runEffects();
  assert.equal(fixture.requests[1].args.page, 2);
  fixture.requests[1].resolve({ events: [event("page-two", "Page two")], total: 45 });
  await fixture.requests[1].promise;

  button(driver.render(), "Next").props.onClick();
  driver.render();
  driver.runEffects();
  assert.equal(fixture.requests[2].args.page, 3);
  fixture.requests[2].resolve({ events: [event("page-three-stale", "Page three")], total: 25 });
  await fixture.requests[2].promise;

  driver.render();
  driver.runEffects();
  assert.equal(fixture.requests[3].args.page, 2);
  fixture.requests[3].resolve({ events: [event("page-two-current", "Corrected page two")], total: 25 });
  await fixture.requests[3].promise;

  const corrected = driver.render();
  assert(hasText(corrected, "Page 2 of 2 · 20 per page."));
  assert(hasText(corrected, "Corrected page two"));
});

test("clearing a page-only status filter does not leave the server list loading", async () => {
  const fixture = requestQueue();
  const driver = pageDriver(fixture.apiClient);

  driver.render();
  driver.runEffects();
  fixture.requests[0].resolve({
    events: [
      event("active", "Active event"),
      { ...event("draft", "Draft event"), status: "draft" },
    ],
    total: 2,
  });
  await fixture.requests[0].promise;

  const loaded = driver.render();
  const statusFilter = find(
    loaded,
    (node) => node.type === "select" && node.props["aria-label"] === "Filter current page by status",
  );
  assert(statusFilter);
  statusFilter.props.onChange({ target: { value: "active" } });

  const filtered = driver.render();
  assert(button(filtered, "Clear filters"));
  assert(hasText(filtered, "Active event"));
  assert.equal(hasText(filtered, "Draft event"), false);

  button(filtered, "Clear filters").props.onClick();
  const cleared = driver.render();

  assert.equal(fixture.requests.length, 1);
  assert.equal(hasText(cleared, "Loading events…"), false);
  assert(hasText(cleared, "Active event"));
  assert(hasText(cleared, "Draft event"));
  assert(hasText(cleared, "Showing 1–2 of 2 events"));
});
test("unmount invalidates an in-flight event page response", async () => {
  const fixture = requestQueue();
  const driver = pageDriver(fixture.apiClient);

  driver.render();
  driver.runEffects();
  driver.unmount();

  fixture.requests[0].resolve({ events: [event("late", "Late event")], total: 1 });
  await fixture.requests[0].promise;

  assert.equal(driver.stateWritesAfterUnmount(), 0);
});

test("event edits omit the date unless it changed and bind changed dates to the loaded version", async () => {
  const fixture = requestQueue();
  const driver = pageDriver(fixture.apiClient);
  driver.render();
  driver.runEffects();
  fixture.requests[0].resolve({ events: [event("versioned-event", "Versioned event")], total: 1 });
  await fixture.requests[0].promise;

  button(driver.render(), "Edit").props.onClick();
  eventFormDrawer(driver.render()).props.onField("title", "Updated metadata");
  await eventFormDrawer(driver.render()).props.onSubmit();

  assert.equal(fixture.updateCalls[0].id, "versioned-event");
  assert.equal(fixture.salesCalls[0].id, "versioned-event");
  const metadataPayload = JSON.parse(JSON.stringify(fixture.updateCalls[0].input));
  assert.equal(Object.hasOwn(metadataPayload, "date"), false);
  assert.equal(Object.hasOwn(metadataPayload, "expected_date_version"), false);
  assert.equal(metadataPayload.title, "Updated metadata");

  const changedFixture = requestQueue();
  const changedDriver = pageDriver(changedFixture.apiClient);
  changedDriver.render();
  changedDriver.runEffects();
  changedFixture.requests[0].resolve({
    events: [{ ...event("changed-event", "Changed event"), date_version: 19 }],
    total: 1,
  });
  await changedFixture.requests[0].promise;

  button(changedDriver.render(), "Edit").props.onClick();
  let drawer = eventFormDrawer(changedDriver.render());
  const nextDate = new Date(drawer.props.form.date);
  nextDate.setDate(nextDate.getDate() + 1);
  const pad = (value) => String(value).padStart(2, "0");
  const changedValue = `${nextDate.getFullYear()}-${pad(nextDate.getMonth() + 1)}-${pad(nextDate.getDate())}T${pad(nextDate.getHours())}:${pad(nextDate.getMinutes())}`;
  drawer.props.onField("date", changedValue);
  await eventFormDrawer(changedDriver.render()).props.onSubmit();

  assert.equal(changedFixture.updateCalls.length, 1);
  assert.equal(changedFixture.updateCalls[0].input.expected_date_version, 19);
  assert.equal(Number.isFinite(Date.parse(changedFixture.updateCalls[0].input.date)), true);
});

test("event date conflict alerts are localized and keep the editor open", async () => {
  for (const scenario of [
    { locale: "en-US", code: "EVENT_DATE_CONFLICT", message: "This event's date changed since you opened it. Reload the page before trying again." },
    { locale: "zh-CN", code: "EVENT_REFUND_TIME_PENDING", message: "该活动存在待处理的未使用票退款审批，暂时不能更改开始时间。请先处理这些退款审批。" },
  ]) {
    const fixture = requestQueue();
    const localeState = { current: scenario.locale };
    const driver = pageDriver(fixture.apiClient, localeState);
    fixture.setUpdateError(new driver.ApiError(scenario.code, "Conflict fixture", 409));
    driver.render();
    driver.runEffects();
    fixture.requests[0].resolve({ events: [event("conflict-event", "Conflict event")], total: 1 });
    await fixture.requests[0].promise;

    button(driver.render(), scenario.locale === "zh-CN" ? "编辑" : "Edit").props.onClick();
    await eventFormDrawer(driver.render()).props.onSubmit();

    const afterConflict = driver.render();
    const drawer = eventFormDrawer(afterConflict);
    assert(drawer, "the editor remains open for a safe retry");
    assert.equal(getI18n(scenario.locale).t(drawer.props.submitError), scenario.message);
  }
});
