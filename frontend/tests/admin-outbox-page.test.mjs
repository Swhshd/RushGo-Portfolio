import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { loadTypeScript } from "./render-typescript.mjs";
import { ManualScheduler } from "./manual-time.mjs";

const { getI18n } = loadTypeScript("lib/i18n/core.ts");

function renderOutboxMarkup(locale) {
  const { default: Page } = loadTypeScript("app/admin/outbox/page.tsx", {
    "next/navigation": {
      useSearchParams: () => new URLSearchParams(),
      useRouter: () => ({ push() {} }),
    },
    "next/link": { __esModule: true, default: ({ children, ...props }) => React.createElement("a", props, children) },
    "@/lib/api/client": { apiClient: { getSessionVersion: () => 0 } },
    "@/lib/auth-context": { useAuth: () => ({ user: { id: "ssr-admin", role: "admin" }, isLoading: false }) },
    "@/lib/i18n": { useI18n: () => getI18n(locale) },
  });
  const page = Page();
  const content = page?.type === React.Suspense ? page.props.children : page;
  return renderToStaticMarkup(content);
}

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

function pageDriver(
  apiClient,
  authState = { user: { id: "admin-fixture", role: "admin" } },
  scheduler = new ManualScheduler(),
  navigation = { source: null, pushes: [] },
  localeState = { current: "en-US" },
) {
  const slots = [];
  const pendingEffects = [];
  const activeEffects = new Map();
  let cursor = 0;
  let mounted = true;
  let writesAfterUnmount = 0;

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
          if (!mounted) writesAfterUnmount += 1;
          slots[index] = typeof value === "function" ? value(slots[index]) : value;
        },
      ];
    },
    useRef(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = { current: initial };
      return slots[index];
    },
    useMemo: memoize,
    useCallback: (callback, dependencies) => memoize(() => callback, dependencies),
    useEffect(setup, dependencies) {
      const index = cursor++;
      const current = slots[index];
      if (current?.kind !== "effect" || !sameDependencies(current.dependencies, dependencies)) {
        slots[index] = { kind: "effect", dependencies };
        pendingEffects.push({ index, setup });
      }
    },
  };

  const { withRequestDeadline } = loadTypeScript("lib/request-deadline.ts");
  const { default: Page } = loadTypeScript("app/admin/outbox/page.tsx", {
    react: hooks,
    "next/navigation": {
      useSearchParams: () => ({ get: (key) => key === "source" ? navigation.source : null }),
      useRouter: () => ({
        push(path) {
          navigation.pushes.push(path);
          if (!navigation.deferPush) {
            navigation.source = new URL(path, "http://localhost").searchParams.get("source");
          }
        },
      }),
    },
    "next/link": { __esModule: true, default: "a" },
    "@/lib/api/client": { apiClient },
    "@/lib/auth-context": { useAuth: () => authState },
    "@/lib/request-deadline": {
      withRequestDeadline: (request, controller) => withRequestDeadline(request, controller, 10000, scheduler),
    },
    "@/lib/i18n": {
      useI18n: () => ({ ...getI18n(localeState.current), setLocale: (locale) => { localeState.current = locale; } }),
    },
  });

  return {
    render() {
      cursor = 0;
      const page = Page();
      return page?.type === React.Suspense
        ? page.props.children.type(page.props.children.props)
        : page;
    },
    runEffects() {
      const pending = pendingEffects.splice(0);
      for (const effect of pending) {
        activeEffects.get(effect.index)?.cleanup?.();
        const cleanup = effect.setup();
        activeEffects.set(effect.index, {
          cleanup: typeof cleanup === "function" ? cleanup : null,
        });
      }
    },
    unmount() {
      mounted = false;
      for (const effect of activeEffects.values()) effect.cleanup?.();
      activeEffects.clear();
    },
    writesAfterUnmount: () => writesAfterUnmount,
    scheduler,
  };
}

async function flushPageAsync() {
  await new Promise((resolve) => setImmediate(resolve));
}

function textContent(node) {
  if (node === null || node === undefined || typeof node === "boolean") return "";
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(textContent).join("");
  if (typeof node.type === "function") return textContent(node.type(node.props));
  return node.props ? textContent(node.props.children) : "";
}

function hasText(tree, expected) {
  return textContent(tree).includes(expected);
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

function button(tree, label) {
  return find(tree, (node) => node.type === "button" && textContent(node.props.children).includes(label));
}

function requestFixture() {
  const requests = [];
  let sessionVersion = 0;
  const apiClient = {
    getSessionVersion() {
      return sessionVersion;
    },
    listAdminOutboxEvents(args, signal) {
      const request = deferred();
      requests.push({ ...request, args, signal });
      return request.promise;
    },
  };
  return {
    apiClient,
    requests,
    setSessionVersion(value) {
      sessionVersion = value;
    },
  };
}

function record(eventId, eventType, status = "pending") {
  return {
    event_id: eventId,
    source_service: "booking",
    event_type: eventType,
    created_at: "2026-10-31T15:30:00Z",
    recorded_status: status,
    consumer_outcome: "not_verified",
  };
}

function response(records, overrides = {}) {
  return {
    source_service: "booking",
    records,
    next_cursor: "",
    has_more: false,
    observed_at: "2026-10-31T16:00:00Z",
    window: { from: "2026-10-30T16:00:00Z", to: "2026-10-31T16:00:00Z" },
    supported_event_types: ["BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"],
    source_record_limit: 5000,
    consumer_outcome: "not_verified",
    ...overrides,
  };
}

test("Outbox filter controls retain descriptive bilingual accessible names in server markup", () => {
  const english = renderOutboxMarkup("en-US");
  assert.match(english, /aria-label="Filter by status"/);
  assert.match(english, /aria-label="Filter by event type"/);

  const chinese = renderOutboxMarkup("zh-CN");
  assert.match(chinese, /aria-label="按状态筛选"/);
  assert.match(chinese, /aria-label="按事件类型筛选"/);
});

function notificationRecord(eventId, eventType, status = "pending") {
  return {
    event_id: eventId,
    source_service: "notification",
    event_type: eventType,
    created_at: "2026-10-31T15:30:00Z",
    recorded_status: status,
    consumer_outcome: "not_verified",
  };
}

function notificationResponse(records, overrides = {}) {
  return response(records, {
    source_service: "notification",
    supported_event_types: ["booking_confirmation", "booking_failure", "booking_cancellation", "welcome_email"],
    ...overrides,
  });
}

function startPage(driver) {
  driver.render();
  driver.runEffects();
  driver.render();
  driver.runEffects();
  driver.render();
}

test("Outbox defaults to a 24-hour Booking read with native filters and owner-only status labels", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  startPage(driver);

  assert.equal(fixture.requests.length, 1);
  assert.equal(fixture.requests[0].args.source, "booking");
  assert.equal(fixture.requests[0].args.status, "all");
  assert.equal(fixture.requests[0].args.event_type, undefined);
  assert.equal(fixture.requests[0].args.limit, 20);
  assert.equal(
    new Date(fixture.requests[0].args.to).getTime() - new Date(fixture.requests[0].args.from).getTime(),
    24 * 60 * 60 * 1000,
  );

  const loading = driver.render();
  assert(find(loading, (node) => node.type === "select" && node.props["aria-label"] === "Filter by status"));
  assert(find(loading, (node) => node.type === "select" && node.props["aria-label"] === "Filter by event type"));
  assert(find(loading, (node) => node.type === "input" && node.props["aria-label"] === "From"));
  assert(find(loading, (node) => node.type === "input" && node.props["aria-label"] === "To"));

  fixture.requests[0].resolve(response([record("event-pending", "BookingConfirmed")]));
  await fixture.requests[0].promise;
  await flushPageAsync();

  const loaded = driver.render();
  assert(hasText(loaded, "BookingConfirmed"));
  assert(hasText(loaded, "Pending"));
  assert(hasText(loaded, "Consumer outcome not verified"));
  assert(find(loaded, (node) => node.type === "time" && node.props.dateTime === "2026-10-31T16:00:00Z"));
  driver.unmount();
});

test("manual refresh keeps the last successful Outbox page on failure and retries the same query", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  startPage(driver);
  fixture.requests[0].resolve(response([record("last-good-event", "BookingConfirmed")]));
  await fixture.requests[0].promise;
  await flushPageAsync();

  let tree = driver.render();
  assert(hasText(tree, "last-good-event"));
  button(tree, "Refresh").props.onClick();
  tree = driver.render();
  assert(hasText(tree, "last-good-event"), "same-query refresh must keep the last successful page visible immediately");
  assert(hasText(tree, "Refreshing Booking Outbox; showing the last successful results."));
  driver.runEffects();
  assert.equal(fixture.requests.length, 2);
  assert.equal(fixture.requests[1].args.source, "booking");
  assert.equal(fixture.requests[1].args.cursor, undefined, "refresh starts from page one");

  fixture.requests[1].reject(Object.assign(new Error("private refresh backend detail"), { status: 503 }));
  await assert.rejects(fixture.requests[1].promise, /private refresh backend detail/);
  await flushPageAsync();

  tree = driver.render();
  assert(hasText(tree, "last-good-event"), "failed refresh must retain the last successful rows");
  assert(hasText(tree, "The latest refresh for Booking Outbox failed. Showing the last successful results."));
  assert.equal(hasText(tree, "private refresh backend detail"), false);
  assert(button(tree, "Retry read"), "failed refresh should offer a Retry read action");

  button(tree, "Retry read").props.onClick();
  tree = driver.render();
  assert(hasText(tree, "last-good-event"), "retry must keep the safe snapshot while the replacement read is pending");
  driver.runEffects();
  assert.equal(fixture.requests.length, 3);
  assert.equal(fixture.requests[2].args.source, "booking");
  assert.equal(fixture.requests[2].args.cursor, undefined);
  fixture.requests[2].resolve(response([record("recovered-event", "BookingConfirmed")]));
  await fixture.requests[2].promise;
  await flushPageAsync();

  tree = driver.render();
  assert(hasText(tree, "recovered-event"));
  assert.equal(hasText(tree, "last-good-event"), false, "a successful retry replaces the stale snapshot");
  assert.equal(
    hasText(tree, "The latest refresh for Booking Outbox failed. Showing the last successful results."),
    false,
    "a successful retry must remove the same refresh failure alert shown above",
  );
  driver.unmount();
});

test("a refresh 401 or 403 clears last successful Outbox data", async () => {
  for (const status of [401, 403]) {
    const fixture = requestFixture();
    const driver = pageDriver(fixture.apiClient);
    startPage(driver);
    fixture.requests[0].resolve(response([record(`private-refresh-row-${status}`, "BookingConfirmed")]));
    await fixture.requests[0].promise;
    await flushPageAsync();

    button(driver.render(), "Refresh").props.onClick();
    driver.render();
    driver.runEffects();
    fixture.requests[1].reject(Object.assign(new Error(`private backend detail ${status}`), { status }));
    await assert.rejects(fixture.requests[1].promise, new RegExp(`private backend detail ${status}`));
    await flushPageAsync();

    const denied = driver.render();
    assert.equal(hasText(denied, `private-refresh-row-${status}`), false, `${status} must clear the previous private rows`);
    assert.equal(hasText(denied, "No records in this time range"), false, "authorization denial is not an empty successful result");
    assert.equal(hasText(denied, `private backend detail ${status}`), false);
    assert.equal(hasText(denied, status === 401 ? "admin session has expired" : "cannot read Booking Outbox records"), true);
    driver.unmount();
  }
});

test("changing locale preserves outbox filters and does not issue a new read", async () => {
  const fixture = requestFixture();
  const localeState = { current: "en-US" };
  const driver = pageDriver(fixture.apiClient, undefined, undefined, undefined, localeState);
  startPage(driver);
  fixture.requests[0].resolve(response([record("locale-event", "BookingConfirmed")]));
  await fixture.requests[0].promise;
  await flushPageAsync();

  localeState.current = "zh-CN";
  const localized = driver.render();
  assert(hasText(localized, "只读数据源"));
  assert(hasText(localized, "记录状态"));
  assert(hasText(localized, "locale-event"));
  assert.equal(fixture.requests.length, 1);
  const sourceSelect = find(localized, (node) => node.type === "select" && node.props["aria-label"] === "发件箱数据源");
  assert.equal(sourceSelect.props.value, "booking");
  driver.unmount();
});

test("a new Outbox filter clears old pagination and ignores its late Load more response", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  startPage(driver);
  fixture.requests[0].resolve(response([record("old-event", "BookingConfirmed")], {
    next_cursor: "old-cursor",
    has_more: true,
  }));
  await fixture.requests[0].promise;
  await flushPageAsync();
  assert(hasText(driver.render(), "old-event"));

  let tree = driver.render();
  button(tree, "Load more").props.onClick();
  driver.render();
  assert.equal(fixture.requests.length, 2);
  assert.equal(fixture.requests[1].args.cursor, "old-cursor");

  const status = find(tree, (node) => node.type === "select" && node.props["aria-label"] === "Filter by status");
  status.props.onChange({ target: { value: "published" } });
  tree = driver.render();
  const eventType = find(tree, (node) => node.type === "select" && node.props["aria-label"] === "Filter by event type");
  eventType.props.onChange({ target: { value: "BookingFailed" } });
  tree = driver.render();
  find(tree, (node) => node.type === "form").props.onSubmit({ preventDefault() {} });
  tree = driver.render();
  assert.equal(hasText(tree, "old-event"), false, "rows from the prior filter should be cleared immediately");
  driver.runEffects();
  assert.equal(fixture.requests.length, 3);
  assert.equal(fixture.requests[2].args.status, "published");
  assert.equal(fixture.requests[2].args.event_type, "BookingFailed");
  assert.equal(fixture.requests[2].args.cursor, undefined);

  fixture.requests[2].resolve(response([record("current-event", "BookingFailed", "published")]));
  await fixture.requests[2].promise;
  await flushPageAsync();
  tree = driver.render();
  assert(hasText(tree, "current-event"));

  fixture.requests[1].resolve(response([record("stale-load-more-event", "BookingConfirmed")]));
  await fixture.requests[1].promise;
  await flushPageAsync();
  tree = driver.render();
  assert(hasText(tree, "current-event"));
  assert.equal(hasText(tree, "stale-load-more-event"), false);
  driver.unmount();
});

test("returning to a previously applied filter stays empty until that filter is read again", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  startPage(driver);
  fixture.requests[0].resolve(response([record("first-query-event", "BookingConfirmed")]));
  await fixture.requests[0].promise;
  await flushPageAsync();

  let tree = driver.render();
  find(tree, (node) => node.type === "select" && node.props["aria-label"] === "Filter by status")
    .props.onChange({ target: { value: "published" } });
  tree = driver.render();
  find(tree, (node) => node.type === "select" && node.props["aria-label"] === "Filter by event type")
    .props.onChange({ target: { value: "BookingFailed" } });
  tree = driver.render();
  find(tree, (node) => node.type === "form").props.onSubmit({ preventDefault() {} });
  tree = driver.render();
  assert.equal(hasText(tree, "first-query-event"), false, "a newly applied filter masks rows from the prior filter");
  driver.runEffects();
  assert.equal(fixture.requests[1].args.status, "published");
  assert.equal(fixture.requests[1].args.event_type, "BookingFailed");

  tree = driver.render();
  find(tree, (node) => node.type === "select" && node.props["aria-label"] === "Filter by status")
    .props.onChange({ target: { value: "all" } });
  tree = driver.render();
  find(tree, (node) => node.type === "select" && node.props["aria-label"] === "Filter by event type")
    .props.onChange({ target: { value: "" } });
  tree = driver.render();
  find(tree, (node) => node.type === "form").props.onSubmit({ preventDefault() {} });
  tree = driver.render();
  assert.equal(hasText(tree, "first-query-event"), false, "returning to an earlier filter must still mask its old rows until a new read completes");
  driver.runEffects();
  assert.equal(fixture.requests.length, 3);
  assert.equal(fixture.requests[2].args.status, "all");
  assert.equal(fixture.requests[2].args.event_type, undefined);

  fixture.requests[1].resolve(response([record("late-second-query-event", "BookingFailed", "published")]));
  await fixture.requests[1].promise;
  await flushPageAsync();
  assert.equal(hasText(driver.render(), "late-second-query-event"), false, "a late prior-filter response cannot overwrite the active filter");

  fixture.requests[2].resolve(response([record("restored-query-event", "BookingConfirmed")]));
  await fixture.requests[2].promise;
  await flushPageAsync();
  tree = driver.render();
  assert(hasText(tree, "restored-query-event"));
  assert.equal(hasText(tree, "first-query-event"), false, "only the new response for the restored filter is shown");
  driver.unmount();
});

test("pending age uses the response observation time and is clearly informational", async () => {
  const fixture = requestFixture();
  const localeState = { current: "en-US" };
  const driver = pageDriver(fixture.apiClient, undefined, undefined, undefined, localeState);
  startPage(driver);

  fixture.requests[0].resolve(response([record("event-pending-age", "BookingConfirmed")], {
    observed_at: "2026-10-31T16:00:00Z",
  }));
  await fixture.requests[0].promise;
  await flushPageAsync();

  const loaded = driver.render();
  assert(hasText(loaded, "Pending for 30m at observation time"));
  assert(hasText(loaded, "Age only; not an SLA or a failure signal."));
  localeState.current = "zh-CN";
  const localized = driver.render();
  assert(hasText(localized, "观测时间待处理 30分钟"));
  assert.equal(hasText(localized, "less than 1m"), false);
  assert.equal(fixture.requests.length, 1, "locale changes do not refetch the Outbox source");
  driver.unmount();
});

test("a future-created pending record has no negative age claim", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  startPage(driver);

  fixture.requests[0].resolve(response([
    { ...record("event-future-created", "BookingConfirmed"), created_at: "2026-10-31T16:01:00Z" },
  ], {
    observed_at: "2026-10-31T16:00:00Z",
  }));
  await fixture.requests[0].promise;
  await flushPageAsync();

  const loaded = driver.render();
  assert(hasText(loaded, "Pending age unavailable at observation time"));
  assert.equal(hasText(loaded, "Pending for -"), false);
  driver.unmount();
});

test("Outbox capacity errors show a safe query-limit state rather than unavailable or empty", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  startPage(driver);

  fixture.requests[0].reject(Object.assign(new Error("private database detail fixture"), {
    code: "OUTBOX_CAPACITY_EXCEEDED",
    status: 503,
  }));
  await Promise.allSettled([fixture.requests[0].promise]);
  await flushPageAsync();

  const failed = driver.render();
  assert(hasText(failed, "Query limit reached"));
  assert.equal(hasText(failed, "private database detail fixture"), false);
  assert.equal(hasText(failed, "No records in this time range"), false);
  driver.unmount();
});

test("source switching isolates Notification status, cursor, and late Booking responses", async () => {
  const fixture = requestFixture();
  const navigation = { source: null, pushes: [] };
  const driver = pageDriver(fixture.apiClient, undefined, undefined, navigation);
  startPage(driver);

  assert.equal(fixture.requests[0].args.source, "booking");
  fixture.requests[0].resolve(response([record("booking-initial", "BookingConfirmed")], {
    next_cursor: "booking-only-cursor",
    has_more: true,
  }));
  await fixture.requests[0].promise;
  await flushPageAsync();
  assert(hasText(driver.render(), "booking-initial"));

  button(driver.render(), "Load more").props.onClick();
  driver.render();
  assert.equal(fixture.requests[1].args.source, "booking");
  assert.equal(fixture.requests[1].args.cursor, "booking-only-cursor");

  const sourceSelect = find(driver.render(), (node) => node.type === "select" && node.props["aria-label"] === "Outbox source");
  assert.ok(sourceSelect, "Outbox exposes its bounded source choices");
  sourceSelect.props.onChange({ target: { value: "notification" } });
  assert.equal(navigation.pushes.at(-1), "/admin/outbox?source=notification");

  let switched = driver.render();
  assert.equal(hasText(switched, "booking-initial"), false, "switching sources masks the prior source immediately");
  driver.runEffects();
  assert.equal(fixture.requests.length, 3);
  assert.equal(fixture.requests[2].args.source, "notification");
  assert.equal(fixture.requests[2].args.cursor, undefined);
  assert.equal(fixture.requests[2].args.status, "all");
  assert.equal(fixture.requests[2].args.event_type, undefined);

  fixture.requests[1].resolve(response([record("booking-late-page", "BookingFailed")]));
  await fixture.requests[1].promise;
  await flushPageAsync();
  assert.equal(hasText(driver.render(), "booking-late-page"), false, "late Booking pagination cannot appear under Notification");

  fixture.requests[2].resolve(notificationResponse([
    notificationRecord("notification-sent", "welcome_email", "sent"),
  ], { next_cursor: "notification-only-cursor", has_more: true }));
  await fixture.requests[2].promise;
  await flushPageAsync();
  switched = driver.render();
  assert(hasText(switched, "Adapter accepted"));
  const notificationRow = find(switched, (node) => node.type === "tr" && hasText(node, "notification-sent"));
  assert.equal(
    textContent(React.Children.toArray(notificationRow.props.children)[1]),
    "Notification",
    "the source cell should reflect the Notification Intents record",
  );
  assert.equal(hasText(switched, "Delivered"), false);
  assert(hasText(switched, "End-user delivery is not verified"));

  const statusSelect = find(switched, (node) => node.type === "select" && node.props["aria-label"] === "Filter by status");
  assert.deepEqual(
    React.Children.toArray(statusSelect.props.children)
      .filter((node) => node.type === "option")
      .map((node) => node.props.value),
    ["all", "pending", "delivering", "sent", "retryable", "unknown", "attention"],
  );
  const eventTypeSelect = find(switched, (node) => node.type === "select" && node.props["aria-label"] === "Filter by event type");
  assert.deepEqual(
    React.Children.toArray(eventTypeSelect.props.children)
      .filter((node) => node.type === "option")
      .map((node) => node.props.value),
    ["", "booking_confirmation", "booking_failure", "booking_cancellation", "welcome_email"],
  );

  button(switched, "Load more").props.onClick();
  driver.render();
  assert.equal(fixture.requests.length, 4);
  assert.equal(fixture.requests[3].args.source, "notification");
  assert.equal(fixture.requests[3].args.cursor, "notification-only-cursor");

  const notificationSourceSelect = find(driver.render(), (node) => node.type === "select" && node.props["aria-label"] === "Outbox source");
  notificationSourceSelect.props.onChange({ target: { value: "booking" } });
  assert.equal(navigation.pushes.at(-1), "/admin/outbox");
  switched = driver.render();
  assert.equal(hasText(switched, "notification-sent"), false, "switching back masks Notification rows immediately");
  driver.runEffects();
  assert.equal(fixture.requests.length, 5);
  assert.equal(fixture.requests[4].args.source, "booking");
  assert.equal(fixture.requests[4].args.cursor, undefined, "the Notification cursor is not reused for Booking");
  assert.equal(fixture.requests[4].args.status, "all");
  assert.equal(fixture.requests[4].args.event_type, undefined);

  fixture.requests[3].resolve(notificationResponse([
    notificationRecord("notification-late-page", "booking_failure", "retryable"),
  ]));
  await fixture.requests[3].promise;
  await flushPageAsync();
  assert.equal(hasText(driver.render(), "notification-late-page"), false, "late Notification pagination cannot appear under Booking");

  fixture.requests[4].resolve(response([record("booking-current", "BookingCancelled", "published")]));
  await fixture.requests[4].promise;
  await flushPageAsync();
  const current = driver.render();
  assert(hasText(current, "booking-current"));
  assert.equal(hasText(current, "notification-sent"), false);
  driver.unmount();
});

test("Outbox initializes from the source query and follows source history changes", async () => {
  const fixture = requestFixture();
  const navigation = { source: "notification", pushes: [] };
  const driver = pageDriver(fixture.apiClient, undefined, undefined, navigation);
  startPage(driver);

  assert.equal(fixture.requests.length, 1);
  assert.equal(fixture.requests[0].args.source, "notification", "a Notification overview link must open Notification Intents directly");
  fixture.requests[0].resolve(notificationResponse([
    notificationRecord("history-notification", "welcome_email", "pending"),
  ], { next_cursor: "notification-history-cursor", has_more: true }));
  await fixture.requests[0].promise;
  await flushPageAsync();
  assert(hasText(driver.render(), "history-notification"));

  const sourceSelect = find(driver.render(), (node) => node.type === "select" && node.props["aria-label"] === "Outbox source");
  sourceSelect.props.onChange({ target: { value: "booking" } });
  assert.equal(navigation.pushes.at(-1), "/admin/outbox", "Booking keeps the legacy default URL");
  driver.render();
  driver.runEffects();
  assert.equal(fixture.requests.length, 2);
  assert.equal(fixture.requests[1].args.source, "booking");

  navigation.source = "notification";
  const returnedToNotification = driver.render();
  assert.equal(hasText(returnedToNotification, "history-notification"), false, "history navigation masks records from the prior source immediately");
  assert.equal(find(returnedToNotification, (node) => node.type === "select" && node.props["aria-label"] === "Outbox source").props.value, "notification");
  driver.runEffects();
  driver.render();
  driver.runEffects();
  assert.equal(fixture.requests.length, 3);
  assert.equal(fixture.requests[2].args.source, "notification");
  assert.equal(fixture.requests[2].args.cursor, undefined, "history navigation starts the source from page one");
  assert.equal(fixture.requests[2].args.status, "all", "history navigation clears the old source's status filter");
  assert.equal(fixture.requests[2].args.event_type, undefined, "history navigation clears the old source's event filter");
  driver.unmount();
});

test("a pending source navigation stays isolated until the URL query updates", async () => {
  const fixture = requestFixture();
  const navigation = { source: null, pushes: [], deferPush: true };
  const driver = pageDriver(fixture.apiClient, undefined, undefined, navigation);
  startPage(driver);
  assert.equal(fixture.requests[0].args.source, "booking");

  const sourceSelect = find(driver.render(), (node) => node.type === "select" && node.props["aria-label"] === "Outbox source");
  sourceSelect.props.onChange({ target: { value: "notification" } });
  assert.equal(navigation.pushes.at(-1), "/admin/outbox?source=notification");
  driver.render();
  assert.equal(find(driver.render(), (node) => node.type === "select" && node.props["aria-label"] === "Outbox source").props.value, "notification");
  driver.runEffects();
  driver.render();
  assert.equal(fixture.requests.length, 1, "a pending route transition must not issue a read for the URL's previous source");

  navigation.source = "notification";
  driver.render();
  driver.runEffects();
  driver.render();
  driver.runEffects();
  assert.equal(fixture.requests.length, 2);
  assert.equal(fixture.requests[1].args.source, "notification");
  driver.unmount();
});

test("transient 500/503 Load more failures retain current records and hide raw errors", async () => {
  for (const status of [500, 503]) {
    const fixture = requestFixture();
    const driver = pageDriver(fixture.apiClient);
    startPage(driver);
    fixture.requests[0].resolve(response([record(`first-event-${status}`, "BookingCancelled")], {
      next_cursor: `next-cursor-${status}`,
      has_more: true,
    }));
    await fixture.requests[0].promise;
    await flushPageAsync();

    const firstPage = driver.render();
    assert(button(firstPage, "Load more"));
    button(firstPage, "Load more").props.onClick();
    driver.render();
    assert.equal(fixture.requests.length, 2);

    fixture.requests[1].reject(Object.assign(new Error(`sensitive backend stack ${status}`), { status }));
    await assert.rejects(fixture.requests[1].promise, new RegExp(`sensitive backend stack ${status}`));
    await flushPageAsync();

    const afterFailure = driver.render();
    assert(hasText(afterFailure, `first-event-${status}`));
    assert(hasText(afterFailure, "Could not load more Booking Outbox records"));
    assert.equal(hasText(afterFailure, `sensitive backend stack ${status}`), false);
    assert(button(afterFailure, "Retry load more"));
    driver.unmount();
  }
});

test("Load more 401 and 403 errors clear prior records and pagination state", async () => {
  for (const status of [401, 403]) {
    const fixture = requestFixture();
    const driver = pageDriver(fixture.apiClient);
    startPage(driver);
    fixture.requests[0].resolve(response([record(`private-page-${status}`, "BookingConfirmed")], {
      next_cursor: `private-cursor-${status}`,
      has_more: true,
    }));
    await fixture.requests[0].promise;
    await flushPageAsync();

    button(driver.render(), "Load more").props.onClick();
    driver.render();
    assert.equal(fixture.requests[1].args.cursor, `private-cursor-${status}`);
    fixture.requests[1].reject(Object.assign(new Error(`private backend detail ${status}`), { status }));
    await assert.rejects(fixture.requests[1].promise, new RegExp(`private backend detail ${status}`));
    await flushPageAsync();

    const denied = driver.render();
    assert.equal(hasText(denied, `private-page-${status}`), false, `${status} must not keep rows read under an earlier authorization state`);
    assert.equal(hasText(denied, "No records in this time range"), false, "authorization denial is not presented as a successful empty query");
    assert.equal(button(denied, "Load more"), null);
    assert.equal(button(denied, "Retry load more"), null);
    assert.equal(hasText(denied, status === 401 ? "admin session has expired" : "cannot read Booking Outbox records"), true);
    assert.equal(hasText(denied, `private backend detail ${status}`), false);
    driver.unmount();
  }
});

test("switching sources restores each source's own applied filters and starts at page one", async () => {
  const fixture = requestFixture();
  const navigation = { source: null, pushes: [] };
  const driver = pageDriver(fixture.apiClient, undefined, undefined, navigation);
  startPage(driver);
  fixture.requests[0].resolve(response([record("booking-default", "BookingConfirmed")]));
  await fixture.requests[0].promise;
  await flushPageAsync();

  const bookingRange = { from: "2026-10-30T00:00", to: "2026-10-31T00:00" };
  let tree = driver.render();
  find(tree, (node) => node.type === "select" && node.props["aria-label"] === "Filter by status")
    .props.onChange({ target: { value: "published" } });
  tree = driver.render();
  find(tree, (node) => node.type === "select" && node.props["aria-label"] === "Filter by event type")
    .props.onChange({ target: { value: "BookingFailed" } });
  tree = driver.render();
  find(tree, (node) => node.type === "input" && node.props["aria-label"] === "From")
    .props.onChange({ target: { value: bookingRange.from } });
  tree = driver.render();
  find(tree, (node) => node.type === "input" && node.props["aria-label"] === "To")
    .props.onChange({ target: { value: bookingRange.to } });
  tree = driver.render();
  find(tree, (node) => node.type === "form").props.onSubmit({ preventDefault() {} });
  driver.render();
  driver.runEffects();
  assert.equal(fixture.requests[1].args.status, "published");
  assert.equal(fixture.requests[1].args.event_type, "BookingFailed");
  assert.equal(fixture.requests[1].args.from, new Date(bookingRange.from).toISOString());
  assert.equal(fixture.requests[1].args.to, new Date(bookingRange.to).toISOString());
  fixture.requests[1].resolve(response([record("booking-filtered", "BookingFailed", "published")], {
    next_cursor: "booking-filtered-cursor",
    has_more: true,
  }));
  await fixture.requests[1].promise;
  await flushPageAsync();

  find(driver.render(), (node) => node.type === "select" && node.props["aria-label"] === "Outbox source")
    .props.onChange({ target: { value: "notification" } });
  tree = driver.render();
  driver.runEffects();
  assert.equal(fixture.requests[2].args.status, "all", "the first Notification read uses its own default query");
  assert.equal(fixture.requests[2].args.event_type, undefined);
  fixture.requests[2].resolve(notificationResponse([notificationRecord("notification-default", "welcome_email")]));
  await fixture.requests[2].promise;
  await flushPageAsync();

  const notificationRange = { from: "2026-10-29T00:00", to: "2026-10-30T12:00" };
  tree = driver.render();
  find(tree, (node) => node.type === "select" && node.props["aria-label"] === "Filter by status")
    .props.onChange({ target: { value: "sent" } });
  tree = driver.render();
  find(tree, (node) => node.type === "select" && node.props["aria-label"] === "Filter by event type")
    .props.onChange({ target: { value: "welcome_email" } });
  tree = driver.render();
  find(tree, (node) => node.type === "input" && node.props["aria-label"] === "From")
    .props.onChange({ target: { value: notificationRange.from } });
  tree = driver.render();
  find(tree, (node) => node.type === "input" && node.props["aria-label"] === "To")
    .props.onChange({ target: { value: notificationRange.to } });
  tree = driver.render();
  find(tree, (node) => node.type === "form").props.onSubmit({ preventDefault() {} });
  driver.render();
  driver.runEffects();
  assert.equal(fixture.requests[3].args.status, "sent");
  assert.equal(fixture.requests[3].args.event_type, "welcome_email");
  assert.equal(fixture.requests[3].args.from, new Date(notificationRange.from).toISOString());
  assert.equal(fixture.requests[3].args.to, new Date(notificationRange.to).toISOString());
  fixture.requests[3].resolve(notificationResponse([notificationRecord("notification-filtered", "welcome_email", "sent")], {
    next_cursor: "notification-filtered-cursor",
    has_more: true,
  }));
  await fixture.requests[3].promise;
  await flushPageAsync();

  find(driver.render(), (node) => node.type === "select" && node.props["aria-label"] === "Outbox source")
    .props.onChange({ target: { value: "booking" } });
  tree = driver.render();
  driver.runEffects();
  assert.equal(fixture.requests[4].args.source, "booking");
  assert.equal(fixture.requests[4].args.status, "published");
  assert.equal(fixture.requests[4].args.event_type, "BookingFailed");
  assert.equal(fixture.requests[4].args.from, new Date(bookingRange.from).toISOString());
  assert.equal(fixture.requests[4].args.to, new Date(bookingRange.to).toISOString());
  assert.equal(fixture.requests[4].args.cursor, undefined, "returning to Booking starts with a fresh source cursor");

  fixture.requests[4].resolve(response([record("booking-restored", "BookingFailed", "published")]));
  await fixture.requests[4].promise;
  await flushPageAsync();
  find(driver.render(), (node) => node.type === "select" && node.props["aria-label"] === "Outbox source")
    .props.onChange({ target: { value: "notification" } });
  tree = driver.render();
  assert.equal(hasText(tree, "notification-filtered"), false, "returning to a source must hide its earlier rows until the new read completes");
  driver.runEffects();
  assert.equal(fixture.requests[5].args.source, "notification");
  assert.equal(fixture.requests[5].args.status, "sent");
  assert.equal(fixture.requests[5].args.event_type, "welcome_email");
  assert.equal(fixture.requests[5].args.from, new Date(notificationRange.from).toISOString());
  assert.equal(fixture.requests[5].args.to, new Date(notificationRange.to).toISOString());
  assert.equal(fixture.requests[5].args.cursor, undefined, "returning to Notification starts with a fresh source cursor");
  driver.unmount();
});

test("an Outbox source error is unavailable, not an empty successful result", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  startPage(driver);
  fixture.requests[0].reject(new Error("private database error fixture"));
  await assert.rejects(fixture.requests[0].promise, /private database error fixture/);
  await flushPageAsync();

  const failed = driver.render();
  assert(hasText(failed, "Booking Outbox is temporarily unavailable"));
  assert.equal(hasText(failed, "No records in this time range"), false);
  assert.equal(hasText(failed, "private database error fixture"), false);
  assert(button(failed, "Retry read"));
  driver.unmount();
});

test("a held first-page read times out into a retryable source error", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  startPage(driver);
  assert.deepEqual(driver.scheduler.pendingDelays(), [10000]);

  driver.scheduler.runAll();
  await flushPageAsync();

  const failed = driver.render();
  assert(hasText(failed, "Booking Outbox is temporarily unavailable"));
  assert.equal(hasText(failed, "Loading Booking Outbox records"), false);
  assert.equal(hasText(failed, "No records in this time range"), false);
  assert(button(failed, "Retry read"));
  assert.equal(fixture.requests[0].signal.aborted, true);

  button(failed, "Retry read").props.onClick();
  driver.render();
  driver.runEffects();
  driver.render();
  assert.equal(fixture.requests.length, 2);
  assert.deepEqual(driver.scheduler.pendingDelays(), [10000]);
  driver.unmount();
  assert.equal(fixture.requests[1].signal.aborted, true);
});

test("a Load more deadline keeps the current page and offers a retry", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  startPage(driver);
  fixture.requests[0].resolve(response([record("kept-event", "BookingConfirmed")], {
    next_cursor: "next-cursor",
    has_more: true,
  }));
  await fixture.requests[0].promise;
  await flushPageAsync();
  assert.deepEqual(driver.scheduler.pendingDelays(), []);

  button(driver.render(), "Load more").props.onClick();
  driver.render();
  assert.deepEqual(driver.scheduler.pendingDelays(), [10000]);
  driver.scheduler.runAll();
  await flushPageAsync();

  const afterDeadline = driver.render();
  assert(hasText(afterDeadline, "kept-event"));
  assert(hasText(afterDeadline, "Could not load more Booking Outbox records"));
  assert(button(afterDeadline, "Retry load more"));
  assert.equal(fixture.requests[1].signal.aborted, true);
  driver.unmount();
});

test("a time range wider than 7 days is rejected before it replaces the current result", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  startPage(driver);
  fixture.requests[0].resolve(response([record("current-window-event", "BookingConfirmed")]));
  await fixture.requests[0].promise;
  await flushPageAsync();

  let tree = driver.render();
  find(tree, (node) => node.type === "input" && node.props["aria-label"] === "From")
    .props.onChange({ target: { value: "2026-10-01T00:00" } });
  tree = driver.render();
  find(tree, (node) => node.type === "input" && node.props["aria-label"] === "To")
    .props.onChange({ target: { value: "2026-10-09T00:00" } });
  tree = driver.render();
  find(tree, (node) => node.type === "form").props.onSubmit({ preventDefault() {} });
  tree = driver.render();

  assert(hasText(tree, "Choose a time range of 7 days or less."));
  assert(hasText(tree, "current-window-event"));
  assert.equal(fixture.requests.length, 1, "invalid range must not issue a replacement query");
  driver.unmount();
});

test("a zero-length time range is rejected before a read starts", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  startPage(driver);
  fixture.requests[0].resolve(response([record("current-window-event", "BookingConfirmed")]));
  await fixture.requests[0].promise;
  await flushPageAsync();

  let tree = driver.render();
  const sameTime = "2026-10-01T00:00";
  find(tree, (node) => node.type === "input" && node.props["aria-label"] === "From")
    .props.onChange({ target: { value: sameTime } });
  tree = driver.render();
  find(tree, (node) => node.type === "input" && node.props["aria-label"] === "To")
    .props.onChange({ target: { value: sameTime } });
  tree = driver.render();
  find(tree, (node) => node.type === "form").props.onSubmit({ preventDefault() {} });
  tree = driver.render();

  assert(hasText(tree, "The start time must be before the end time."));
  assert.equal(fixture.requests.length, 1);
  assert(hasText(tree, "current-window-event"));
  driver.unmount();
});

test("a new admin identity immediately hides prior rows and ignores late pagination", async () => {
  const fixture = requestFixture();
  const authState = { user: { id: "admin-first", role: "admin" } };
  const driver = pageDriver(fixture.apiClient, authState);
  startPage(driver);
  fixture.requests[0].resolve(response([record("first-session-event", "BookingConfirmed")], {
    next_cursor: "first-session-cursor",
    has_more: true,
  }));
  await fixture.requests[0].promise;
  await flushPageAsync();
  assert(hasText(driver.render(), "first-session-event"));

  button(driver.render(), "Load more").props.onClick();
  driver.render();
  assert.equal(fixture.requests.length, 2);

  authState.user = { id: "admin-second", role: "admin" };
  fixture.setSessionVersion(1);
  const switched = driver.render();
  assert.equal(hasText(switched, "first-session-event"), false, "prior identity rows must be masked during the render that sees the new identity");
  assert(hasText(switched, "Loading Booking Outbox records"));
  assert.equal(fixture.requests.length, 2, "the new owner is masked before its effect starts");
  driver.runEffects();
  driver.render();
  assert.equal(fixture.requests.length, 3);

  fixture.requests[2].resolve(response([record("second-session-event", "BookingConfirmed")]));
  await fixture.requests[2].promise;
  await flushPageAsync();
  assert(hasText(driver.render(), "second-session-event"));

  fixture.requests[1].resolve(response([record("late-first-session-page", "BookingFailed")]));
  await fixture.requests[1].promise;
  await flushPageAsync();
  const current = driver.render();
  assert(hasText(current, "second-session-event"));
  assert.equal(hasText(current, "late-first-session-page"), false);
  driver.unmount();
});

test("an in-flight Outbox read is cancelled and ignored when the page unmounts", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  startPage(driver);
  assert.equal(fixture.requests.length, 1);
  driver.unmount();

  fixture.requests[0].resolve(response([record("unmounted-event", "BookingConfirmed")]));
  await fixture.requests[0].promise;
  await flushPageAsync();

  assert.equal(fixture.requests[0].signal.aborted, true);
  assert.equal(driver.writesAfterUnmount(), 0);
});
