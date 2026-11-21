import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { loadTypeScript } from "./render-typescript.mjs";
import { ManualScheduler } from "./manual-time.mjs";

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

function pageDriver(
  apiClient,
  authState = { user: { id: "admin-fixture", role: "admin" } },
  scheduler = new ManualScheduler(),
  localeState = { current: "en-US" },
) {
  const slots = [];
  const pendingLayoutEffects = [];
  const pendingEffects = [];
  const activeLayoutEffects = new Map();
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
    useLayoutEffect(setup, dependencies) {
      const index = cursor++;
      const current = slots[index];
      if (current?.kind !== "layout-effect" || !sameDependencies(current.dependencies, dependencies)) {
        slots[index] = { kind: "layout-effect", dependencies };
        pendingLayoutEffects.push({ index, setup });
      }
    },
  };

  const { withRequestDeadline } = loadTypeScript("lib/request-deadline.ts");
  const { default: Page } = loadTypeScript("app/admin/page.tsx", {
    react: hooks,
    "next/navigation": { redirect() {} },
    "next/link": { __esModule: true, default: "a" },
    "@/lib/api/client": { apiClient },
    "@/lib/auth-context": {
      useAuth: () => authState,
    },
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
      const element = Page();
      const pending = pendingLayoutEffects.splice(0);
      for (const effect of pending) {
        activeLayoutEffects.get(effect.index)?.cleanup?.();
        const cleanup = effect.setup();
        activeLayoutEffects.set(effect.index, {
          cleanup: typeof cleanup === "function" ? cleanup : null,
        });
      }
      return element;
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
      for (const effect of activeLayoutEffects.values()) effect.cleanup?.();
      activeLayoutEffects.clear();
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

function requestFixture() {
  const requests = { events: [], jobs: [], outbox: [] };
  let sessionVersion = 0;
  const apiClient = {
    getSessionVersion() {
      return sessionVersion;
    },
    setSessionVersion(value) {
      sessionVersion = value;
    },
    listEventsPage(args, signal) {
      const request = deferred();
      requests.events.push({ ...request, args, signal });
      return request.promise;
    },
    listSchedulerJobs(signal) {
      const request = deferred();
      requests.jobs.push({ ...request, signal });
      return request.promise;
    },
    listAdminOutboxEvents(args, signal) {
      const request = deferred();
      requests.outbox.push({ ...request, args, signal });
      return request.promise;
    },
  };
  return { apiClient, requests };
}

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
  return {
    source_service: "notification",
    records,
    next_cursor: "",
    has_more: false,
    observed_at: "2026-10-31T16:00:00Z",
    window: { from: "2026-10-30T16:00:00Z", to: "2026-10-31T16:00:00Z" },
    supported_event_types: ["booking_confirmation", "booking_failure", "booking_cancellation", "welcome_email"],
    source_record_limit: 5000,
    consumer_outcome: "not_verified",
    ...overrides,
  };
}

test("Overview keeps valid source totals and Booking records visible when one independent read fails", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);

  const initial = driver.render();
  assert.ok(React.isValidElement(initial), "/admin should render an overview page");
  assert(hasText(initial, "Overview"));
  driver.runEffects();
  assert.equal(fixture.requests.events.length, 1);
  assert.equal(fixture.requests.jobs.length, 1);
  assert.equal(fixture.requests.outbox.length, 2);
  assert.equal(fixture.requests.outbox[0].args.source, "booking");
  assert.equal(fixture.requests.outbox[1].args.source, "notification");

  fixture.requests.events[0].resolve({ events: [], total: 17, page: 1, page_size: 1 });
  fixture.requests.jobs[0].reject(new Error("private scheduler stack fixture"));
  fixture.requests.outbox[0].resolve({
    source_service: "booking",
    records: [{
      event_id: "00000000-0000-4000-8000-000000000041",
      source_service: "booking",
      event_type: "BookingConfirmed",
      created_at: "2026-10-31T15:00:00Z",
      recorded_status: "published",
      consumer_outcome: "not_verified",
    }],
    next_cursor: "",
    has_more: false,
    observed_at: "2026-10-31T16:00:00Z",
    window: { from: "2026-10-30T16:00:00Z", to: "2026-10-31T16:00:00Z" },
    supported_event_types: ["BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"],
    source_record_limit: 5000,
    consumer_outcome: "not_verified",
  });
  fixture.requests.outbox[1].resolve(notificationResponse([]));
  await Promise.allSettled([
    fixture.requests.events[0].promise,
    fixture.requests.jobs[0].promise,
    fixture.requests.outbox[0].promise,
    fixture.requests.outbox[1].promise,
  ]);
  await flushPageAsync();

  const overview = driver.render();
  assert(hasText(overview, "17"), "the existing Events total should remain visible");
  assert(hasText(overview, "Unavailable"), "a failed scheduler read must be unavailable, not zero");
  assert(hasText(overview, "BookingConfirmed"));
  assert(hasText(overview, "Published"));
  assert(hasText(overview, "Consumer outcome not verified"));
  assert(find(overview, (node) => node.type === "time" && node.props.dateTime === "2026-10-31T16:00:00Z"), "the Outbox source read time should be shown");
  assert.equal(hasText(overview, "private scheduler stack fixture"), false);
  driver.unmount();
});

test("overview rerenders in Chinese without changing the source-read identity", () => {
  const fixture = requestFixture();
  const localeState = { current: "en-US" };
  const driver = pageDriver(fixture.apiClient, undefined, undefined, localeState);
  const english = driver.render();
  assert(hasText(english, "Admin console"));
  const englishBookingRegion = find(
    english,
    (node) => node.type === "section" && node.props["aria-label"] === "Booking Outbox data source",
  );
  const englishNotificationRegion = find(
    english,
    (node) => node.type === "section" && node.props["aria-label"] === "Notification Intents data source",
  );
  assert(englishBookingRegion, "the Booking Outbox region name should interpolate its source");
  assert(englishNotificationRegion, "the Notification Intents region name should interpolate its source");
  assert.doesNotMatch(englishBookingRegion.props["aria-label"], /\{[^}]+\}/);
  assert.doesNotMatch(englishNotificationRegion.props["aria-label"], /\{[^}]+\}/);
  driver.runEffects();
  assert.equal(fixture.requests.events.length, 1);
  assert.equal(fixture.requests.jobs.length, 1);
  assert.equal(fixture.requests.outbox.length, 2);

  localeState.current = "zh-CN";
  const chinese = driver.render();
  assert(hasText(chinese, "管理员控制台"));
  assert(hasText(chinese, "总览"));
  assert.equal(find(chinese, (node) => node.type === "section" && node.props["aria-label"] === "系统总览") !== null, true);
  const chineseBookingRegion = find(
    chinese,
    (node) => node.type === "section" && node.props["aria-label"] === "Booking 发件箱数据源",
  );
  const chineseNotificationRegion = find(
    chinese,
    (node) => node.type === "section" && node.props["aria-label"] === "通知意图数据源",
  );
  assert(chineseBookingRegion, "the Booking Outbox region name should be localized and interpolated");
  assert(chineseNotificationRegion, "the Notification Intents region name should be localized and interpolated");
  assert.doesNotMatch(chineseBookingRegion.props["aria-label"], /\{[^}]+\}/);
  assert.doesNotMatch(chineseNotificationRegion.props["aria-label"], /\{[^}]+\}/);
  assert.equal(fixture.requests.events.length, 1);
  assert.equal(fixture.requests.jobs.length, 1);
  assert.equal(fixture.requests.outbox.length, 2);
});

test("Overview ignores source responses that settle after the page unmounts", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  const initial = driver.render();
  assert.ok(React.isValidElement(initial), "/admin should render an overview page");
  driver.runEffects();
  driver.unmount();

  fixture.requests.events[0].resolve({ events: [], total: 3, page: 1, page_size: 1 });
  fixture.requests.jobs[0].resolve({ schedulers: [] });
  fixture.requests.outbox[0].resolve({
    source_service: "booking",
    records: [],
    next_cursor: "",
    has_more: false,
    observed_at: "2026-10-31T16:00:00Z",
    window: { from: "2026-10-30T16:00:00Z", to: "2026-10-31T16:00:00Z" },
    supported_event_types: ["BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"],
    source_record_limit: 5000,
    consumer_outcome: "not_verified",
  });
  fixture.requests.outbox[1].resolve(notificationResponse([notificationRecord("unmounted-notification", "welcome_email", "sent")]));
  await Promise.all([
    fixture.requests.events[0].promise,
    fixture.requests.jobs[0].promise,
    fixture.requests.outbox[0].promise,
    fixture.requests.outbox[1].promise,
  ]);
  await flushPageAsync();

  assert.equal(driver.writesAfterUnmount(), 0);
});

test("Overview shows the returned scheduler-config count and a real zero Events total", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  driver.render();
  driver.runEffects();

  fixture.requests.events[0].resolve({ events: [], total: 0, page: 1, page_size: 1 });
  fixture.requests.jobs[0].resolve({
    schedulers: [
      {
        id: "job-a",
        name: "fixture-a",
        cron_expression: "0 0 * * * *",
        is_enabled: true,
        version: 1,
        timeout: 30,
        created_at: "2026-10-30T00:00:00Z",
        updated_at: "2026-10-30T00:00:00Z",
      },
      {
        id: "job-b",
        name: "fixture-b",
        cron_expression: "0 0 * * * *",
        is_enabled: false,
        version: 2,
        timeout: 45,
        created_at: "2026-10-30T00:00:00Z",
        updated_at: "2026-10-30T00:00:00Z",
      },
    ],
  });
  fixture.requests.outbox[0].resolve({
    source_service: "booking",
    records: [],
    next_cursor: "",
    has_more: false,
    observed_at: "2026-10-31T16:00:00Z",
    window: { from: "2026-10-30T16:00:00Z", to: "2026-10-31T16:00:00Z" },
    supported_event_types: ["BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"],
    source_record_limit: 5000,
    consumer_outcome: "not_verified",
  });
  fixture.requests.outbox[1].resolve(notificationResponse([]));
  await Promise.allSettled([
    fixture.requests.events[0].promise,
    fixture.requests.jobs[0].promise,
    fixture.requests.outbox[0].promise,
    fixture.requests.outbox[1].promise,
  ]);

  const overview = driver.render();
  assert(hasText(overview, "0"), "a successful Events total of zero should remain a factual zero");
  assert(hasText(overview, "2"), "the scheduler summary should count only configs returned by its API");
  assert(hasText(overview, "No Booking Outbox records were returned for the last 24 hours."));
  assert.equal(hasText(overview, "Unavailable"), false);
  driver.unmount();
});

test("Overview bounds each independent source read and reports a timeout as unavailable", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  driver.render();
  driver.runEffects();
  assert.deepEqual(driver.scheduler.pendingDelays(), [10000, 10000, 10000, 10000]);

  driver.scheduler.runAll();
  await flushPageAsync();

  const overview = driver.render();
  assert(hasText(overview, "Events could not be loaded."));
  assert(hasText(overview, "Cron Jobs could not be loaded."));
  assert(hasText(overview, "Booking Outbox is temporarily unavailable."));
  assert(hasText(overview, "Notification Intents is temporarily unavailable."));
  assert.equal(hasText(overview, "Loading"), false);
  assert.equal(fixture.requests.events[0].signal.aborted, true);
  assert.equal(fixture.requests.jobs[0].signal.aborted, true);
  assert.equal(fixture.requests.outbox[0].signal.aborted, true);
  assert.equal(fixture.requests.outbox[1].signal.aborted, true);
  driver.unmount();
});

test("Overview masks source values synchronously when the admin identity changes", async () => {
  const fixture = requestFixture();
  const authState = { user: { id: "admin-first", role: "admin" } };
  const driver = pageDriver(fixture.apiClient, authState);
  driver.render();
  driver.runEffects();

  fixture.requests.events[0].resolve({ events: [], total: 17, page: 1, page_size: 1 });
  fixture.requests.jobs[0].resolve({ schedulers: [] });
  fixture.requests.outbox[0].resolve({
    source_service: "booking",
    records: [],
    next_cursor: "",
    has_more: false,
    observed_at: "2026-10-31T16:00:00Z",
    window: { from: "2026-10-30T16:00:00Z", to: "2026-10-31T16:00:00Z" },
    supported_event_types: ["BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"],
    source_record_limit: 5000,
    consumer_outcome: "not_verified",
  });
  fixture.requests.outbox[1].resolve(notificationResponse([]));
  await Promise.allSettled([
    fixture.requests.events[0].promise,
    fixture.requests.jobs[0].promise,
    fixture.requests.outbox[0].promise,
    fixture.requests.outbox[1].promise,
  ]);
  await flushPageAsync();
  assert(hasText(driver.render(), "17"));

  authState.user = { id: "admin-second", role: "admin" };
  fixture.apiClient.setSessionVersion(1);
  const switched = driver.render();
  assert.equal(hasText(switched, "17"), false, "the prior identity snapshot must be masked before the next effect runs");
  assert(hasText(switched, "Loading"));
  assert.equal(fixture.requests.events.length, 1);
  driver.runEffects();
  assert.equal(fixture.requests.events.length, 2);
  driver.unmount();
});

test("Overview reports finite source facts for Booking and Notification independently", async () => {
  const fixture = requestFixture();
  const localeState = { current: "en-US" };
  const driver = pageDriver(fixture.apiClient, undefined, undefined, localeState);
  driver.render();
  driver.runEffects();

  fixture.requests.events[0].resolve({ events: [], total: 1, page: 1, page_size: 1 });
  fixture.requests.jobs[0].resolve({ schedulers: [] });
  fixture.requests.outbox[0].resolve({
    source_service: "booking",
    records: [
      {
        event_id: "00000000-0000-4000-8000-000000000051",
        source_service: "booking",
        event_type: "BookingConfirmed",
        created_at: "2026-10-31T15:30:00Z",
        recorded_status: "pending",
        consumer_outcome: "not_verified",
      },
      {
        event_id: "00000000-0000-4000-8000-000000000052",
        source_service: "booking",
        event_type: "BookingCancelled",
        created_at: "2026-10-31T15:50:00Z",
        recorded_status: "published",
        consumer_outcome: "not_verified",
      },
    ],
    next_cursor: "",
    has_more: false,
    observed_at: "2026-10-31T16:00:00Z",
    window: { from: "2026-10-30T16:00:00Z", to: "2026-10-31T16:00:00Z" },
    supported_event_types: ["BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"],
    source_record_limit: 5000,
    consumer_outcome: "not_verified",
  });
  fixture.requests.outbox[1].resolve(notificationResponse([
    notificationRecord("00000000-0000-4000-8000-000000000061", "welcome_email", "sent"),
  ]));
  await Promise.allSettled([
    fixture.requests.events[0].promise,
    fixture.requests.jobs[0].promise,
    fixture.requests.outbox[0].promise,
    fixture.requests.outbox[1].promise,
  ]);
  await flushPageAsync();

  const overview = driver.render();
  assert(hasText(overview, "Supported recorded states: Pending and Published"));
  assert(hasText(overview, "2 records returned in the last 24 hours"));
  assert(hasText(overview, "Overview sample, up to 5; not a source total"));
  assert(hasText(overview, "Newest record returned"));
  assert(find(overview, (node) => node.type === "time" && node.props.dateTime === "2026-10-31T15:50:00Z"));
  assert(hasText(overview, "Observed at"));
  assert(find(overview, (node) => node.type === "time" && node.props.dateTime === "2026-10-31T16:00:00Z"));
  assert(hasText(overview, "Notification Intents"));
  assert(hasText(overview, "1 record returned in the last 24 hours"));
  assert(hasText(overview, "welcome_email"));
  assert(hasText(overview, "Adapter accepted"));
  assert(hasText(overview, "Pending for 30m at observation time"));
  assert.equal(hasText(overview, "Delivered"), false);
  assert.equal(
    find(overview, (node) => node.type === "a" && node.props["aria-label"] === "Review Notification Intents records")?.props.href,
    "/admin/outbox?source=notification",
    "the Notification coverage link must open the Notification source",
  );
  assert.equal(
    find(overview, (node) => node.type === "a" && node.props["aria-label"] === "View all Notification Intents records")?.props.href,
    "/admin/outbox?source=notification",
    "the Notification snapshot link must open the Notification source",
  );
  assert.equal(
    find(overview, (node) => node.type === "a" && node.props["aria-label"] === "Review Booking Outbox records")?.props.href,
    "/admin/outbox",
    "the Booking coverage link keeps the default source URL",
  );
  assert.equal(fixture.requests.outbox.length, 2, "Overview reads each configured source independently");
  localeState.current = "zh-CN";
  const localized = driver.render();
  assert(hasText(localized, "在观测时间待处理 30分钟"));
  assert.equal(hasText(localized, "less than 1m"), false);
  assert.equal(fixture.requests.outbox.length, 2, "locale changes do not refetch Overview sources");
  driver.unmount();
});

test("Overview keeps each healthy source visible when the other Outbox source fails", async () => {
  for (const failedSource of ["booking", "notification"]) {
    const fixture = requestFixture();
    const driver = pageDriver(fixture.apiClient);
    driver.render();
    driver.runEffects();

    const bookingRequest = fixture.requests.outbox.find((request) => request.args.source === "booking");
    const notificationRequest = fixture.requests.outbox.find((request) => request.args.source === "notification");
    assert.ok(bookingRequest);
    assert.ok(notificationRequest);

    if (failedSource === "booking") {
      bookingRequest.reject(Object.assign(new Error("private booking backend fixture"), { status: 503 }));
      notificationRequest.resolve(notificationResponse([notificationRecord("notif-kept", "booking_failure", "retryable")]));
    } else {
      bookingRequest.resolve({
        source_service: "booking",
        records: [{
          event_id: "booking-kept",
          source_service: "booking",
          event_type: "BookingConfirmed",
          created_at: "2026-10-31T15:30:00Z",
          recorded_status: "pending",
          consumer_outcome: "not_verified",
        }],
        next_cursor: "",
        has_more: false,
        observed_at: "2026-10-31T16:00:00Z",
        window: { from: "2026-10-30T16:00:00Z", to: "2026-10-31T16:00:00Z" },
        supported_event_types: ["BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"],
        source_record_limit: 5000,
        consumer_outcome: "not_verified",
      });
      notificationRequest.reject(Object.assign(new Error("private notification backend fixture"), { status: 503 }));
    }

    await Promise.allSettled([
      bookingRequest.promise,
      notificationRequest.promise,
    ]);
    await flushPageAsync();

    const overview = driver.render();
    assert(hasText(overview, failedSource === "booking" ? "booking_failure" : "BookingConfirmed"));
    assert(hasText(overview, "Unavailable"), `${failedSource} failure should stay unavailable, not zero`);
    assert.equal(hasText(overview, "private booking backend fixture"), false);
    assert.equal(hasText(overview, "private notification backend fixture"), false);
    driver.unmount();
  }
});

test("Overview refresh keeps last successful source data through a partial read failure and can retry", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture.apiClient);
  driver.render();
  driver.runEffects();

  fixture.requests.events[0].resolve({ events: [], total: 17, page: 1, page_size: 1 });
  fixture.requests.jobs[0].resolve({ schedulers: [] });
  fixture.requests.outbox[0].resolve({
    source_service: "booking",
    records: [{
      event_id: "00000000-0000-4000-8000-000000000051",
      source_service: "booking",
      event_type: "BookingConfirmed",
      created_at: "2026-10-31T15:00:00Z",
      recorded_status: "published",
      consumer_outcome: "not_verified",
    }],
    next_cursor: "",
    has_more: false,
    observed_at: "2026-10-31T16:00:00Z",
    window: { from: "2026-10-30T16:00:00Z", to: "2026-10-31T16:00:00Z" },
    supported_event_types: ["BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"],
    source_record_limit: 5000,
    consumer_outcome: "not_verified",
  });
  fixture.requests.outbox[1].resolve(notificationResponse([
    notificationRecord("00000000-0000-4000-8000-000000000052", "welcome_email", "sent"),
  ]));
  await Promise.all([
    fixture.requests.events[0].promise,
    fixture.requests.jobs[0].promise,
    fixture.requests.outbox[0].promise,
    fixture.requests.outbox[1].promise,
  ]);
  await flushPageAsync();

  let overview = driver.render();
  assert(hasText(overview, "BookingConfirmed"));
  assert(hasText(overview, "welcome_email"));
  find(overview, (node) => node.type === "button" && hasText(node, "Refresh")).props.onClick();

  overview = driver.render();
  assert(hasText(overview, "BookingConfirmed"), "same-query refresh must keep valid Booking data visible immediately");
  assert(hasText(overview, "welcome_email"), "refreshing one source must not hide another source");
  assert(hasText(overview, "Refreshing Booking Outbox; showing the last successful data."));
  driver.runEffects();

  fixture.requests.events[1].resolve({ events: [], total: 18, page: 1, page_size: 1 });
  fixture.requests.jobs[1].resolve({ schedulers: [] });
  fixture.requests.outbox[0 + 2].reject(Object.assign(new Error("private booking refresh fixture"), { status: 503 }));
  fixture.requests.outbox[3].resolve(notificationResponse([
    notificationRecord("00000000-0000-4000-8000-000000000053", "booking_failure", "pending"),
  ]));
  await Promise.allSettled([
    fixture.requests.events[1].promise,
    fixture.requests.jobs[1].promise,
    fixture.requests.outbox[2].promise,
    fixture.requests.outbox[3].promise,
  ]);
  await flushPageAsync();

  overview = driver.render();
  assert(hasText(overview, "BookingConfirmed"), "a failed refresh must retain the last successful source rows");
  assert(hasText(overview, "booking_failure"), "a healthy source refresh must still publish its new rows");
  assert(hasText(overview, "The latest refresh for Booking Outbox failed. Showing the last successful data."));
  assert(hasText(overview, "Booking Outbox is temporarily unavailable. Retry the read."));
  assert.equal(hasText(overview, "private booking refresh fixture"), false);
  const retry = find(overview, (node) => node.type === "button" && hasText(node, "Retry"));
  assert.ok(retry, "a failed refresh should expose a retry action");

  retry.props.onClick();
  overview = driver.render();
  assert(hasText(overview, "BookingConfirmed"), "retry should keep the safe snapshot while its read is pending");
  driver.runEffects();
  assert.equal(fixture.requests.events.length, 3);
  assert.equal(fixture.requests.jobs.length, 3);
  assert.equal(fixture.requests.outbox.length, 6);
  fixture.requests.events[2].resolve({ events: [], total: 19, page: 1, page_size: 1 });
  fixture.requests.jobs[2].resolve({ schedulers: [] });
  fixture.requests.outbox[4].resolve({
    source_service: "booking",
    records: [{
      event_id: "00000000-0000-4000-8000-000000000054",
      source_service: "booking",
      event_type: "BookingFailed",
      created_at: "2026-10-31T15:30:00Z",
      recorded_status: "published",
      consumer_outcome: "not_verified",
    }],
    next_cursor: "",
    has_more: false,
    observed_at: "2026-10-31T16:30:00Z",
    window: { from: "2026-10-30T16:30:00Z", to: "2026-10-31T16:30:00Z" },
    supported_event_types: ["BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"],
    source_record_limit: 5000,
    consumer_outcome: "not_verified",
  });
  fixture.requests.outbox[5].resolve(notificationResponse([]));
  await Promise.all([
    fixture.requests.events[2].promise,
    fixture.requests.jobs[2].promise,
    fixture.requests.outbox[4].promise,
    fixture.requests.outbox[5].promise,
  ]);
  await flushPageAsync();
  overview = driver.render();
  assert(hasText(overview, "BookingFailed"));
  assert.equal(hasText(overview, "BookingConfirmed"), false);
  driver.unmount();
});

test("Overview refresh authorization failures clear private data and ignore late source replies", async () => {
  for (const status of [401, 403]) {
    const fixture = requestFixture();
    const driver = pageDriver(fixture.apiClient);
    driver.render();
    driver.runEffects();

    fixture.requests.events[0].resolve({ events: [], total: 17, page: 1, page_size: 1 });
    fixture.requests.jobs[0].resolve({ schedulers: [] });
    fixture.requests.outbox[0].resolve({
      source_service: "booking",
      records: [{
        event_id: `private-overview-booking-${status}`,
        source_service: "booking",
        event_type: "BookingConfirmed",
        created_at: "2026-10-31T15:00:00Z",
        recorded_status: "published",
        consumer_outcome: "not_verified",
      }],
      next_cursor: "",
      has_more: false,
      observed_at: "2026-10-31T16:00:00Z",
      window: { from: "2026-10-30T16:00:00Z", to: "2026-10-31T16:00:00Z" },
      supported_event_types: ["BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"],
      source_record_limit: 5000,
      consumer_outcome: "not_verified",
    });
    fixture.requests.outbox[1].resolve(notificationResponse([
      notificationRecord(`private-overview-notification-${status}`, "welcome_email", "sent"),
    ]));
    await Promise.all([
      fixture.requests.events[0].promise,
      fixture.requests.jobs[0].promise,
      fixture.requests.outbox[0].promise,
      fixture.requests.outbox[1].promise,
    ]);
    await flushPageAsync();

    const populated = driver.render();
    assert(hasText(populated, "BookingConfirmed"));
    assert(hasText(populated, "welcome_email"));
    find(populated, (node) => node.type === "button" && hasText(node, "Refresh")).props.onClick();
    driver.render();
    driver.runEffects();
    assert.equal(fixture.requests.events.length, 2);
    assert.equal(fixture.requests.jobs.length, 2);
    assert.equal(fixture.requests.outbox.length, 4);

    fixture.requests.events[1].reject(Object.assign(new Error(`private auth refresh detail ${status}`), { status }));
    await assert.rejects(fixture.requests.events[1].promise, new RegExp(`private auth refresh detail ${status}`));
    await flushPageAsync();

    const denied = driver.render();
    assert.equal(hasText(denied, "BookingConfirmed"), false, `${status} must clear prior Booking rows`);
    assert.equal(hasText(denied, "welcome_email"), false, `${status} must clear prior Notification rows`);
    assert.equal(
      hasText(denied, status === 401 ? "admin session has expired" : "cannot read Booking Outbox records"),
      true,
      "the authorization failure should remain visible instead of becoming an empty result",
    );
    assert.equal(fixture.requests.jobs[1].signal.aborted, true);
    assert.equal(fixture.requests.outbox[2].signal.aborted, true);
    assert.equal(fixture.requests.outbox[3].signal.aborted, true);

    fixture.requests.jobs[1].resolve({ schedulers: [] });
    fixture.requests.outbox[2].resolve({
      source_service: "booking",
      records: [{
        event_id: `late-overview-booking-${status}`,
        source_service: "booking",
        event_type: "BookingConfirmed",
        created_at: "2026-10-31T15:30:00Z",
        recorded_status: "published",
        consumer_outcome: "not_verified",
      }],
      next_cursor: "",
      has_more: false,
      observed_at: "2026-10-31T16:30:00Z",
      window: { from: "2026-10-30T16:30:00Z", to: "2026-10-31T16:30:00Z" },
      supported_event_types: ["BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"],
      source_record_limit: 5000,
      consumer_outcome: "not_verified",
    });
    fixture.requests.outbox[3].resolve(notificationResponse([
      notificationRecord(`late-overview-notification-${status}`, "welcome_email", "sent"),
    ]));
    await Promise.all([
      fixture.requests.jobs[1].promise,
      fixture.requests.outbox[2].promise,
      fixture.requests.outbox[3].promise,
    ]);
    await flushPageAsync();

    const afterLateReplies = driver.render();
    assert.equal(hasText(afterLateReplies, "BookingConfirmed"), false, "a late aborted Booking reply cannot restore private rows");
    assert.equal(hasText(afterLateReplies, "welcome_email"), false, "a late aborted Notification reply cannot restore private rows");
    assert(hasText(afterLateReplies, "Cron Jobs could not be loaded."), "a late scheduler reply cannot replace the denial snapshot");
    assert.equal(
      hasText(afterLateReplies, status === 401 ? "admin session has expired" : "cannot read Booking Outbox records"),
      true,
    );
    driver.unmount();
  }
});
