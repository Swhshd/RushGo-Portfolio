import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { loadTypeScript } from "./render-typescript.mjs";
import { ManualScheduler } from "./manual-time.mjs";

const EVENT_A = "00000000-0000-4000-8000-000000000001";
const EVENT_B = "00000000-0000-4000-8000-000000000002";

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
    Array.isArray(left) && Array.isArray(right) &&
    left.length === right.length && left.every((value, index) => Object.is(value, right[index]))
  );
}

function pageDriver(
  fixture,
  authState = { user: { id: "admin-a", role: "admin" } },
  route = { id: EVENT_A },
  scheduler = new ManualScheduler(),
  localeState = { value: "en-US" },
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
    if (current?.kind === "memo" && sameDependencies(current.dependencies, dependencies)) return current.value;
    const value = factory();
    slots[index] = { kind: "memo", dependencies, value };
    return value;
  }

  const hooks = {
    ...React,
    useState(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = typeof initial === "function" ? initial() : initial;
      return [slots[index], (value) => {
        if (!mounted) writesAfterUnmount += 1;
        slots[index] = typeof value === "function" ? value(slots[index]) : value;
      }];
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
  const { adminEn, adminZh } = loadTypeScript("lib/i18n/admin.ts");
  const dictionaries = { "en-US": adminEn, "zh-CN": adminZh };
  function t(key, params = {}) {
    const template = dictionaries[localeState.value]?.[key] ?? key;
    return template.replace(/\{([a-zA-Z][a-zA-Z0-9_]*)\}/g, (_match, name) => String(params[name] ?? ""));
  }
  const { default: Page } = loadTypeScript("app/admin/events/[id]/operations/page.tsx", {
    react: hooks,
    "next/navigation": { useParams: () => ({ id: route.id }) },
    "next/link": { __esModule: true, default: "a" },
    "lucide-react": {
      Activity: "svg", AlertCircle: "svg", ArrowLeft: "svg", CheckCircle2: "svg",
      Clock3: "svg", RefreshCw: "svg", ShieldCheck: "svg", Ticket: "svg",
    },
    "@/lib/api/client": { apiClient: fixture.apiClient },
    "@/lib/auth-context": { useAuth: () => authState },
    "@/lib/i18n": {
      useI18n: () => ({
        locale: localeState.value,
        setLocale(value) { localeState.value = value; },
        t,
        text: (message) => message,
        number: (value) => new Intl.NumberFormat(localeState.value).format(value),
        date: (value, options) => new Intl.DateTimeFormat(localeState.value, options).format(new Date(value)),
        usdCents: (cents) => new Intl.NumberFormat(localeState.value, { style: "currency", currency: "USD" }).format(cents / 100),
      }),
    },
    "@/lib/request-deadline": {
      withRequestDeadline: (request, controller) => withRequestDeadline(request, controller, 10_000, scheduler),
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
        const cleanup = effect.setup();
        activeEffects.set(effect.index, { cleanup: typeof cleanup === "function" ? cleanup : null });
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

function requestFixture() {
  const requests = [];
  const ticketRequests = [];
  let sessionVersion = 0;
  const apiClient = {
    getSessionVersion: () => sessionVersion,
    getAdminEventTicketOperations(eventId, signal) {
      const request = deferred();
      requests.push({ ...request, eventId, signal });
      return request.promise;
    },
    listAdminEventTickets(eventId, params, signal) {
      const request = deferred();
      ticketRequests.push({ ...request, eventId, params, signal });
      return request.promise;
    },
  };
  return {
    apiClient,
    requests,
    ticketRequests,
    setSessionVersion(value) { sessionVersion = value; },
  };
}

function operations(eventId, title, total) {
  return {
    event_id: eventId,
    event_title: title,
    event_venue: "RushGo Arena",
    event_date: "2026-11-01T19:30:00Z",
    observed_at: "2026-11-01T12:00:00Z",
    source_service: "booking",
    source_status: "available",
    coverage: "recorded_ticket_snapshot",
    total_recorded_count: total,
    issued_count: Math.max(0, total - 2),
    checked_in_count: total > 1 ? 1 : 0,
    revoked_count: total > 0 ? 1 : 0,
    successful_checkin_count: 1,
    latest_checkin_at: "2026-11-01T11:55:00Z",
    recent_checkins: [{ checked_in_at: "2026-11-01T11:55:00Z" }],
    source_record_limit: 5000,
    live_admission_eligibility: "not_evaluated",
  };
}

function emptyOperations(eventId) {
  return {
    ...operations(eventId, "Empty event", 0),
    issued_count: 0,
    checked_in_count: 0,
    revoked_count: 0,
    successful_checkin_count: 0,
    latest_checkin_at: null,
    recent_checkins: [],
  };
}

function ticketPage(eventId, status = "ALL", tickets = [], overrides = {}) {
  return {
    event_id: eventId,
    observed_at: "2026-11-01T12:01:00Z",
    source_service: "booking",
    source_status: "available",
    coverage: "recorded_ticket_snapshot",
    source_record_limit: 5000,
    status_filter: status,
    page_size: 20,
    has_more: false,
    next_cursor: null,
    tickets,
    ...overrides,
  };
}

function ticket(ticketId) {
  return {
    ticket_id: ticketId,
    status: "ISSUED",
    issued_at: "2026-11-01T11:00:00Z",
    checked_in_at: null,
    revoked_at: null,
  };
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

async function flushPageAsync() {
  await new Promise((resolve) => setImmediate(resolve));
}

function startPage(driver) {
  driver.render();
  driver.runEffects();
  driver.render();
}

test("Event Operations shows separate recorded and accepted counts plus the scanner detail return path", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture);
  startPage(driver);
  assert.equal(fixture.requests.length, 1);
  assert.equal(fixture.requests[0].eventId, EVENT_A);
  fixture.requests[0].resolve(operations(EVENT_A, "Midnight Arena", 4));
  await fixture.requests[0].promise;
  await flushPageAsync();

  const ready = driver.render();
  assert(hasText(ready, "Midnight Arena"));
  assert(hasText(ready, "Recorded tickets"));
  assert(hasText(ready, "Successful check-ins"));
  assert(hasText(ready, "Live admission eligibility: not evaluated"));
  assert(hasText(ready, "not one atomic cross-service snapshot"));
  assert.equal(hasText(ready, "Attendees"), false);
  const scannerLink = find(ready, (node) => node.type === "a" && textContent(node.props.children).includes("Manage scanner assignments"));
  assert.equal(scannerLink?.props.href, `/admin/events?eventId=${EVENT_A}`);
  driver.unmount();
});

test("delivery exceptions raise a review notice without claiming payment", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture);
  startPage(driver);
  fixture.requests[0].resolve({
    ...operations(EVENT_A, "Delivery event", 2),
    delivery_exceptions: [{
      booking_id: "00000000-0000-4000-8000-000000000010",
      state: "CONFIRMED_TICKET_DELIVERY_PENDING",
      booking_status: "CONFIRMED",
      expected_ticket_count: 2,
      recorded_ticket_count: 0,
      issuance_receipt_outcome: "NOT_RECORDED",
      issuance_reason_code: "issuance_pending",
      booking_created_at: "2026-11-01T11:00:00Z",
    }],
  });
  await fixture.requests[0].promise;
  await flushPageAsync();

  const ready = driver.render();
  assert(hasText(ready, "1 confirmed bookings need ticket-delivery review"));
  assert(hasText(ready, "does not prove payment collection"));
  const link = find(ready, (node) => node.type === "a" && node.props.href === "#admin-event-delivery-exceptions");
  assert.ok(link);
  assert.equal(hasText(ready, "paid"), false);
  driver.unmount();
});

test("switching events masks previous counts and a late response cannot replace the new event", async () => {
  const fixture = requestFixture();
  const route = { id: EVENT_A };
  const driver = pageDriver(fixture, undefined, route);
  startPage(driver);
  fixture.requests[0].resolve(operations(EVENT_A, "Event A", 4));
  await fixture.requests[0].promise;
  await flushPageAsync();
  assert(hasText(driver.render(), "Event A"));

  route.id = EVENT_B;
  assert.equal(hasText(driver.render(), "Event A"), false);
  driver.runEffects();
  assert.equal(fixture.requests.length, 2);
  fixture.requests[1].resolve(operations(EVENT_B, "Event B", 7));
  await fixture.requests[1].promise;
  await flushPageAsync();
  fixture.requests[0].resolve(operations(EVENT_A, "Late Event A", 99));
  await fixture.requests[0].promise;
  await flushPageAsync();

  const current = driver.render();
  assert(hasText(current, "Event B"));
  assert.equal(hasText(current, "Late Event A"), false);
  assert.equal(hasText(current, "99"), false);
  driver.unmount();
});

test("an identity or session change masks old data before the next read starts", async () => {
  const fixture = requestFixture();
  const authState = { user: { id: "admin-a", role: "admin" } };
  const driver = pageDriver(fixture, authState);
  startPage(driver);
  fixture.requests[0].resolve(operations(EVENT_A, "Private Event", 4));
  await fixture.requests[0].promise;
  await flushPageAsync();

  authState.user = { id: "admin-b", role: "admin" };
  fixture.setSessionVersion(1);
  assert.equal(hasText(driver.render(), "Private Event"), false);
  driver.runEffects();
  assert.equal(fixture.requests.length, 2);
  fixture.requests[1].resolve(operations(EVENT_A, "Current Event", 3));
  await fixture.requests[1].promise;
  await flushPageAsync();
  assert(hasText(driver.render(), "Current Event"));
  driver.unmount();
});

test("Refresh issues a new bounded read and unmount aborts its active request", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture);
  startPage(driver);
  fixture.requests[0].resolve(operations(EVENT_A, "Event A", 4));
  await fixture.requests[0].promise;
  await flushPageAsync();
  const refresh = button(driver.render(), "Refresh");
  assert.ok(refresh);
  refresh.props.onClick();
  driver.render();
  driver.runEffects();
  assert.equal(fixture.requests.length, 2);
  const activeSignal = fixture.requests[1].signal;
  driver.unmount();
  assert.equal(activeSignal.aborted, true);
});

test("statistics refresh still reads after the ticket list has settled", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture);
  startPage(driver);
  fixture.requests[0].resolve(operations(EVENT_A, "Event before refresh", 4));
  fixture.ticketRequests[0].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-refresh-independent")]));
  await Promise.all([fixture.requests[0].promise, fixture.ticketRequests[0].promise]);
  await flushPageAsync();

  const statsRefresh = find(driver.render(), (node) => node.type === "button" && textContent(node.props.children).trim() === "Refresh");
  assert.ok(statsRefresh);
  statsRefresh.props.onClick();
  driver.render();
  driver.runEffects();

  assert.equal(fixture.requests.length, 2, "statistics refresh should issue a new operations read");
  assert.equal(fixture.ticketRequests.length, 1, "statistics refresh does not refresh ticket records");
  fixture.requests[1].resolve(operations(EVENT_A, "Event after refresh", 5));
  await fixture.requests[1].promise;
  await flushPageAsync();
  assert(hasText(driver.render(), "Event after refresh"));
  driver.unmount();
});

test("ticket records filter, paginate and refresh in the current locale without locale-owned reads", async () => {
  const fixture = requestFixture();
  const localeState = { value: "en-US" };
  const driver = pageDriver(fixture, undefined, undefined, undefined, localeState);
  startPage(driver);
  assert.equal(fixture.requests.length, 1);
  assert.equal(fixture.ticketRequests.length, 1);
  assert(hasText(driver.render(), "Loading ticket records…"));
  assert.equal(fixture.ticketRequests[0].eventId, EVENT_A);
  assert.deepEqual(fixture.ticketRequests[0].params, { status: "ALL", page_size: 20 });

  fixture.requests[0].resolve(operations(EVENT_A, "Ticket event", 1));
  fixture.ticketRequests[0].resolve(ticketPage(EVENT_A, "ALL", [{
    ticket_id: "00000000-0000-4000-8000-000000000031",
    status: "ISSUED",
    issued_at: "2026-11-01T11:00:00Z",
    checked_in_at: null,
    revoked_at: null,
  }], { has_more: true, next_cursor: "first-page-cursor" }));
  await Promise.all([fixture.requests[0].promise, fixture.ticketRequests[0].promise]);
  await flushPageAsync();

  const english = driver.render();
  assert(hasText(english, "Ticket records"));
  assert(hasText(english, "Issued"));
  const englishStatsRefresh = find(english, (node) => node.type === "button" && textContent(node.props.children).trim() === "Refresh");
  const englishTicketRefresh = find(english, (node) => node.type === "button" && textContent(node.props.children).trim() === "Refresh ticket records");
  assert.ok(englishStatsRefresh);
  assert.ok(englishTicketRefresh);
  localeState.value = "zh-CN";
  const chinese = driver.render();
  assert(hasText(chinese, "票券记录"));
  assert(hasText(chinese, "已出票"));
  const chineseStatsRefresh = find(chinese, (node) => node.type === "button" && textContent(node.props.children).trim() === "刷新");
  const chineseTicketRefresh = find(chinese, (node) => node.type === "button" && textContent(node.props.children).trim() === "刷新票券记录");
  assert.ok(chineseStatsRefresh);
  assert.ok(chineseTicketRefresh);
  assert.notEqual(chineseStatsRefresh, chineseTicketRefresh);
  assert.equal(fixture.requests.length, 1, "locale changes must not refetch operations");
  assert.equal(fixture.ticketRequests.length, 1, "locale changes must not refetch ticket records");

  const statusFilter = find(chinese, (node) => node.type === "select" && node.props["aria-label"] === "按状态筛选票券");
  assert.ok(statusFilter);
  statusFilter.props.onChange({ target: { value: "ISSUED" } });
  driver.render();
  driver.runEffects();
  assert(hasText(driver.render(), "正在加载票券记录…"));
  assert.equal(fixture.ticketRequests.length, 2);
  assert.deepEqual(fixture.ticketRequests[1].params, { status: "ISSUED", page_size: 20 });
  fixture.ticketRequests[1].resolve(ticketPage(EVENT_A, "ISSUED", [{
    ticket_id: "00000000-0000-4000-8000-000000000032",
    status: "ISSUED",
    issued_at: "2026-11-01T10:00:00Z",
    checked_in_at: null,
    revoked_at: null,
  }], { has_more: true, next_cursor: "second-page-cursor" }));
  await fixture.ticketRequests[1].promise;
  await flushPageAsync();

  const loadMore = button(driver.render(), "加载更多");
  assert.ok(loadMore);
  loadMore.props.onClick();
  driver.render();
  driver.runEffects();
  assert(hasText(driver.render(), "正在加载更多票券…"));
  assert.equal(fixture.ticketRequests.length, 3);
  assert.deepEqual(fixture.ticketRequests[2].params, {
    status: "ISSUED",
    page_size: 20,
    cursor: "second-page-cursor",
  });
  fixture.ticketRequests[2].resolve(ticketPage(EVENT_A, "ISSUED", [], {
    has_more: false,
    next_cursor: null,
  }));
  await fixture.ticketRequests[2].promise;
  await flushPageAsync();

  const ticketPanel = find(driver.render(), (node) => node.type === "section" && node.props.id === "admin-event-ticket-records");
  const refresh = find(ticketPanel, (node) => node.type === "button" && textContent(node.props.children).trim() === "刷新票券记录");
  assert.ok(refresh);
  refresh.props.onClick();
  driver.render();
  driver.runEffects();
  assert.equal(fixture.ticketRequests.length, 4);
  assert.deepEqual(fixture.ticketRequests[3].params, { status: "ISSUED", page_size: 20 });
  driver.unmount();
});

test("ticket pagination starts at page one when the event changes", async () => {
  const fixture = requestFixture();
  const route = { id: EVENT_A };
  const driver = pageDriver(fixture, undefined, route);
  startPage(driver);

  fixture.ticketRequests[0].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-event-a-page-1")], {
    has_more: true,
    next_cursor: "event-a-cursor-1",
  }));
  await fixture.ticketRequests[0].promise;
  await flushPageAsync();
  button(driver.render(), "Load more").props.onClick();
  driver.render();
  driver.runEffects();
  fixture.ticketRequests[1].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-event-a-page-2")], {
    has_more: true,
    next_cursor: "event-a-cursor-2",
  }));
  await fixture.ticketRequests[1].promise;
  await flushPageAsync();

  route.id = EVENT_B;
  const switched = driver.render();
  assert.equal(hasText(switched, "ticket-event-a-page-1"), false);
  assert.equal(hasText(switched, "ticket-event-a-page-2"), false);
  driver.runEffects();
  assert.equal(fixture.ticketRequests.length, 3);
  assert.equal(fixture.ticketRequests[2].eventId, EVENT_B);
  assert.deepEqual(fixture.ticketRequests[2].params, { status: "ALL", page_size: 20 });

  fixture.ticketRequests[2].resolve(ticketPage(EVENT_B, "ALL", [ticket("ticket-event-b-page-1")]));
  await fixture.ticketRequests[2].promise;
  await flushPageAsync();
  const current = driver.render();
  assert(hasText(current, "ticket-event-b-page-1"));
  assert.equal(hasText(current, "ticket-event-a-page-1"), false);
  driver.unmount();
});

test("returning to an event after another event loaded cannot reactivate its stale cursor", async () => {
  const fixture = requestFixture();
  const route = { id: EVENT_A };
  const driver = pageDriver(fixture, undefined, route);
  startPage(driver);
  fixture.ticketRequests[0].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-return-a-old-page-1")], {
    has_more: true,
    next_cursor: "return-a-old-cursor-1",
  }));
  await fixture.ticketRequests[0].promise;
  await flushPageAsync();
  button(driver.render(), "Load more").props.onClick();
  driver.render();
  driver.runEffects();
  fixture.ticketRequests[1].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-return-a-old-page-2")], {
    has_more: true,
    next_cursor: "return-a-old-cursor-2",
  }));
  await fixture.ticketRequests[1].promise;
  await flushPageAsync();

  route.id = EVENT_B;
  driver.render();
  driver.runEffects();
  assert.deepEqual(fixture.ticketRequests[2].params, { status: "ALL", page_size: 20 });
  fixture.ticketRequests[2].resolve(ticketPage(EVENT_B, "ALL", [ticket("ticket-return-b-page-1")]));
  await fixture.ticketRequests[2].promise;
  await flushPageAsync();

  route.id = EVENT_A;
  const returning = driver.render();
  assert.equal(hasText(returning, "ticket-return-b-page-1"), false);
  driver.runEffects();
  assert.equal(fixture.ticketRequests.length, 4);
  assert.equal(fixture.ticketRequests[3].eventId, EVENT_A);
  assert.deepEqual(fixture.ticketRequests[3].params, { status: "ALL", page_size: 20 });
  fixture.ticketRequests[3].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-return-a-fresh-page-1")], {
    has_more: true,
    next_cursor: "return-a-fresh-cursor-1",
  }));
  await fixture.ticketRequests[3].promise;
  await flushPageAsync();
  assert(hasText(driver.render(), "ticket-return-a-fresh-page-1"));
  driver.runEffects();
  assert.equal(fixture.ticketRequests.length, 4, "the old event cursor must not start another page read");
  driver.unmount();
});

test("ticket pagination starts at page one when the session changes on the same event", async () => {
  const fixture = requestFixture();
  const authState = { user: { id: "admin-a", role: "admin" } };
  const driver = pageDriver(fixture, authState);
  startPage(driver);

  fixture.ticketRequests[0].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-session-page-1")], {
    has_more: true,
    next_cursor: "session-cursor-1",
  }));
  await fixture.ticketRequests[0].promise;
  await flushPageAsync();
  button(driver.render(), "Load more").props.onClick();
  driver.render();
  driver.runEffects();
  fixture.ticketRequests[1].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-session-page-2")], {
    has_more: true,
    next_cursor: "session-cursor-2",
  }));
  await fixture.ticketRequests[1].promise;
  await flushPageAsync();

  fixture.setSessionVersion(1);
  const switched = driver.render();
  assert.equal(hasText(switched, "ticket-session-page-1"), false);
  assert.equal(hasText(switched, "ticket-session-page-2"), false);
  driver.runEffects();
  assert.equal(fixture.ticketRequests.length, 3);
  assert.equal(fixture.ticketRequests[2].eventId, EVENT_A);
  assert.deepEqual(fixture.ticketRequests[2].params, { status: "ALL", page_size: 20 });

  fixture.ticketRequests[2].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-session-current-page-1")]));
  await fixture.ticketRequests[2].promise;
  await flushPageAsync();
  const current = driver.render();
  assert(hasText(current, "ticket-session-current-page-1"));
  assert.equal(hasText(current, "ticket-session-page-1"), false);
  driver.unmount();
});

test("reselecting the current ticket status starts a fresh first-page read", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture);
  startPage(driver);
  fixture.ticketRequests[0].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-status-reset-page-1")], {
    has_more: true,
    next_cursor: "status-reset-cursor-1",
  }));
  await fixture.ticketRequests[0].promise;
  await flushPageAsync();
  button(driver.render(), "Load more").props.onClick();
  driver.render();
  driver.runEffects();
  fixture.ticketRequests[1].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-status-reset-page-2")], {
    has_more: true,
    next_cursor: "status-reset-cursor-2",
  }));
  await fixture.ticketRequests[1].promise;
  await flushPageAsync();

  const statusFilter = find(driver.render(), (node) => node.type === "select" && node.props["aria-label"] === "Filter tickets by status");
  assert.ok(statusFilter);
  statusFilter.props.onChange({ target: { value: "ALL" } });
  const loading = driver.render();
  assert.equal(hasText(loading, "ticket-status-reset-page-1"), false);
  assert.equal(hasText(loading, "ticket-status-reset-page-2"), false);
  driver.runEffects();
  assert.equal(fixture.ticketRequests.length, 3);
  assert.deepEqual(fixture.ticketRequests[2].params, { status: "ALL", page_size: 20 });
  driver.unmount();
});

test("ticket retry after terminal auth or scope errors reloads page one without prior rows", async () => {
  for (const status of [401, 403, 404]) {
    const fixture = requestFixture();
    const driver = pageDriver(fixture);
    startPage(driver);

    fixture.ticketRequests[0].resolve(ticketPage(EVENT_A, "ALL", [ticket(`ticket-${status}-page-1`)], {
      has_more: true,
      next_cursor: `cursor-${status}-1`,
    }));
    await fixture.ticketRequests[0].promise;
    await flushPageAsync();
    button(driver.render(), "Load more").props.onClick();
    driver.render();
    driver.runEffects();
    fixture.ticketRequests[1].resolve(ticketPage(EVENT_A, "ALL", [ticket(`ticket-${status}-page-2`)], {
      has_more: true,
      next_cursor: `cursor-${status}-2`,
    }));
    await fixture.ticketRequests[1].promise;
    await flushPageAsync();

    button(driver.render(), "Load more").props.onClick();
    driver.render();
    driver.runEffects();
    assert.deepEqual(fixture.ticketRequests[2].params, {
      status: "ALL",
      page_size: 20,
      cursor: `cursor-${status}-2`,
    });
    fixture.ticketRequests[2].reject({ status });
    await assert.rejects(fixture.ticketRequests[2].promise);
    await flushPageAsync();
    const denied = driver.render();
    assert.equal(hasText(denied, `ticket-${status}-page-1`), false);
    assert.equal(hasText(denied, `ticket-${status}-page-2`), false);

    button(denied, "Retry").props.onClick();
    driver.render();
    driver.runEffects();
    assert.deepEqual(fixture.ticketRequests[3].params, { status: "ALL", page_size: 20 });
    fixture.ticketRequests[3].resolve(ticketPage(EVENT_A, "ALL", [ticket(`ticket-${status}-recovered-page-1`)], {
      has_more: true,
      next_cursor: `cursor-${status}-recovered-1`,
    }));
    await fixture.ticketRequests[3].promise;
    await flushPageAsync();
    const recovered = driver.render();
    assert(hasText(recovered, `ticket-${status}-recovered-page-1`));
    assert.equal(hasText(recovered, "No matching recorded tickets"), false);
    driver.unmount();
  }
});

test("ticket page overlap is rejected while the complete prior prefix stays visible", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture);
  startPage(driver);
  fixture.ticketRequests[0].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-overlap-id")], {
    has_more: true,
    next_cursor: "overlap-cursor-1",
  }));
  await fixture.ticketRequests[0].promise;
  await flushPageAsync();

  button(driver.render(), "Load more").props.onClick();
  driver.render();
  driver.runEffects();
  fixture.ticketRequests[1].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-overlap-id")], {
    has_more: false,
    next_cursor: null,
  }));
  await fixture.ticketRequests[1].promise;
  await flushPageAsync();

  const rejected = driver.render();
  assert(hasText(rejected, "Could not load more ticket records"));
  assert.equal(textContent(rejected).split("ticket-overlap-id").length - 1, 1);
  assert.equal(button(rejected, "Load more"), null);
  driver.unmount();
});

test("ticket pagination stops when the server repeats a previously used cursor", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture);
  startPage(driver);
  fixture.ticketRequests[0].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-repeated-cursor")], {
    has_more: true,
    next_cursor: "repeated-cursor",
  }));
  await fixture.ticketRequests[0].promise;
  await flushPageAsync();

  button(driver.render(), "Load more").props.onClick();
  driver.render();
  driver.runEffects();
  fixture.ticketRequests[1].resolve(ticketPage(EVENT_A, "ALL", [ticket("ticket-next-page")], {
    has_more: true,
    next_cursor: "repeated-cursor",
  }));
  await fixture.ticketRequests[1].promise;
  await flushPageAsync();

  const rejected = driver.render();
  assert(hasText(rejected, "Could not load more ticket records"));
  assert(hasText(rejected, "ticket-repeated-cursor"));
  assert.equal(hasText(rejected, "ticket-next-page"), false);
  assert.equal(button(rejected, "Load more"), null);
  driver.unmount();
});

test("ticket capacity copy covers event and audit limits without recommending a status filter", async () => {
  const fixture = requestFixture();
  const localeState = { value: "en-US" };
  const driver = pageDriver(fixture, undefined, undefined, undefined, localeState);
  startPage(driver);
  fixture.ticketRequests[0].reject({ code: "ADMIN_EVENT_OPERATIONS_CAPACITY_EXCEEDED" });
  await assert.rejects(fixture.ticketRequests[0].promise);
  await flushPageAsync();

  const english = driver.render();
  assert(hasText(english, "Event ticket or audit records exceed the safe read capacity"));
  assert(hasText(english, "Refresh after the source condition changes"));
  assert.equal(hasText(english, "Refine the status filter"), false);

  localeState.value = "zh-CN";
  const chinese = driver.render();
  assert(hasText(chinese, "活动票券或审计记录超出安全读取上限"));
  assert(hasText(chinese, "数据源条件变化后请刷新"));
  assert.equal(hasText(chinese, "缩小状态筛选范围"), false);
  driver.unmount();
});

test("event record region uses the localized accessible name", async () => {
  const fixture = requestFixture();
  const localeState = { value: "en-US" };
  const driver = pageDriver(fixture, undefined, undefined, undefined, localeState);
  startPage(driver);
  fixture.requests[0].resolve(operations(EVENT_A, "Event record label", 1));
  await fixture.requests[0].promise;
  await flushPageAsync();

  const english = driver.render();
  const englishRegion = find(english, (node) => node.type === "section" && node.props.className?.includes("sm:grid-cols-3"));
  assert.equal(englishRegion?.props["aria-label"], "Event record");
  localeState.value = "zh-CN";
  const chinese = driver.render();
  const chineseRegion = find(chinese, (node) => node.type === "section" && node.props.className?.includes("sm:grid-cols-3"));
  assert.equal(chineseRegion?.props["aria-label"], "活动记录");
  driver.unmount();
});

test("a valid zero result is shown as empty, separate from unavailable", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture);
  startPage(driver);
  fixture.requests[0].resolve(emptyOperations(EVENT_A));
  await fixture.requests[0].promise;
  await flushPageAsync();
  const ready = driver.render();
  assert(hasText(ready, "No recorded tickets for this event yet."));
  assert.equal(hasText(ready, "Event operations unavailable"), false);
  assert(hasText(ready, "Successful check-ins"));
  driver.unmount();
});

test("a refresh denied with 403 clears the prior statistics and uses fixed safe copy", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture);
  startPage(driver);
  fixture.requests[0].resolve(operations(EVENT_A, "Private event", 4));
  await fixture.requests[0].promise;
  await flushPageAsync();

  button(driver.render(), "Refresh").props.onClick();
  driver.render();
  driver.runEffects();
  assert.equal(hasText(driver.render(), "Private event"), false);
  fixture.requests[1].reject({ status: 403, message: "PRIVATE_FORBIDDEN_REASON" });
  await assert.rejects(fixture.requests[1].promise);
  await flushPageAsync();

  const denied = driver.render();
  assert(hasText(denied, "Your account cannot view event operations."));
  assert.equal(find(denied, (node) => node.type === "section" && node.props["aria-label"] === "Recorded ticket counts"), null);
  assert.equal(hasText(denied, "Private event"), false);
  driver.unmount();
});

test("a read deadline becomes a retryable unavailable error", async () => {
  const fixture = requestFixture();
  const driver = pageDriver(fixture);
  startPage(driver);
  assert.deepEqual(driver.scheduler.pendingDelays(), [10_000, 10_000]);
  driver.scheduler.runAll();
  await flushPageAsync();

  const timedOut = driver.render();
  assert(hasText(timedOut, "Event operations are temporarily unavailable. Try again."));
  assert.equal(hasText(timedOut, "Loading event operations"), false);
  assert.equal(fixture.requests[0].signal.aborted, true);
  assert(button(timedOut, "Retry read"));
  driver.unmount();
});
