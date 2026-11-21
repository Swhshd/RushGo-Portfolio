import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { loadTypeScript, browserOverrides } from "./render-typescript.mjs";
import { cart, provider } from "./booking-provider.mjs";

function driver(file, overrides, seed = {}) {
  const slots = [], effects = [];
  let cursor = 0;
  const hooks = { ...React,
    useContext(context) { return context._currentValue; },
    useState(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = index in seed ? seed[index] : typeof initial === "function" ? initial() : initial;
      return [slots[index], value => { slots[index] = typeof value === "function" ? value(slots[index]) : value; }];
    },
    useRef(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = index in seed ? seed[index] : { current: initial };
      return slots[index];
    },
    useMemo: fn => fn(), useCallback: fn => fn,
    useEffect: fn => { effects.push(fn); },
    useSyncExternalStore(_subscribe, getSnapshot) { return getSnapshot(); },
  };
  const component = loadTypeScript(file, { ...browserOverrides, ...overrides, react: hooks });
  const render = () => { cursor = 0; let tree = (component.default || component.AuthProvider)({ children: null });
    // Login's production Suspense boundary contains its query-reading form.
    if (tree.type === React.Suspense) tree = tree.props.children.type(tree.props.children.props);
    return tree;
  };
  return { render, effects };
}
function find(tree, predicate) {
  if (!tree || typeof tree !== "object") return null;
  if (predicate(tree)) return tree;
  for (const child of React.Children.toArray(tree.props?.children)) { const found = find(child, predicate); if (found) return found; }
  return null;
}
function seats(user, initiateBooking, extraAuth = {}, authoritativeStatuses = ["available"]) {
  const pushes = [], mutations = [];
  globalThis.window ||= { sessionStorage: globalThis.sessionStorage };
  if (!Array.isArray(authoritativeStatuses)) authoritativeStatuses = [authoritativeStatuses];
  const authoritativeSeats = authoritativeStatuses.map((status, index) => ({
    id: index === 0 ? "seat" : `seat-${index + 1}`, event_id: "event", ticket_tier_id: "tier-1", status,
    position: { sectionId: "Floor-center", row: "A", seat: index + 1, x: 200 + index * 20, y: 200 },
    created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z", seat_revision: "1",
  }));
  let snapshotCalls = 0;
  const section = { id: "Floor-center", name: "Floor", tier: "Floor", tierId: "tier-1", price: 25, color: "#E8567F", path: "", labelPosition: { x: 200, y: 200 }, totalSeats: authoritativeSeats.length, availableSeats: authoritativeStatuses.filter((status) => status === "available").length, accountingReady: true, rows: [{ id: "Floor-center-A", label: "Row A", seats: authoritativeSeats.map((seat, index) => ({ id: seat.id, label: String(index + 1), status: "available", seatRevision: "1" })) }] };
  const initialSelections = authoritativeSeats.map((seat, index) => ({ ...cart.seats[0], seatId: seat.id, seatLabel: String(index + 1), sectionId: "Floor-center", tierId: "tier-1", price: 25 }));
  const d = driver("app/(customer)/events/[id]/seats/page.tsx", {
    "next/navigation": { useParams: () => ({ id: "event" }), useRouter: () => ({ push: href => pushes.push(href) }) },
    "@/lib/auth-context": { useAuth: () => ({ user, isLoading: false, ...extraAuth }) },
    "@/lib/api/client": { apiClient: { getSeats: async () => { snapshotCalls++; return { seats: authoritativeSeats }; } } },
    "@/lib/booking-context": { useBooking: () => ({ getEvent: () => ({ id: "event", title: "Demo", category: "concerts", venue: "Venue", tiers: [{ id: "tier-1", name: "Floor", description: "10 of 12 remaining", price: 25, available: 10, accountingReady: true }] }), isLoading: false,
      setCart: value => mutations.push(["cart", value]), clearPaymentState: () => mutations.push(["payment"]), initiateBooking, refreshEvents: async () => {} }) },
  }, {
    0: null,
    1: null, 2: initialSelections, 3: { sections: [section] }, 4: { eventId: "event", state: "ready" }, 5: "event", 6: { eventId: "event", phase: "LIVE" },
    7: { current: "LIVE" }, 8: 0, 9: { current: true }, 10: { current: "event" },
    11: false, 12: null, 13: { current: false }, 14: { current: authoritativeSeats }, 15: { current: false },
  });
  return { ...d, pushes, mutations, get snapshotCalls() { return snapshotCalls; } };
}
function continueButton(tree) {
  return find(tree, n => n.type === "button" && JSON.stringify(n.props.children).includes("Continue to Checkout"));
}

test("logged-out Continue redirects to this seats route before any booking, cart or payment initialization", async () => {
  let bookingCalls = 0;
  const d = seats(null, async () => { bookingCalls++; return { bookingId: "booking" }; });
  await continueButton(d.render()).props.onClick();
  assert.equal(bookingCalls, 0);
  assert.deepEqual(d.mutations, []);
  assert.equal(d.pushes.length, 1);
  const target = new URL(d.pushes[0], "https://rushgo.example");
  assert.equal(target.pathname, "/login");
  assert.equal(target.searchParams.get("redirect"), "/events/event/seats");
  assert.equal(target.searchParams.get("reason"), "AUTH_REQUIRED");
});

test("authenticated Continue still starts booking then opens checkout", async () => {
  let calls = 0; const d = seats({ id: "user" }, async () => { calls++; return { bookingId: "booking" }; });
  await continueButton(d.render()).props.onClick();
  assert.equal(calls, 1); assert.deepEqual(d.pushes, ["/checkout"]);
  assert.equal(d.snapshotCalls, 1);
});

test("authenticated Continue rejects a seat the server now reports as reserved", async () => {
  let calls = 0; const d = seats({ id: "user" }, async () => { calls++; return { bookingId: "booking" }; }, {}, "reserved");
  await continueButton(d.render()).props.onClick();
  assert.equal(d.snapshotCalls, 1);
  assert.equal(calls, 0);
  assert.deepEqual(d.pushes, []);
  assert.equal(continueButton(d.render()).props.disabled, true);
});

test("invalidated seats require an explicit choice before continuing with the remaining seats", async () => {
  let calls = 0;
  const d = seats({ id: "user" }, async () => { calls++; return { bookingId: "booking" }; }, {}, ["available", "reserved"]);
  await continueButton(d.render()).props.onClick();
  assert.equal(calls, 0);
  assert.equal(continueButton(d.render()).props.disabled, true);
  const acceptRemaining = find(d.render(), node => node.type === "button" && node.props?.children === "Continue with the remaining seats");
  assert(acceptRemaining);
  acceptRemaining.props.onClick();
  assert.equal(continueButton(d.render()).props.disabled, false);
  await continueButton(d.render()).props.onClick();
  assert.equal(calls, 1);
  assert.equal(d.snapshotCalls, 2);
  assert.deepEqual(d.pushes, ["/checkout"]);
});

test("session failure from booking navigates login with expiry reason instead of payment error", async () => {
  const d = seats({ id: "user" }, async () => { throw Object.assign(new Error("Session expired"), { code: "SESSION_EXPIRED" }); });
  await continueButton(d.render()).props.onClick();
  const target = new URL(d.pushes[0], "https://rushgo.example");
  assert.equal(target.pathname, "/login"); assert.equal(target.searchParams.get("reason"), "SESSION_EXPIRED");
  assert.equal(target.searchParams.get("redirect"), "/events/event/seats");
});
test("credentials disappearing behind a logged-in UI are reconciled as session expiry", async () => {
  const d = seats({ id: "user" }, async () => { throw Object.assign(new Error("Missing credentials"), { code: "AUTH_REQUIRED" }); });
  await continueButton(d.render()).props.onClick();
  assert.equal(new URL(d.pushes[0], "https://rushgo.example").searchParams.get("reason"), "SESSION_EXPIRED");
});

test("booking provider rejects logged-out invocation before createBooking", async () => {
  let calls = 0; const render = provider(async () => ({}), { user: null, createBooking: async () => { calls++; return {}; } });
  await render().ensureEvent(cart.eventId);
  await assert.rejects(render().initiateBooking(cart), e => e.code === "AUTH_REQUIRED");
  assert.equal(calls, 0);
});

for (const [code, message] of [["BOOKING_CONFLICT", "Some selected seats are no longer available."], ["ACCOUNTING_NOT_READY", "This ticket tier is not available for sale right now."], ["BOOKING_ERROR", "Your booking could not be started."], ["PAYMENT_INIT_ERROR", "Failed to initiate payment."]]) {
  test(`Seats shows the ${code} outcome without replacing it with another failure`, async () => {
    const d = seats({ id: "user" }, async () => { throw Object.assign(new Error("backend error"), { code }); });
    await continueButton(d.render()).props.onClick();
    assert.deepEqual(d.pushes, []);
    assert(JSON.stringify(d.render()).includes(message));
    if (code !== "PAYMENT_INIT_ERROR") assert(!JSON.stringify(d.render()).includes("Failed to initiate payment"));
    if (code === "ACCOUNTING_NOT_READY") assert(!JSON.stringify(d.render()).includes("no longer available"));
  });
}

test("a successful booking response missing its payment secret is a payment initiation error", async () => {
  const render = provider(async () => ({}), { createBooking: async () => ({ id: "booking", total_amount_cents: 1000 }) });
  await render().ensureEvent(cart.eventId);
  await assert.rejects(render().initiateBooking(cart), e => e.code === "PAYMENT_INIT_ERROR");
  assert.equal(render().paymentClientSecret, null);
});

for (const target of ["https://evil.example.com", "//evil.example.com", "/\\evil.example.com", "/%2f%2fevil.example.com", "javascript:alert(1)", "/.//evil.example", "/events/..//evil.example", "/%2e//evil.example"]) {
  test(`login rejects unsafe redirect ${target}`, async () => {
    const pushes = []; const query = new URLSearchParams({ redirect: target });
    globalThis.window = { location: { search: `?${query}` } };
    const d = driver("app/(customer)/login/page.tsx", {
      "next/navigation": { useRouter: () => ({ push: href => pushes.push(href) }), useSearchParams: () => query },
      "@/lib/auth-context": { useAuth: () => ({ login: async () => ({ role: "customer" }) }) },
    });
    await find(d.render(), n => n.type === "form").props.onSubmit({ preventDefault() {} });
    assert.deepEqual(pushes, ["/"]); delete globalThis.window;
  });
}
test("login preserves safe seats redirect and displays the bounded auth reason", async () => {
  const pushes = [], query = new URLSearchParams({ redirect: "/events/event/seats", reason: "SESSION_EXPIRED" });
  globalThis.window = { location: { search: `?${query}` } };
  const d = driver("app/(customer)/login/page.tsx", {
    "next/navigation": { useRouter: () => ({ push: href => pushes.push(href) }), useSearchParams: () => query },
    "@/lib/auth-context": { useAuth: () => ({ login: async () => ({ role: "customer" }) }) },
  });
  assert(JSON.stringify(d.render()).includes("Your session has expired. Please sign in again."));
  await find(d.render(), n => n.type === "form").props.onSubmit({ preventDefault() {} });
  assert.deepEqual(pushes, ["/events/event/seats"]); delete globalThis.window;
});

test("AuthProvider immediately clears its user when the API session becomes invalid", async () => {
  let invalidated; const pushes = [];
  globalThis.window = { location: { pathname: "/events/event/seats", search: "" }, addEventListener() {}, removeEventListener() {} };
  const d = driver("lib/auth-context.tsx", { "next/navigation": { useRouter: () => ({ push: href => pushes.push(href) }) }, "./api/client": { apiClient: {
    isLoggedIn: () => true, getSessionVersion: () => 1, getProfile: async () => ({ id: "user", name: "Demo" }),
    subscribeSessionExpired: callback => { invalidated = callback; return () => {}; },
  } } });
  d.render(); for (const effect of [...d.effects]) effect(); await new Promise(resolve => setImmediate(resolve));
  assert.equal(d.render().props.value.user.id, "user");
  assert.equal(typeof invalidated, "function"); invalidated();
  assert.equal(d.render().props.value.user, null);
  assert.equal(new URL(pushes[0], "https://rushgo.example").searchParams.get("reason"), "SESSION_EXPIRED");
  assert.equal(new URL(pushes[0], "https://rushgo.example").searchParams.get("redirect"), "/events/event/seats");
  delete globalThis.window;
});

test("AuthProvider replaces stale user state when another tab changes the session", async () => {
  const listeners = new Map(); let sessionVersion = 0; let profileCall = 0;
  let resolveOldProfile, resolveNewProfile;
  globalThis.window = {
    location: { pathname: "/my-tickets", search: "" },
    addEventListener: (name, callback) => listeners.set(name, callback),
    removeEventListener: name => listeners.delete(name),
  };
  const d = driver("lib/auth-context.tsx", {
    "next/navigation": { useRouter: () => ({ push() {} }) },
    "./api/client": { apiClient: {
      isLoggedIn: () => true,
      getSessionVersion: () => sessionVersion,
      notifyExternalSessionChange: () => ++sessionVersion,
      getProfile: () => new Promise(resolve => {
        profileCall++;
        if (profileCall === 1) resolveOldProfile = resolve;
        else resolveNewProfile = resolve;
      }),
      subscribeSessionExpired: () => () => {},
    } },
  });
  d.render(); for (const effect of [...d.effects]) effect();
  await Promise.resolve();
  assert.equal(profileCall, 1);

  listeners.get("storage")({ key: "access_token", oldValue: "token-a", newValue: "token-b" });
  let auth = d.render().props.value;
  assert.equal(auth.user, null);
  assert.equal(auth.isLoading, true);
  assert.equal(profileCall, 2);

  resolveOldProfile({ id: "user-a", email: "a@example.test", name: "Account A", role: "customer" });
  await new Promise(resolve => setImmediate(resolve));
  auth = d.render().props.value;
  assert.equal(auth.user, null, "late profile from the previous tab session must not restore the old account");
  assert.equal(auth.isLoading, true);

  resolveNewProfile({ id: "user-b", email: "b@example.test", name: "Account B", role: "customer" });
  await new Promise(resolve => setImmediate(resolve));
  auth = d.render().props.value;
  assert.equal(auth.user.id, "user-b");
  assert.equal(auth.isLoading, false);
  delete globalThis.window;
});

test("login signup link preserves a safe seat redirect for registration", () => {
  const query = new URLSearchParams({ redirect: "/events/event-1/seats", reason: "AUTH_REQUIRED" });
  globalThis.window = { location: { search: "?" + query.toString() } };
  const d = driver("app/(customer)/login/page.tsx", {
    "next/navigation": { useRouter: () => ({ push() {} }), useSearchParams: () => query },
    "@/lib/auth-context": { useAuth: () => ({ login: async () => ({ role: "customer" }) }) },
  });
  const signup = find(d.render(), node => node.props?.href?.startsWith("/register"));
  assert.equal(signup.props.href, "/register?redirect=%2Fevents%2Fevent-1%2Fseats");
  delete globalThis.window;
});
