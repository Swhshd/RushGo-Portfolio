import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { readFileSync } from "node:fs";
import { renderToStaticMarkup } from "react-dom/server";
import { loadTypeScript } from "./render-typescript.mjs";

const ticket = {
  id: "f6e20140-c1dc-4a10-b946-11680cdb7062",
  booking_id: "cdb9a4db-6aa7-4c58-97e6-dd9fae45c806",
  event_id: "c2edcaf1-eefd-49af-a44b-3ef9d7c1ac6b",
  seat_id: "80619946-8c22-49c5-b612-5588115a982c",
  ticket_tier_id: "d99fd885-d363-429d-a48c-eb500b32234a",
  status: "ISSUED",
  booking_status: "CONFIRMED",
  issued_at: "2026-10-22T10:00:00Z",
  checked_in_at: null,
};

test("wallet renders only server-issued ticket identity and requests the short-lived code explicitly", () => {
  const { WalletTicketCard } = loadTypeScript("components/wallet-ticket-card.tsx", {
    react: React,
    "@/lib/api/client": { apiClient: { rotateTicketCredential: async () => { throw new Error("not called during render"); } } },
  });
  const html = renderToStaticMarkup(React.createElement(WalletTicketCard, { ticket, eventTitle: "Night Lights", eventDate: "2026-12-15T20:00:00Z", refundAccess: "ALLOWED" }));
  assert.match(html, /Night Lights/);
  assert.match(html, /Seat reference/);
  assert.match(html, /Show entry QR/);
  assert.doesNotMatch(html, /<title>Entry credential for Night Lights<\/title>/);
  assert.doesNotMatch(html, /credential/);
  assert.doesNotMatch(html, /booking_id/);
});

test("ticket-to-order link carries the selected safe wallet filter", () => {
  const { WalletTicketCard } = loadTypeScript("components/wallet-ticket-card.tsx", {
    react: React,
    "@/lib/api/client": { apiClient: { rotateTicketCredential: async () => { throw new Error("not called during render"); } } },
  });
  const html = renderToStaticMarkup(React.createElement(WalletTicketCard, {
    ticket,
    eventTitle: "Night Lights",
    refundAccess: "ALLOWED",
    tab: "past",
  }));
  assert.match(html, /href="\/my-tickets\/orders\/cdb9a4db-6aa7-4c58-97e6-dd9fae45c806\?tab=past"/);
});

test("wallet disables QR actions and explains a cancelled Booking while preserving Ticket persistence", () => {
  const { WalletTicketCard } = loadTypeScript("components/wallet-ticket-card.tsx", {
    react: React,
    "@/lib/api/client": { apiClient: { rotateTicketCredential: async () => { throw new Error("not called during render"); } } },
  });
  const html = renderToStaticMarkup(React.createElement(WalletTicketCard, {
    ticket: { ...ticket, booking_status: "CANCELLED" },
    eventTitle: "Night Lights",
    refundAccess: "ALLOWED",
  }));
  assert.match(html, /Ticket unavailable — booking cancelled/);
  assert.match(html, /disabled=""/);
  assert.doesNotMatch(html, /Refresh code/);
  assert.equal(ticket.status, "ISSUED"); // the persisted Ticket state remains unchanged
});

test("wallet hides ticket QR while an authoritative refund obligation blocks entry", () => {
  const { WalletTicketCard } = loadTypeScript("components/wallet-ticket-card.tsx", {
    react: React,
    "@/lib/api/client": { apiClient: { rotateTicketCredential: async () => { throw new Error("not called during render"); } } },
  });
  const html = renderToStaticMarkup(React.createElement(WalletTicketCard, {
    ticket,
    eventTitle: "Night Lights",
    refundAccess: "BLOCKED",
  }));
  assert.match(html, /Refund blocks entry/);
  assert.match(html, /No QR is available/);
  assert.doesNotMatch(html, /Show entry QR/);
});

test("wallet fails closed when Booking status is missing or unknown", () => {
  const { WalletTicketCard } = loadTypeScript("components/wallet-ticket-card.tsx", {
    react: React,
    "@/lib/api/client": { apiClient: { rotateTicketCredential: async () => { throw new Error("not called during render"); } } },
  });
  for (const bookingStatus of [undefined, "FUTURE_STATUS"]) {
    const html = renderToStaticMarkup(React.createElement(WalletTicketCard, {
      ticket: { ...ticket, booking_status: bookingStatus },
      eventTitle: "Night Lights",
      refundAccess: "ALLOWED",
    }));
    assert.match(html, /Entry eligibility unavailable/);
    assert.match(html, /disabled=""/);
    assert.doesNotMatch(html, /Refresh code/);
  }
});

test("wallet cards subscribe to the shared expiry clock only while a credential is visible", () => {
  const source = readFileSync(new URL("../components/wallet-ticket-card.tsx", import.meta.url), "utf8");
  assert.ok(source.includes("const clockSubscriptionEnabled = credentialEligible && pass !== null;"));
  assert.ok(source.includes("const subscribeToClock = useCallback("));
  assert.ok(source.includes("clockSubscriptionEnabled ? subscribeTicketClock(listener) : () => {}"));
  assert.ok(source.includes("useSyncExternalStore(subscribeToClock, getClockSnapshot, getTicketClockServerSnapshot)"));
});

test("wallet API client marks credential reads and scans as no-store operations", () => {
  const source = readFileSync(new URL("../lib/api/client.ts", import.meta.url), "utf8");
  assert.match(source, /rotateTicketCredential[\s\S]*?cache: "no-store"/);
  assert.match(source, /rotateTicketCredential[\s\S]*?Idempotency-Key/);
  assert.match(source, /checkInTicket[\s\S]*?cache: "no-store"/);
});

test("wallet accepts only the newest credential version across tabs", () => {
  const { canDisplayCredentialVersion, highestCredentialVersion, ticketCredentialPendingStorageKey } = loadTypeScript("lib/ticket-credential-version.ts");
  assert.equal(canDisplayCredentialVersion(3, 2), false);
  assert.equal(canDisplayCredentialVersion(3, 3), true);
  assert.equal(canDisplayCredentialVersion(3, 4), true);
  assert.equal(canDisplayCredentialVersion(3, Number.NaN), false);
  assert.equal(highestCredentialVersion(4, 2), 4);
  assert.equal(highestCredentialVersion(4, 6), 6);
  assert.equal(ticketCredentialPendingStorageKey(ticket.id), `rushgo:ticket-credential-pending:${ticket.id}`);

  const source = readFileSync(new URL("../components/wallet-ticket-card.tsx", import.meta.url), "utf8");
  const storage = readFileSync(new URL("../lib/ticket-credential-storage.ts", import.meta.url), "utf8");
  assert.match(source, /BroadcastChannel\("rushgo-ticket-credential-version"\)/);
  assert.match(source, /ALREADY_APPLIED/);
  assert.match(storage, /sessionStorage\.setItem/);
  assert.match(source, /retry this request with the same key/i);
});

test("pending rotation keys hydrate after render and survive a blocked storage write in memory", () => {
  const originalWindow = globalThis.window;
  const values = new Map([[`rushgo:ticket-credential-pending:${ticket.id}`, "stored-key"]]);
  globalThis.window = { sessionStorage: {
    getItem: key => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
    removeItem: key => values.delete(key),
  } };
  try {
    const storage = loadTypeScript("lib/ticket-credential-storage.ts");
    assert.deepEqual(storage.getPendingRotationKeySnapshot(ticket.id), { loaded: false, key: null });
    let changes = 0;
    const unsubscribe = storage.subscribePendingRotationKey(ticket.id, () => { changes++; });
    assert.deepEqual(storage.getPendingRotationKeySnapshot(ticket.id), { loaded: true, key: "stored-key" });
    storage.setPendingRotationKey(ticket.id, "retry-key");
    assert.equal(storage.getPendingRotationKeySnapshot(ticket.id).key, "retry-key");
    assert.equal(values.get(`rushgo:ticket-credential-pending:${ticket.id}`), "retry-key");
    assert(changes >= 2);
    unsubscribe();

    globalThis.window.sessionStorage.setItem = () => { throw new Error("storage unavailable"); };
    storage.setPendingRotationKey(ticket.id, "in-memory-retry-key");
    assert.equal(storage.getPendingRotationKeySnapshot(ticket.id).key, "in-memory-retry-key");
  } finally {
    if (originalWindow === undefined) delete globalThis.window; else globalThis.window = originalWindow;
  }
});

test("ticket rotation errors keep retryable authority failures distinct from permanent ineligibility", () => {
  const { responseError } = loadTypeScript("lib/api/errors.ts");
  assert.equal(responseError("/tickets/id/credential", 503, "temporarily unavailable", "ticket_authority_unavailable").code, "TICKET_AUTHORITY_UNAVAILABLE");
  assert.equal(responseError("/tickets/id/credential", 409, "retry with same key", "idempotency_in_progress").code, "TICKET_IDEMPOTENCY_IN_PROGRESS");
  assert.equal(responseError("/tickets/id/credential", 409, "not eligible", "ticket_not_eligible").code, "TICKET_NOT_ELIGIBLE");
  assert.equal(responseError("/tickets/id/credential", 409, "key conflict", "idempotency_conflict").code, "TICKET_IDEMPOTENCY_CONFLICT");
});

test("a replayed rotation receipt hides an older code in another tab without restoring a token", async () => {
  const originalWindow = globalThis.window, originalChannel = globalThis.BroadcastChannel;
  const values = new Map(), channels = [];
  const storage = { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) };
  globalThis.BroadcastChannel = class {
    constructor() { channels.push(this); }
    postMessage(data) { for (const channel of channels) if (channel !== this) channel.onmessage?.({ data }); }
    close() {}
  };
  function tab(response, pending) {
    const slots = [], effects = [], session = new Map(pending ? [[`rushgo:ticket-credential-pending:${ticket.id}`, pending]] : []);
    let cursor = 0, calls = 0;
    const window = { localStorage: storage, sessionStorage: { getItem: key => session.get(key) ?? null, setItem: (key, value) => session.set(key, value), removeItem: key => session.delete(key) }, addEventListener() {}, removeEventListener() {} };
    const hooks = { ...React,
      useContext(context) { return context._currentValue; },
      useCallback(callback) { cursor++; return callback; },
      useState(initial) { const index = cursor++; if (!(index in slots)) slots[index] = typeof initial === "function" ? initial() : initial; return [slots[index], value => { slots[index] = typeof value === "function" ? value(slots[index]) : value; }]; },
      useRef(initial) { const index = cursor++; return slots[index] ||= { current: initial }; },
      useSyncExternalStore(subscribe, getSnapshot) { cursor++; const unsubscribe = subscribe(() => {}); const value = getSnapshot(); unsubscribe(); return value; },
      useEffect(effect) { effects.push(effect); },
    };
    const { WalletTicketCard } = loadTypeScript("components/wallet-ticket-card.tsx", {
      react: hooks,
      "@/lib/api/client": { apiClient: { rotateTicketCredential: async () => { calls++; return response; } } },
      "@/lib/ticket-clock": {
        subscribeTicketClock: () => () => {},
        getTicketClockSnapshot: () => Date.now(),
        getTicketClockServerSnapshot: () => 0,
        getTicketClockNow: () => Date.now(),
        refreshTicketClock() {},
      },
    });
    const render = () => {
      globalThis.window = window;
      cursor = 0;
      const element = WalletTicketCard({ ticket, eventTitle: "Night Lights", refundAccess: "ALLOWED" });
      return element.type(element.props);
    };
    render(); effects.splice(0).forEach(effect => effect());
    return { render, calls: () => calls };
  }
  function find(tree, predicate) {
    if (!tree || typeof tree !== "object") return null;
    if (predicate(tree)) return tree;
    for (const child of React.Children.toArray(tree.props?.children)) { const found = find(child, predicate); if (found) return found; }
    return null;
  }
  try {
    const expires_at = new Date(Date.now() + 60_000).toISOString();
    const a = tab({ outcome: "ROTATED", credential: "synthetic-v1", credential_version: 1, expires_at });
    const b = tab({ outcome: "ALREADY_APPLIED", credential_version: 2, expires_at }, "lost-response-key");
    await find(a.render(), node => node.type === "button").props.onClick();
    assert(find(a.render(), node => node.props?.value === "synthetic-v1"));
    await find(b.render(), node => node.type === "button").props.onClick();
    assert.equal(values.get(`rushgo:ticket-credential-version:${ticket.id}`), "2");
    assert.equal(find(a.render(), node => node.props?.value === "synthetic-v1"), null);
    assert.equal(find(b.render(), node => node.props?.value), null);
    assert.equal(b.calls(), 1);
  } finally {
    if (originalWindow === undefined) delete globalThis.window; else globalThis.window = originalWindow;
    globalThis.BroadcastChannel = originalChannel;
  }
});
