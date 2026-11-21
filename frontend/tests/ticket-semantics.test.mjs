import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { readFileSync } from "node:fs";
import { renderToStaticMarkup } from "react-dom/server";
import { browserOverrides, loadTypeScript } from "./render-typescript.mjs";

const { transformBookingToTicket, transformEvent } = loadTypeScript("lib/api/transformers.ts");
const booking = (status) => ({ id: "booking", event_id: "event", status, total_amount_cents: 1000, created_at: "2026-01-01T00:00:00Z", items: [{ quantity: 1, unit_price_cents: 1000, tier_name: "Floor" }] });

for (const status of ["CONFIRMED", "confirmed", "Confirmed"]) {
  test(`${status} remains a confirmed booking projection, not an issued Ticket`, () => {
    assert.equal(transformBookingToTicket(booking(status)).status, "Confirmed");
  });
}

for (const status of ["PENDING", "EXPIRED", "FAILED", "ROLLED_BACK", "CANCELLED", "PROCESSING", "UNKNOWN", "", null]) {
  test(`${status} is not eligible for a confirmed booking projection`, () => {
    assert.equal(transformBookingToTicket(booking(status)), null);
  });
}

test("event transformation retains the authoritative timestamp with its timezone", () => {
  const raw = { id: "event", title: "Demo", date: "2026-12-15T20:00:00-05:00", tiers: [], category: "concerts" };
  assert.equal(transformEvent(raw).startsAt, raw.date);
});

test("my tickets waits for auth restoration before showing the sign-in prompt", () => {
  const { default: MyTickets } = loadTypeScript("app/(customer)/my-tickets/page.tsx", {
    ...browserOverrides,
    react: { ...React, useSyncExternalStore: () => true },
    "@/lib/auth-context": { useAuth: () => ({ user: null, isLoading: true }) },
    "@/lib/api/client": { apiClient: {} },
    "@/lib/booking-context": { useBooking: () => ({ lastPurchaseSuccess: false, clearSuccessFlag() {} }) },
  });
  const html = renderToStaticMarkup(React.createElement(MyTickets));
  assert.match(html, /Checking your session/);
  assert.doesNotMatch(html, /Sign in to view your orders/);
});

test("signed-out wallet links to login with a preserved redirect", () => {
  const { default: MyTickets } = loadTypeScript("app/(customer)/my-tickets/page.tsx", {
    ...browserOverrides,
    react: { ...React, useSyncExternalStore: () => true },
    "@/lib/auth-context": { useAuth: () => ({ user: null, isLoading: false }) },
    "@/lib/api/client": { apiClient: {} },
    "@/lib/booking-context": { useBooking: () => ({ lastPurchaseSuccess: false, clearSuccessFlag() {} }) },
  });
  const html = renderToStaticMarkup(React.createElement(MyTickets));
  assert.match(html, /Sign in to view your orders and tickets/);
  assert.match(html, />Sign In</);
  assert.match(html, /redirect=%2Fmy-tickets/);
  assert.match(html, /reason=AUTH_REQUIRED/);
});

test("signed-out wallet preserves an allowlisted filter in the login redirect", () => {
  const { default: MyTickets } = loadTypeScript("app/(customer)/my-tickets/page.tsx", {
    ...browserOverrides,
    "next/navigation": {
      useRouter: () => ({ replace() {}, push() {} }),
      useSearchParams: () => new URLSearchParams("tab=upcoming"),
    },
    react: { ...React, useSyncExternalStore: () => true },
    "@/lib/auth-context": { useAuth: () => ({ user: null, isLoading: false }) },
    "@/lib/api/client": { apiClient: {} },
    "@/lib/booking-context": { useBooking: () => ({ lastPurchaseSuccess: false, clearSuccessFlag() {} }) },
  });
  const html = renderToStaticMarkup(React.createElement(MyTickets));
  assert.match(html, /redirect=%2Fmy-tickets%3Ftab%3Dupcoming/);
});

test("signed-out wallet sign-in action is localized in Simplified Chinese", () => {
  const { getI18n } = loadTypeScript("lib/i18n/index.ts", { react: React });
  const { default: MyTickets } = loadTypeScript("app/(customer)/my-tickets/page.tsx", {
    ...browserOverrides,
    react: { ...React, useSyncExternalStore: () => true },
    "@/lib/i18n/index": { useI18n: () => getI18n("zh-CN") },
    "@/lib/auth-context": { useAuth: () => ({ user: null, isLoading: false }) },
    "@/lib/api/client": { apiClient: {} },
    "@/lib/booking-context": { useBooking: () => ({ lastPurchaseSuccess: false, clearSuccessFlag() {} }) },
  });
  const html = renderToStaticMarkup(React.createElement(MyTickets));
  assert.match(html, /登录/);
  assert.doesNotMatch(html, />Sign In</);
});

test("signed-in wallet does not render context/mock tickets while the API is loading", () => {
  const { default: MyTickets } = loadTypeScript("app/(customer)/my-tickets/page.tsx", {
    ...browserOverrides,
    react: { ...React, useSyncExternalStore: () => true },
    "@/lib/auth-context": { useAuth: () => ({ user: { id: "user-1", name: "Demo" }, isLoading: false }) },
    "@/lib/api/client": { apiClient: {} },
    "@/lib/booking-context": {
      useBooking: () => ({
        purchasedTickets: [{ id: "context-only", title: "Context Only Ticket" }],
        lastPurchaseSuccess: false,
        clearSuccessFlag() {},
      }),
    },
  });
  const html = renderToStaticMarkup(React.createElement(MyTickets));
  assert.match(html, /Loading your orders/);
  assert.doesNotMatch(html, /Context Only Ticket/);
  assert.doesNotMatch(html, /No orders yet/);
});

test("wallet data is keyed to the authenticated user and purchase notice has an owner", () => {
  const page = readFileSync(new URL("../app/(customer)/my-tickets/page.tsx", import.meta.url), "utf8");
  const booking = readFileSync(new URL("../lib/booking-context.tsx", import.meta.url), "utf8");
  assert.match(page, /key=\{user\.id\}/);
  assert.match(page, /lastPurchaseSuccessUserId === user\.id/);
  assert.match(booking, /lastPurchaseSuccessUserId: string \| null/);
  assert.match(booking, /setLastPurchaseSuccessUserId\(userId \?\? null\)/);
  assert.ok(
    booking.includes("}, [pendingBookingId, cart, events, clearPaymentState, fetchEvent, setCart, userId]);"),
    "confirmPayment must track the user whose purchase success it attributes"
  );
});

test("My Tickets keeps the matching user's purchase notice after clearing provider state", () => {
  const page = readFileSync(new URL("../app/(customer)/my-tickets/page.tsx", import.meta.url), "utf8");
  assert.ok(page.includes("const [purchaseNoticeVisible, setPurchaseNoticeVisible] = useState(showSuccess);"));
  assert.ok(page.includes("{purchaseNoticeVisible && !error && ("));
});
