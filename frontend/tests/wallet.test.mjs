import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { loadTypeScript } from "./render-typescript.mjs";

const wallet = () => loadTypeScript("lib/wallet.ts");

const event = {
  id: "event-1",
  title: "Reliability Night",
  description: "",
  category: "concerts",
  venue: "RushGo Arena",
  location: "Ho Chi Minh City",
  date: "2026-12-15T20:00:00Z",
  image_url: "",
  status: "active",
  tiers: [
    {
      id: "tier-1",
      event_id: "event-1",
      name: "Floor",
      price_cents: 2500,
      total_quantity: 12,
      available_quantity: 10,
      version: 1,
    },
  ],
  created_at: "2026-10-01T00:00:00Z",
};

const booking = (overrides = {}) => ({
  id: "booking-1",
  event_id: "event-1",
  user_id: "user-1",
  status: "CONFIRMED",
  total_amount_cents: 5000,
  created_at: "2026-10-21T10:00:00Z",
  items: [
    {
      id: "item-1",
      ticket_tier_id: "tier-1",
      tier_name: "",
      quantity: 2,
      unit_price_cents: 2500,
      seat_ids: ["seat-1", "seat-2"],
    },
  ],
  ...overrides,
});

test("wallet statuses keep the authoritative enum and never guess unknown states", () => {
  const { normalizeWalletStatus, walletStatusLabel, walletStatusTone } = wallet();
  assert.equal(normalizeWalletStatus("confirmed"), "CONFIRMED");
  assert.equal(normalizeWalletStatus(" PENDING "), "PENDING");
  assert.equal(normalizeWalletStatus("FAILED"), "FAILED");
  assert.equal(normalizeWalletStatus("WAIT_FOR_PAYMENT"), "UNKNOWN");
  assert.equal(normalizeWalletStatus(""), "UNKNOWN");
  assert.equal(walletStatusLabel("PENDING"), "Pending");
  assert.equal(walletStatusLabel("UNKNOWN"), "Unknown");
  assert.equal(walletStatusTone("CONFIRMED"), "success");
  assert.equal(walletStatusTone("EXPIRED"), "danger");
});

test("tier names come from the booking or the joined event, never a fake General", () => {
  const { resolveTierName } = wallet();
  assert.equal(resolveTierName({ tier_name: "Balcony", ticket_tier_id: "tier-1" }, event), "Balcony");
  assert.equal(resolveTierName({ tier_name: "", ticket_tier_id: "tier-1" }, event), "Floor");
  const fallback = resolveTierName({ tier_name: "", ticket_tier_id: "missing" }, null);
  assert.equal(fallback, "Tier details unavailable");
  assert.doesNotMatch(fallback, /general/i);
});

test("wallet order derives quantity, total and seat IDs from the list payload", () => {
  const { buildWalletOrder } = wallet();
  const order = buildWalletOrder(
    booking({
      items: [
        { id: "a", ticket_tier_id: "tier-1", tier_name: "", quantity: 2, unit_price_cents: 2500, seat_ids: ["s1", "s2"] },
        { id: "b", ticket_tier_id: "tier-1", tier_name: "Floor", quantity: 1, unit_price_cents: 2500, seat_ids: ["s2", "s3"] },
      ],
    }),
    event,
  );
  assert.equal(order.status, "CONFIRMED");
  assert.equal(order.isConfirmedBooking, true);
  assert.equal(order.quantity, 3);
  assert.equal(order.totalCents, 5000);
  assert.deepEqual(order.seatIds.sort(), ["s1", "s2", "s3"]);
  assert.equal(order.items[0].tierName, "Floor");
});

test("pagination merge dedupes bookings and reports remaining pages", () => {
  const { mergeBookings, hasMoreBookings } = wallet();
  const first = booking({ id: "one" });
  const second = booking({ id: "two" });
  const merged = mergeBookings([first], [first, second]);
  assert.deepEqual(merged.map((item) => item.id), ["one", "two"]);
  assert.equal(hasMoreBookings(2, 3), true);
  assert.equal(hasMoreBookings(3, 3), false);
});

test("All/Upcoming/Past filtering is based on the event instant and keeps undated orders in All", () => {
  const { buildWalletOrder, filterWalletOrders } = wallet();
  const fixedNow = Date.parse("2026-06-01T00:00:00Z");
  const future = buildWalletOrder(booking({ id: "future" }), {
    ...event,
    date: "2099-01-01T00:00:00Z",
  });
  const past = buildWalletOrder(booking({ id: "past" }), {
    ...event,
    date: "2000-01-01T00:00:00Z",
  });
  const undated = buildWalletOrder(booking({ id: "undated" }), null);

  assert.deepEqual(
    filterWalletOrders([future, past, undated], "all", fixedNow).map((o) => o.id),
    ["future", "past", "undated"],
  );
  assert.deepEqual(
    filterWalletOrders([future, past, undated], "upcoming", fixedNow).map((o) => o.id),
    ["future"],
  );
  assert.deepEqual(
    filterWalletOrders([future, past, undated], "past", fixedNow).map((o) => o.id),
    ["past"],
  );
});

test("wallet filter links only carry an allowlisted tab and round-trip the detail return path", () => {
  const { readWalletTab, walletHref, walletOrderHref } = wallet();
  assert.equal(readWalletTab(null), "all");
  assert.equal(readWalletTab(" UPCOMING "), "upcoming");
  assert.equal(readWalletTab("past"), "past");
  assert.equal(readWalletTab("all"), "all");
  assert.equal(readWalletTab("javascript:alert(1)"), "all");
  assert.equal(readWalletTab("https://example.test"), "all");
  assert.equal(walletHref("all"), "/my-tickets");
  assert.equal(walletHref("upcoming"), "/my-tickets?tab=upcoming");
  assert.equal(walletHref("past"), "/my-tickets?tab=past");
  assert.equal(walletOrderHref("booking-1", "all"), "/my-tickets/orders/booking-1");
  assert.equal(walletOrderHref("booking-1", "past"), "/my-tickets/orders/booking-1?tab=past");
});

test("wallet order card carries the selected safe filter into the detail route", () => {
  const { WalletOrderCard } = loadTypeScript("components/wallet-order-card.tsx");
  const order = wallet().buildWalletOrder(booking(), event);
  const html = renderToStaticMarkup(
    React.createElement(WalletOrderCard, {
      order,
      expanded: false,
      onToggle() {},
      tab: "upcoming",
    }),
  );
  assert.match(html, /href="\/my-tickets\/orders\/booking-1\?tab=upcoming"/);
});

test("calendar export uses the exact UTC instant without inventing a timezone or duration", () => {
  const { buildCalendarInvite, calendarFileName } = wallet();
  const order = wallet().buildWalletOrder(booking(), event);
  const invite = buildCalendarInvite(order, new Date("2026-01-01T00:00:00Z"));

  assert.ok(invite);
  assert.match(invite, /DTSTART:20261215T200000Z/);
  assert.match(invite, /DTSTAMP:20260101T000000Z/);
  assert.match(invite, /SUMMARY:Reliability Night/);
  assert.match(invite, /UID:booking-1@rushgo/);
  assert.doesNotMatch(invite, /TZID/);
  assert.doesNotMatch(invite, /DTEND/);
  assert.equal(calendarFileName(order), "rushgo-booking-1.ics");
});

test("calendar export escapes text and refuses non-tickets or missing dates", () => {
  const { buildCalendarInvite } = wallet();
  const order = wallet().buildWalletOrder(booking(), {
    ...event,
    title: "Show, Live; One\nTwo",
  });
  const invite = buildCalendarInvite(order);
  assert.match(invite, /SUMMARY:Show\\, Live\\; One\\nTwo/);

  const pending = wallet().buildWalletOrder(booking({ status: "PENDING" }), event);
  assert.equal(buildCalendarInvite(pending), null);
  const missing = wallet().buildWalletOrder(booking(), null);
  assert.equal(buildCalendarInvite(missing), null);
});

test("calendar folding keeps every physical line within 75 UTF-8 octets", () => {
  const { buildCalendarInvite } = wallet();
  const title = `${"🎟️".repeat(30)} Vé máy bay; đêm nhạc, Sài Gòn`;
  const order = wallet().buildWalletOrder(booking(), { ...event, title });
  const invite = buildCalendarInvite(order, new Date("2026-01-01T00:00:00Z"));
  const encoder = new TextEncoder();

  assert.ok(invite);
  for (const line of invite.split("\r\n")) {
    assert.ok(
      encoder.encode(line).length <= 75,
      `line exceeds 75 octets: ${line}`,
    );
    assert.doesNotMatch(line, /[\uD800-\uDBFF]$/);
    assert.doesNotMatch(line.replace(/^ /, ""), /^[\uDC00-\uDFFF]/);
  }
  const unfolded = invite.replace(/\r\n /g, "");
  assert.match(unfolded, /SUMMARY:/);
  assert.ok(unfolded.includes("🎟️".repeat(3)));
  assert.doesNotMatch(invite, /�/);
});

test("money and event labels do not fabricate values", () => {
  const { formatUsdCents, walletEventTitle, formatWalletDate } = wallet();
  assert.equal(formatUsdCents(5000), "$50.00");
  assert.equal(formatUsdCents(Number.NaN), "—");
  assert.equal(walletEventTitle(undefined), "Loading event details…");
  assert.equal(walletEventTitle(null), "Event details unavailable");
  assert.equal(formatWalletDate("not-a-date"), "Date unavailable");
});

test("wallet card renders confirmed booking details and no fake QR/View Ticket action", () => {
  const { WalletOrderCard } = loadTypeScript("components/wallet-order-card.tsx");
  const order = wallet().buildWalletOrder(booking(), event);
  const html = renderToStaticMarkup(
    React.createElement(WalletOrderCard, {
      order,
      expanded: true,
      onToggle() {},
    }),
  );

  assert.match(html, /Reliability Night/);
  assert.match(html, /Floor/);
  assert.match(html, /2 seats/);
  assert.match(html, /\$50\.00/);
  assert.match(html, /seat-1/);
  assert.match(html, /Add to calendar/);
  assert.match(html, /aria-expanded="true"/);
  assert.match(html, /admission pass appears above only after the server issues a separate seat ticket/);
  assert.doesNotMatch(html, /View Ticket/);
  assert.doesNotMatch(html, />General</);
});

test("wallet card distinguishes pending issuance from issued and unknown ticket states", () => {
  const { WalletOrderCard } = loadTypeScript("components/wallet-order-card.tsx");
  const order = wallet().buildWalletOrder(booking(), event);
  const render = (extra) => renderToStaticMarkup(React.createElement(WalletOrderCard, {
    order, expanded: true, onToggle() {}, ...extra,
  }));

  const pending = render({ ticketsKnown: true, hasAnyTicket: false });
  assert.match(pending, /Ticket issuance pending/);
  assert.match(pending, /has not issued a seat ticket or admission pass yet/);
  assert.match(pending, /no QR code is shown until a ticket exists/);

  const issued = render({ ticketsKnown: true, hasAnyTicket: true, hasIssuedTicket: true });
  assert.doesNotMatch(issued, /Ticket issuance pending/);
  assert.match(issued, /Your ticket is shown above/);

  const checkedIn = render({ ticketsKnown: true, hasAnyTicket: true, hasCheckedInTicket: true });
  assert.doesNotMatch(checkedIn, /Ticket issuance pending/);

  const unknown = render({ ticketsKnown: false, hasAnyTicket: false });
  assert.doesNotMatch(unknown, /Ticket issuance pending/);
  assert.match(unknown, /confirmed booking record/);
});

test("wallet card shows true non-confirmed status and never calls it a ticket", () => {
  const { WalletOrderCard } = loadTypeScript("components/wallet-order-card.tsx");
  const order = wallet().buildWalletOrder(booking({ status: "PENDING" }), event);
  const html = renderToStaticMarkup(
    React.createElement(WalletOrderCard, {
      order,
      expanded: true,
      onToggle() {},
    }),
  );

  assert.match(html, />Pending</);
  assert.match(html, /2 items/);
  assert.doesNotMatch(html, /2 tickets/);
  assert.match(html, /This order is not confirmed and does not have an admission pass/);
  assert.doesNotMatch(html, /Add to calendar/);
});

test("wallet card keeps the booking when event metadata is unavailable", () => {
  const { WalletOrderCard } = loadTypeScript("components/wallet-order-card.tsx");
  const order = wallet().buildWalletOrder(booking(), null);
  const html = renderToStaticMarkup(
    React.createElement(WalletOrderCard, {
      order,
      expanded: true,
      onToggle() {},
    }),
  );

  assert.match(html, /Event details unavailable/);
  assert.match(html, /\$50\.00/);
  assert.match(html, /Tier details unavailable/);
});

test("wallet card localizes missing tier metadata without translating a colliding real tier name", () => {
  const { buildWalletOrder } = wallet();
  const i18n = loadTypeScript("lib/i18n/index.ts", { react: React });
  const { WalletOrderCard } = loadTypeScript("components/wallet-order-card.tsx", {
    "@/lib/i18n/index": { ...i18n, useI18n: () => i18n.getI18n("zh-CN") },
  });
  const missingTier = buildWalletOrder(booking({
    items: [{ id: "missing", ticket_tier_id: "missing-tier", tier_name: "", quantity: 1, unit_price_cents: 2500, seat_ids: [] }],
  }), null);
  const realTierWithFallbackText = buildWalletOrder(booking({
    items: [{ id: "named", ticket_tier_id: "tier-1", tier_name: "Tier details unavailable", quantity: 1, unit_price_cents: 2500, seat_ids: [] }],
  }), null);

  assert.equal(missingTier.items[0].tierNameIsFallback, true);
  assert.equal(realTierWithFallbackText.items[0].tierNameIsFallback, false);
  const render = (order) => renderToStaticMarkup(React.createElement(WalletOrderCard, {
    order,
    expanded: true,
    onToggle() {},
  }));
  const missingMarkup = render(missingTier);
  const namedMarkup = render(realTierWithFallbackText);
  assert.match(missingMarkup, /票档信息不可用/);
  assert.doesNotMatch(missingMarkup, /Tier details unavailable/);
  assert.match(namedMarkup, /Tier details unavailable/);
  assert.doesNotMatch(namedMarkup, /票档信息不可用/);
});

test("wallet calendar action is localized while retaining the same calendar invite", () => {
  const { buildWalletOrder, buildCalendarInvite } = wallet();
  const i18n = loadTypeScript("lib/i18n/index.ts", { react: React });
  const { WalletOrderCard } = loadTypeScript("components/wallet-order-card.tsx", {
    "@/lib/i18n/index": { ...i18n, useI18n: () => i18n.getI18n("zh-CN") },
  });
  const order = buildWalletOrder(booking(), event);
  const markup = renderToStaticMarkup(React.createElement(WalletOrderCard, {
    order,
    expanded: true,
    onToggle() {},
  }));

  assert.match(markup, /添加到日历/);
  assert.doesNotMatch(markup, />Add to calendar</);
  assert.match(buildCalendarInvite(order) ?? "", /DTSTART:20261215T200000Z/);
});
