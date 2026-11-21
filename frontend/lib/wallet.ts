import type { ApiBooking, ApiBookingItem, ApiEvent } from "./api/types";
import { getI18n, type Locale } from "./i18n/core";

/**
 * Wallet projections and calendar helpers.
 *
 * Booking history remains a separate projection from the Ticket entitlements
 * returned by the authoritative ticket wallet API.
 */

export const WALLET_STATUSES = [
  "PENDING",
  "CONFIRMED",
  "FAILED",
  "CANCELLED",
  "EXPIRED",
] as const;

export type WalletStatus = (typeof WALLET_STATUSES)[number] | "UNKNOWN";

export type WalletStatusTone = "success" | "pending" | "danger" | "muted";

export interface WalletOrderItem {
  id: string;
  tierId: string;
  tierName: string;
  tierNameIsFallback: boolean;
  quantity: number;
  unitPriceCents: number;
  seatIds: string[];
}

export interface WalletOrder {
  id: string;
  eventId: string;
  status: WalletStatus;
  rawStatus: string;
  statusLabel: string;
  statusTone: WalletStatusTone;
  isConfirmedBooking: boolean;
  totalCents: number;
  createdAt: string;
  quantity: number;
  seatIds: string[];
  items: WalletOrderItem[];
  /** undefined while event metadata is loading, null when unavailable. */
  event: ApiEvent | null | undefined;
}

export function normalizeWalletStatus(raw: string | null | undefined): WalletStatus {
  const status = (raw ?? "").trim().toUpperCase();
  if ((WALLET_STATUSES as readonly string[]).includes(status)) {
    return status as WalletStatus;
  }
  // Do not reinterpret an unknown backend status as pending/confirmed.
  return "UNKNOWN";
}

export function walletStatusLabel(status: WalletStatus): string {
  switch (status) {
    case "PENDING":
      return "Pending";
    case "CONFIRMED":
      return "Confirmed";
    case "FAILED":
      return "Failed";
    case "CANCELLED":
      return "Cancelled";
    case "EXPIRED":
      return "Expired";
    default:
      return "Unknown";
  }
}

export function walletStatusTone(status: WalletStatus): WalletStatusTone {
  switch (status) {
    case "CONFIRMED":
      return "success";
    case "PENDING":
      return "pending";
    case "FAILED":
    case "CANCELLED":
    case "EXPIRED":
      return "danger";
    default:
      return "muted";
  }
}

/** Resolve a real tier name from the booking or the joined event tiers. */
export function resolveTierNameWithFallback(
  item: Pick<ApiBookingItem, "tier_name" | "ticket_tier_id">,
  event: ApiEvent | null | undefined,
): { name: string; isFallback: boolean } {
  const fromBooking = item.tier_name?.trim();
  if (fromBooking) return { name: fromBooking, isFallback: false };
  const fromEvent = event?.tiers?.find(
    (tier) => tier.id === item.ticket_tier_id,
  )?.name;
  const name = fromEvent?.trim();
  // Never fabricate "General" as if it were the purchased tier.
  return name
    ? { name, isFallback: false }
    : { name: "Tier details unavailable", isFallback: true };
}

export function resolveTierName(
  item: Pick<ApiBookingItem, "tier_name" | "ticket_tier_id">,
  event: ApiEvent | null | undefined,
): string {
  return resolveTierNameWithFallback(item, event).name;
}

export function buildWalletOrder(
  booking: ApiBooking,
  event: ApiEvent | null | undefined,
): WalletOrder {
  const status = normalizeWalletStatus(booking.status);
  const items = (booking.items ?? []).map<WalletOrderItem>((item) => {
    const resolvedTier = resolveTierNameWithFallback(item, event);
    return {
      id: item.id,
      tierId: item.ticket_tier_id,
      tierName: resolvedTier.name,
      tierNameIsFallback: resolvedTier.isFallback,
      quantity: Number.isFinite(item.quantity) ? Math.max(0, item.quantity) : 0,
      unitPriceCents: Number.isFinite(item.unit_price_cents)
        ? item.unit_price_cents
        : 0,
      seatIds: (item.seat_ids ?? []).filter(Boolean),
    };
  });
  return {
    id: booking.id,
    eventId: booking.event_id,
    status,
    rawStatus: booking.status ?? "",
    statusLabel: walletStatusLabel(status),
    statusTone: walletStatusTone(status),
    isConfirmedBooking: status === "CONFIRMED",
    totalCents: Number.isFinite(booking.total_amount_cents)
      ? booking.total_amount_cents
      : 0,
    createdAt: booking.created_at,
    quantity: items.reduce((sum, item) => sum + item.quantity, 0),
    seatIds: Array.from(new Set(items.flatMap((item) => item.seatIds))),
    items,
    event,
  };
}

/** Append one server page without duplicating a booking. */
export function mergeBookings(
  existing: ApiBooking[],
  incoming: ApiBooking[],
): ApiBooking[] {
  const seen = new Set(existing.map((booking) => booking.id));
  return [
    ...existing,
    ...incoming.filter((booking) => {
      if (seen.has(booking.id)) return false;
      seen.add(booking.id);
      return true;
    }),
  ];
}

export function hasMoreBookings(loaded: number, total: number): boolean {
  return Number.isFinite(total) && total > loaded;
}

export function formatUsdCents(cents: number, locale: Locale = "en-US"): string {
  return getI18n(locale).usdCents(cents);
}

export function formatWalletDate(iso: string | null | undefined, locale: Locale = "en-US"): string {
  const display = getI18n(locale);
  if (!iso) return display.t("customer.wallet.dateUnavailable");
  return display.date(iso, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }) || display.t("customer.wallet.dateUnavailable");
}

export function eventStartMs(event: ApiEvent | null | undefined): number {
  if (!event?.date) return Number.NaN;
  return Date.parse(event.date);
}

export type WalletTab = "all" | "upcoming" | "past";

export function readWalletTab(value: string | null | undefined): WalletTab {
  const normalized = typeof value === "string" ? value.trim().toLowerCase() : "";
  return normalized === "upcoming" || normalized === "past" || normalized === "all"
    ? normalized
    : "all";
}

export function walletHref(tab: WalletTab): string {
  const safe = readWalletTab(tab);
  return safe === "all" ? "/my-tickets" : `/my-tickets?tab=${safe}`;
}

export function walletOrderHref(
  bookingId: string,
  tab: WalletTab = "all",
): string {
  const base = `/my-tickets/orders/${encodeURIComponent(bookingId)}`;
  const safe = readWalletTab(tab);
  return safe === "all" ? base : `${base}?tab=${safe}`;
}

export function filterWalletOrders(
  orders: WalletOrder[],
  tab: WalletTab,
  now: number = Date.now(),
): WalletOrder[] {
  if (tab === "all") return orders;
  return orders.filter((order) => {
    const start = eventStartMs(order.event);
    if (!Number.isFinite(start)) return false;
    return tab === "upcoming" ? start > now : start <= now;
  });
}

export function walletEventTitle(event: ApiEvent | null | undefined): string {
  if (event === undefined) return "Loading event details…";
  if (event === null) return "Event details unavailable";
  return event.title || "Untitled event";
}

export function walletEventLocation(event: ApiEvent | null | undefined): string {
  if (!event) return "";
  return [event.venue, event.location].filter(Boolean).join(", ");
}

function escapeIcsText(value: string): string {
  return value
    .replace(/\\/g, "\\\\")
    .replace(/\r\n|\r|\n/g, "\\n")
    .replace(/;/g, "\\;")
    .replace(/,/g, "\\,");
}

function icsUtc(date: Date): string {
  const pad = (part: number) => String(part).padStart(2, "0");
  return (
    `${date.getUTCFullYear()}${pad(date.getUTCMonth() + 1)}${pad(date.getUTCDate())}` +
    `T${pad(date.getUTCHours())}${pad(date.getUTCMinutes())}${pad(date.getUTCSeconds())}Z`
  );
}

function foldIcsLine(line: string): string {
  // RFC 5545 limits physical lines to 75 octets, not UTF-16 code units. Fold
  // on UTF-8 byte counts and iterate code points so surrogate pairs and
  // multi-byte characters are never split.
  const encoder = new TextEncoder();
  let result = "";
  let lineBytes = 0;
  // Array.from iterates code points even when the test harness down-levels
  // for...of, so the folding guarantee does not depend on the TS target.
  for (const character of Array.from(line)) {
    const bytes = encoder.encode(character).length;
    if (lineBytes + bytes > 75) {
      result += "\r\n ";
      lineBytes = 1 + bytes; // continuation lines start with one space
    } else {
      lineBytes += bytes;
    }
    result += character;
  }
  return result;
}

/**
 * Build an RFC 5545 calendar entry for the event's exact start instant.
 *
 * The backend stores an instant only, with no venue timezone and no end time,
 * so the file intentionally uses UTC DTSTART and omits TZID/DTEND. It is not a
 * ticket credential; the booking UUID is only used as the calendar UID.
 */
export function buildCalendarInvite(
  order: WalletOrder,
  now: Date = new Date(),
): string | null {
  const startsAt = eventStartMs(order.event);
  if (!order.isConfirmedBooking || !Number.isFinite(startsAt)) return null;
  const start = new Date(startsAt);
  const title = order.event?.title?.trim() || "RushGo event";
  const location = walletEventLocation(order.event);
  const lines = [
    "BEGIN:VCALENDAR",
    "VERSION:2.0",
    "PRODID:-//RushGo//Ticket Wallet//EN",
    "CALSCALE:GREGORIAN",
    "METHOD:PUBLISH",
    "BEGIN:VEVENT",
    `UID:${escapeIcsText(order.id)}@rushgo`,
    `DTSTAMP:${icsUtc(now)}`,
    `DTSTART:${icsUtc(start)}`,
    `SUMMARY:${escapeIcsText(title)}`,
  ];
  if (location) lines.push(`LOCATION:${escapeIcsText(location)}`);
  lines.push(
    `DESCRIPTION:${escapeIcsText(`Confirmed RushGo booking ${order.id}`)}`,
    "STATUS:CONFIRMED",
    "END:VEVENT",
    "END:VCALENDAR",
  );
  return lines.map(foldIcsLine).join("\r\n") + "\r\n";
}

export function calendarFileName(order: WalletOrder): string {
  return `rushgo-${order.id.replace(/[^A-Za-z0-9_-]/g, "").slice(0, 12) || "event"}.ics`;
}
