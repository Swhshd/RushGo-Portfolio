import { isAdminEventOperationsId } from "./admin-event-operations";
import type { ApiBookingRefund, ApiEventBookingRefundsResponse, ApiEventRefundApprover } from "./api/types";

export class BookingRefundResponseError extends Error {
  readonly code = "invalid_response";
}

export type BuyerRefundEntryAccess = "ALLOWED" | "BLOCKED" | "UNKNOWN";

export type BuyerRefundAccessEntry = {
  access: BuyerRefundEntryAccess;
  // Fallback results belong to the exact fallback query cycle that produced
  // them. The page recreates this array whenever the fallback set changes, so
  // a new query for the same order cannot reuse an older ALLOWED result.
  fallbackCycle: readonly string[] | null;
};

export function currentBuyerRefundEntryAccess(
  entry: BuyerRefundAccessEntry | undefined,
  isFallback: boolean,
  fallbackCycle: readonly string[],
): BuyerRefundEntryAccess {
  if (!entry) return "UNKNOWN";
  if (isFallback && entry.fallbackCycle !== fallbackCycle) return "UNKNOWN";
  return entry.access;
}

function record(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function timestamp(value: unknown): value is string {
  if (typeof value !== "string" || value.length > 40) return false;
  const match = /^(\d{4})-(\d{2})-(\d{2})T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,9})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.exec(value);
  if (!match || !Number.isFinite(Date.parse(value))) return false;
  const date = new Date(0);
  date.setUTCFullYear(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
  return date.getUTCFullYear() === Number(match[1]) && date.getUTCMonth() === Number(match[2]) - 1 && date.getUTCDate() === Number(match[3]);
}

function invalid(): never { throw new BookingRefundResponseError("Booking refund response was invalid."); }
function safeText(value: unknown, max = 500): value is string { return typeof value === "string" && value.length <= max; }

export function canSubmitBuyerRefundRequest(statusKnown: boolean, currentRefund: ApiBookingRefund | null, bookingStatus: string, hasIssuedTicket: boolean, hasCheckedInTicket: boolean): boolean {
  return statusKnown && currentRefund === null && bookingStatus === "CONFIRMED" && hasIssuedTicket && !hasCheckedInTicket;
}

export function buyerRefundEntryAccess(statusKnown: boolean, currentRefund: ApiBookingRefund | null): BuyerRefundEntryAccess {
  if (!statusKnown) return "UNKNOWN";
  if (!currentRefund) return "ALLOWED";
  // A lagging top-level status must not reopen entry after downstream work began.
  const payment = currentRefund.payment_refund_state.toLowerCase();
  const ticket = currentRefund.ticket_revocation_state.toUpperCase();
  if (["APPROVED", "REFUND_PROCESSING", "REFUNDED", "NEEDS_ATTENTION"].includes(currentRefund.status) ||
      ["pending", "succeeded", "unknown"].includes(payment) ||
      ["PENDING", "REVOKED", "NEEDS_ATTENTION"].includes(ticket)) return "BLOCKED";
  if ((payment && payment !== "not_required") || (ticket && ticket !== "NOT_STARTED")) return "UNKNOWN";
  switch (currentRefund.status) {
    case "APPROVED":
    case "REFUND_PROCESSING":
    case "REFUNDED":
    case "NEEDS_ATTENTION":
      return "BLOCKED";
    case "REQUESTED":
    case "WAITING_CHECKIN_RESOLUTION":
    case "REJECTED":
    case "WITHDRAWN":
      return "ALLOWED";
    default:
      return "UNKNOWN";
  }
}

export function canApproveBookingRefund(status: string, approvalEnabled: boolean, currency = "USD"): boolean {
  return (status === "REQUESTED" || status === "WAITING_CHECKIN_RESOLUTION") && approvalEnabled && currency === "USD";
}

export function canRejectBookingRefund(status: string): boolean {
  return status === "REQUESTED" || status === "WAITING_CHECKIN_RESOLUTION";
}

/** Return only the allowlisted buyer-safe fields from a refund response. */
export function parseBookingRefund(value: unknown, bookingId: string): ApiBookingRefund {
  if (!record(value) || !isAdminEventOperationsId(bookingId) || value.booking_id !== bookingId ||
      !isAdminEventOperationsId(value.request_id) || !isAdminEventOperationsId(value.event_id) ||
      !safeText(value.status, 48) || !safeText(value.reason) || !safeText(value.decision_reason) ||
      !(value.refund_decision_id === "" || isAdminEventOperationsId(value.refund_decision_id)) ||
      typeof value.amount_cents !== "number" || !Number.isSafeInteger(value.amount_cents) || value.amount_cents < 0 ||
      typeof value.currency !== "string" || value.currency.toUpperCase() !== "USD" ||
      !timestamp(value.requested_at) || !(value.decided_at === "" || value.decided_at === null || timestamp(value.decided_at)) ||
      !safeText(value.payment_refund_state, 48) || !safeText(value.ticket_revocation_state, 48) ||
      typeof value.can_withdraw !== "boolean" || typeof value.approval_enabled !== "boolean" ||
      !safeText(value.eligibility_code, 80)) return invalid();
  return {
    request_id: value.request_id,
    booking_id: value.booking_id,
    event_id: value.event_id,
    status: value.status,
    reason: value.reason,
    decision_reason: value.decision_reason,
    refund_decision_id: value.refund_decision_id,
    amount_cents: Number(value.amount_cents),
    currency: "USD",
    requested_at: value.requested_at,
    decided_at: value.decided_at ?? "",
    payment_refund_state: value.payment_refund_state,
    ticket_revocation_state: value.ticket_revocation_state,
    can_withdraw: value.can_withdraw,
    approval_enabled: value.approval_enabled,
    eligibility_code: value.eligibility_code,
  };
}

export function parseEventBookingRefunds(value: unknown, eventId: string): ApiEventBookingRefundsResponse {
  if (!record(value) || !isAdminEventOperationsId(eventId) || !Array.isArray(value.refunds) || value.refunds.length > 100 ||
      !safeText(value.next_cursor, 2048) || (value.next_cursor !== "" && !/^[A-Za-z0-9_-]+$/.test(value.next_cursor)) ||
      typeof value.approval_enabled !== "boolean" || !safeText(value.approval_block_reason, 500)) return invalid();
  const seen = new Set<string>();
  const refunds = value.refunds.map((entry) => {
    const bookingId = record(entry) ? entry.booking_id : null;
    if (!isAdminEventOperationsId(bookingId) || !record(entry) || entry.event_id !== eventId || seen.has(String(entry.request_id))) invalid();
    seen.add(String((entry as Record<string, unknown>).request_id));
    return parseBookingRefund(entry, bookingId);
  });
  return { refunds, next_cursor: value.next_cursor, approval_enabled: value.approval_enabled, approval_block_reason: value.approval_block_reason };
}

export function parseEventRefundApprovers(value: unknown, eventId: string): { assignments: ApiEventRefundApprover[] } {
  if (!record(value) || !Array.isArray(value.assignments) || value.assignments.length > 500) return invalid();
  const seen = new Set<string>();
  const assignments = value.assignments.map((entry) => {
    if (!record(entry) || entry.event_id !== eventId || !isAdminEventOperationsId(entry.staff_user_id) ||
        typeof entry.active !== "boolean" || !timestamp(entry.changed_at) || seen.has(entry.staff_user_id.toLowerCase())) invalid();
    seen.add(String(entry.staff_user_id).toLowerCase());
    return { event_id: String(entry.event_id), staff_user_id: String(entry.staff_user_id), active: Boolean(entry.active), changed_at: String(entry.changed_at) };
  });
  return { assignments };
}
