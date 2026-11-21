import type {
  ApiAdminBookingDetail,
  ApiAdminBookingDetailItem,
  ApiAdminBookingDetailReceipt,
  ApiAdminBookingDetailRefund,
  ApiAdminBookingDetailTicket,
} from "./api/types";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const TIMESTAMP = /^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,9})?(?:Z|[+-](?:0\d|1\d|2[0-3]):[0-5]\d)$/;

export class AdminBookingDetailResponseError extends Error {
  readonly code = "invalid_response";
}

function record(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function uuid(value: unknown): value is string {
  return typeof value === "string" && UUID.test(value);
}

function text(value: unknown, limit: number, required = false): value is string {
  return typeof value === "string" && value.length <= limit * 2 && Array.from(value).length <= limit &&
    !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(value) && (!required || value.trim().length > 0);
}

function timestamp(value: unknown, nullable = false): value is string | null {
  if (value === null && nullable) return true;
  return typeof value === "string" && value.length <= 40 && TIMESTAMP.test(value) && Number.isFinite(Date.parse(value));
}

function nonNegative(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function invalid(): never {
  throw new AdminBookingDetailResponseError("Booking detail response was invalid.");
}

function parseItem(value: unknown): ApiAdminBookingDetailItem {
  if (!record(value) || !uuid(value.id) || !uuid(value.ticket_tier_id) || !text(value.tier_name, 240) ||
      !Number.isSafeInteger(value.quantity) || (value.quantity as number) < 1 || !nonNegative(value.unit_price_cents) ||
      !Array.isArray(value.seat_ids) || value.seat_ids.length > 100 || !value.seat_ids.every(uuid)) invalid();
  return {
    id: value.id as string, ticket_tier_id: value.ticket_tier_id as string, tier_name: value.tier_name as string,
    quantity: value.quantity as number, unit_price_cents: value.unit_price_cents as number,
    seat_ids: value.seat_ids as string[],
  };
}

function parseTicket(value: unknown): ApiAdminBookingDetailTicket {
  if (!record(value) || !uuid(value.ticket_id) || !text(value.status, 32, true) || !uuid(value.seat_id) ||
      !uuid(value.ticket_tier_id) || !timestamp(value.issued_at) || !timestamp(value.checked_in_at, true) ||
      !timestamp(value.revoked_at, true)) invalid();
  return {
    ticket_id: value.ticket_id as string, status: value.status as string, seat_id: value.seat_id as string,
    ticket_tier_id: value.ticket_tier_id as string, issued_at: value.issued_at as string,
    checked_in_at: value.checked_in_at as string | null, revoked_at: value.revoked_at as string | null,
  };
}

function parseReceipt(value: unknown): ApiAdminBookingDetailReceipt | null {
  if (value === null) return null;
  if (!record(value) || (value.outcome !== "issued" && value.outcome !== "denied") || !text(value.reason_code, 80) ||
      !nonNegative(value.ticket_count) || !Number.isSafeInteger(value.receipt_count) || (value.receipt_count as number) < 1 ||
      !timestamp(value.observed_at, true) || !timestamp(value.created_at, true)) invalid();
  return {
    outcome: value.outcome as string, reason_code: value.reason_code as string,
    ticket_count: value.ticket_count as number, receipt_count: value.receipt_count as number,
    observed_at: value.observed_at as string | null, created_at: value.created_at as string | null,
  };
}

function parseRefund(value: unknown): ApiAdminBookingDetailRefund | null {
  if (value === null) return null;
  if (!record(value) || !uuid(value.request_id) || !text(value.status, 48, true) || !text(value.reason, 500) ||
      !text(value.decision_reason, 500) || !(value.refund_decision_id === "" || uuid(value.refund_decision_id)) ||
      !nonNegative(value.amount_cents) || value.currency !== "USD" || !timestamp(value.requested_at, true) ||
      !timestamp(value.decided_at, true) || !text(value.payment_refund_state, 48) ||
      !text(value.ticket_revocation_state, 48) || typeof value.can_withdraw !== "boolean" ||
      typeof value.approval_enabled !== "boolean") invalid();
  return {
    request_id: value.request_id, status: value.status, reason: value.reason,
    decision_reason: value.decision_reason, refund_decision_id: value.refund_decision_id,
    amount_cents: value.amount_cents, currency: "USD", requested_at: value.requested_at as string | null,
    decided_at: value.decided_at as string | null, payment_refund_state: value.payment_refund_state,
    ticket_revocation_state: value.ticket_revocation_state, can_withdraw: value.can_withdraw,
    approval_enabled: value.approval_enabled,
  };
}

export function parseAdminBookingDetail(value: unknown, bookingId: string): ApiAdminBookingDetail {
  if (!record(value) || !uuid(bookingId) || value.booking_id !== bookingId || !uuid(value.user_id) || !uuid(value.event_id) ||
      !text(value.booking_status, 50, true) || !nonNegative(value.total_amount_cents) || value.currency !== "USD" ||
      !text(value.booking_mode, 32) || !Number.isSafeInteger(value.lifecycle_generation) ||
      (value.lifecycle_generation as number) < 0 || !text(value.lifecycle_phase, 32) ||
      !timestamp(value.created_at) || !timestamp(value.observed_at) || !Array.isArray(value.items) ||
      value.items.length > 1000 || !Array.isArray(value.tickets) || value.tickets.length > 5000 ||
      value.source_service !== "booking" || value.coverage !== "booking_owned_exact") invalid();
  return {
    booking_id: value.booking_id, user_id: value.user_id, event_id: value.event_id,
    booking_status: value.booking_status, total_amount_cents: value.total_amount_cents,
    currency: "USD", booking_mode: value.booking_mode, lifecycle_generation: value.lifecycle_generation as number,
    lifecycle_phase: value.lifecycle_phase, created_at: value.created_at as string, observed_at: value.observed_at as string,
    items: value.items.map(parseItem), tickets: value.tickets.map(parseTicket),
    receipt: parseReceipt(value.receipt), refund: parseRefund(value.refund),
    source_service: "booking", coverage: "booking_owned_exact",
  };
}

export type AdminBookingSignal =
  | "multiple_receipts"
  | "receipt_without_ticket"
  | "issuance_denied"
  | "tickets_not_recorded"
  | "refund_payment_pending"
  | "refund_result_unknown"
  | "revocation_pending"
  | "revocation_not_synced";

// These are explanation labels for Booking-owned facts. They never assert
// provider collection and never infer a Kafka/consumer cause.
export function adminBookingSignals(detail: ApiAdminBookingDetail): AdminBookingSignal[] {
  const signals: AdminBookingSignal[] = [];
  if ((detail.receipt?.receipt_count ?? 0) > 1) signals.push("multiple_receipts");
  const confirmed = detail.booking_status.toUpperCase() === "CONFIRMED";
  if (confirmed && detail.tickets.length === 0) {
    if (detail.receipt?.outcome === "issued") signals.push("receipt_without_ticket");
    else if (detail.receipt?.outcome === "denied") signals.push("issuance_denied");
    else signals.push("tickets_not_recorded");
  }
  const refund = detail.refund;
  if (refund) {
    const payment = refund.payment_refund_state.toLowerCase();
    const ticket = refund.ticket_revocation_state.toUpperCase();
    const downstream = refund.status === "APPROVED" || refund.status === "REFUND_PROCESSING" ||
      refund.status === "REFUNDED" || refund.status === "NEEDS_ATTENTION";
    if (refund.status === "NEEDS_ATTENTION" || (downstream && payment !== "pending" && payment !== "succeeded")) signals.push("refund_result_unknown");
    else if (downstream && payment === "pending") signals.push("refund_payment_pending");
    if (downstream && ticket === "PENDING") signals.push("revocation_pending");
    else if (downstream && payment === "succeeded" && ticket !== "REVOKED") signals.push("revocation_not_synced");
  }
  return signals;
}
