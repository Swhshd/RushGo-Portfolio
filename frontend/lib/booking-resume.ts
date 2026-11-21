import type { ApiBooking, ApiBookingResumeResponse } from "./api/types";

export type BookingCheckoutState = "ready" | "processing" | "expired" | "completed" | "unavailable";

export interface ParsedBookingResume {
  booking: ApiBooking | null;
  checkout_state: BookingCheckoutState;
  reservation_deadline: string | null;
  can_cancel: boolean;
}

const CHECKOUT_STATES = new Set<BookingCheckoutState>([
  "ready",
  "processing",
  "expired",
  "completed",
  "unavailable",
]);
const PAYMENT_INTENT_SECRET = /^pi_[A-Za-z0-9]+_secret_[A-Za-z0-9]+$/;
const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;

function validDeadline(value: unknown): value is string | null {
  return value === null || (
    typeof value === "string" &&
    RFC3339.test(value) &&
    Number.isFinite(Date.parse(value))
  );
}

export function parseBookingResume(value: unknown, expectedBookingId: string): ParsedBookingResume {
  const unavailable: ParsedBookingResume = {
    booking: null,
    checkout_state: "unavailable",
    reservation_deadline: null,
    can_cancel: false,
  };
  if (!value || typeof value !== "object") return unavailable;
  const response = value as Partial<ApiBookingResumeResponse>;
  const state = response.checkout_state;
  const booking = response as Partial<ApiBooking>;
  if (
    typeof state !== "string" ||
    !CHECKOUT_STATES.has(state as BookingCheckoutState) ||
    booking.id !== expectedBookingId ||
    typeof booking.event_id !== "string" ||
    typeof booking.user_id !== "string" ||
    typeof booking.status !== "string" ||
    !Number.isSafeInteger(booking.total_amount_cents) ||
    !Array.isArray(booking.items) ||
    typeof response.can_cancel !== "boolean" ||
    !validDeadline(response.reservation_deadline)
  ) {
    return unavailable;
  }

  const cleanBooking: ApiBooking = {
    ...(booking as ApiBooking),
    paymentIntentClientSecret: undefined,
  };
  const normalizedState = state as BookingCheckoutState;
  if (normalizedState !== "ready") {
    return {
      booking: cleanBooking,
      checkout_state: normalizedState,
      reservation_deadline: response.reservation_deadline ?? null,
      can_cancel: response.can_cancel,
    };
  }

  const secret = booking.paymentIntentClientSecret;
  if (
    booking.status.toUpperCase() !== "PENDING" ||
    !Number.isSafeInteger(booking.total_amount_cents) ||
    booking.total_amount_cents! <= 0 ||
    typeof secret !== "string" ||
    !PAYMENT_INTENT_SECRET.test(secret)
  ) {
    return {
      booking: cleanBooking,
      checkout_state: "unavailable",
      reservation_deadline: response.reservation_deadline ?? null,
      can_cancel: false,
    };
  }

  return {
    booking: { ...cleanBooking, paymentIntentClientSecret: secret },
    checkout_state: "ready",
    reservation_deadline: response.reservation_deadline ?? null,
    can_cancel: response.can_cancel,
  };
}