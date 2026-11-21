export type ApiErrorCode = "AUTH_REQUIRED" | "SESSION_EXPIRED" | "BOOKING_CONFLICT" | "ACCOUNTING_NOT_READY" | "BOOKING_ERROR" | "PAYMENT_INIT_ERROR" | "TICKET_IDEMPOTENCY_CONFLICT" | "TICKET_IDEMPOTENCY_IN_PROGRESS" | "TICKET_AUTHORITY_UNAVAILABLE" | "TICKET_NOT_ELIGIBLE" | "OUTBOX_CAPACITY_EXCEEDED" | "ADMIN_EVENT_OPERATIONS_CAPACITY_EXCEEDED" | "EVENT_DATE_CONFLICT" | "EVENT_REFUND_TIME_PENDING" | "REQUEST_ERROR";

export class ApiError extends Error {
  constructor(public readonly code: ApiErrorCode, message: string, public readonly status?: number) {
    super(message);
    this.name = "ApiError";
  }
}

export function responseError(path: string, status: number, message: string, backendCode?: string): ApiError {
  let code: ApiErrorCode = "REQUEST_ERROR";
  const endpoint = path.split("?", 1)[0];
  if (
    endpoint === "/admin/outbox-events" &&
    (backendCode === "capacity_exceeded" || backendCode === "outbox_capacity_exceeded")
  ) {
    return new ApiError(
      "OUTBOX_CAPACITY_EXCEEDED",
      "Query limit reached. Booking Outbox exceeded the safe read capacity.",
      status,
    );
  }
  if (
    /^\/admin\/events\/[0-9a-f-]+\/(?:ticket-operations|tickets)$/i.test(endpoint) &&
    backendCode === "capacity_exceeded"
  ) {
    return new ApiError(
      "ADMIN_EVENT_OPERATIONS_CAPACITY_EXCEEDED",
      "Query limit reached. Event operations exceed the safe read capacity.",
      status,
    );
  }
  if (backendCode === "idempotency_conflict") code = "TICKET_IDEMPOTENCY_CONFLICT";
  else if (backendCode === "idempotency_in_progress") code = "TICKET_IDEMPOTENCY_IN_PROGRESS";
  else if (backendCode === "ticket_authority_unavailable") code = "TICKET_AUTHORITY_UNAVAILABLE";
  else if (backendCode === "ticket_not_eligible") code = "TICKET_NOT_ELIGIBLE";
  else if (backendCode === "EVENT_DATE_CONFLICT") code = "EVENT_DATE_CONFLICT";
  else if (backendCode === "EVENT_REFUND_TIME_PENDING") code = "EVENT_REFUND_TIME_PENDING";
  if (path === "/bookings") {
    // The server distinguishes an unsellable tier from a taken seat; keep that
    // distinction instead of reporting every 409 as a seat conflict.
    if (backendCode === "accounting_not_ready") return new ApiError("ACCOUNTING_NOT_READY", message, status);
    code = "BOOKING_ERROR";
    if (status === 409 || /seat.*(?:conflict|no longer available|not available|unavailable|already reserved|already booked)/i.test(message)) code = "BOOKING_CONFLICT";
    else if (/(?:create|creating|initiat\w*).*(?:payment intent|payment)|payment.*(?:intent|initiat\w*).*fail/i.test(message)) code = "PAYMENT_INIT_ERROR";
  }
  return new ApiError(code, message, status);
}

export function bookingFailure(error: unknown): ApiError {
  // Preserve typed errors, including errors crossing a provider boundary.
  if (error instanceof Error && "code" in error) return error as ApiError;
  return new ApiError("BOOKING_ERROR", "Your booking could not be started. Please try again.");
}

export function bookingFailureMessage(error: ApiError): string {
  if (error.code === "AUTH_REQUIRED") return "Sign in to continue with your booking.";
  if (error.code === "SESSION_EXPIRED") return "Your session has expired. Please sign in again.";
  if (error.code === "ACCOUNTING_NOT_READY") return "This ticket tier is not available for sale right now. Please choose another tier.";
  if (error.code === "BOOKING_CONFLICT") return "Some selected seats are no longer available. Please choose available seats and try again.";
  if (error.code === "PAYMENT_INIT_ERROR") return "Failed to initiate payment. Please try again.";
  return "Your booking could not be started. Please try again.";
}
