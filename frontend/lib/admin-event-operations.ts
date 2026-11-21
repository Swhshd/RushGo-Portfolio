export const ADMIN_EVENT_OPERATIONS_RECORD_LIMIT = 5000;

export type AdminEventOperationsErrorCode =
  | "invalid_request"
  | "unauthorized"
  | "forbidden"
  | "not_found"
  | "capacity_exceeded"
  | "unavailable"
  | "invalid_response";

const SAFE_ERROR_COPY: Record<AdminEventOperationsErrorCode, string> = {
  invalid_request: "This event request is invalid.",
  unauthorized: "Your session has expired. Please sign in again.",
  forbidden: "Your account cannot view event operations.",
  not_found: "Event operations were not found for this event.",
  capacity_exceeded: "Query limit reached. Event operations exceed the safe read capacity.",
  unavailable: "Event operations are temporarily unavailable. Try again.",
  invalid_response: "Event operations could not be verified. Try again.",
};

const SAFE_ERROR_COPY_ZH: Record<AdminEventOperationsErrorCode, string> = {
  invalid_request: "活动请求无效。",
  unauthorized: "登录会话已过期，请重新登录。",
  forbidden: "当前账号无权查看活动运营数据。",
  not_found: "未找到此活动的运营数据。",
  capacity_exceeded: "已达到查询上限。活动运营数据超出安全读取容量。",
  unavailable: "活动运营数据暂时不可用，请重试。",
  invalid_response: "无法验证活动运营数据，请重试。",
};

export class AdminEventOperationsError extends Error {
  readonly code: AdminEventOperationsErrorCode;

  constructor(code: AdminEventOperationsErrorCode) {
    super(SAFE_ERROR_COPY[code]);
    this.name = "AdminEventOperationsError";
    this.code = code;
  }
}

export interface AdminEventTicketOperations {
  event_id: string;
  event_title: string;
  event_venue: string;
  event_date: string;
  observed_at: string;
  source_service: "booking";
  source_status: "available";
  coverage: "recorded_ticket_snapshot";
  total_recorded_count: number;
  issued_count: number;
  checked_in_count: number;
  revoked_count: number;
  successful_checkin_count: number;
  latest_checkin_at: string | null;
  recent_checkins: Array<{ checked_in_at: string }>;
  /** Present on newer booking-service snapshots; absence means unavailable, not zero. */
  delivery_exceptions?: AdminEventDeliveryException[];
  source_record_limit: typeof ADMIN_EVENT_OPERATIONS_RECORD_LIMIT;
  live_admission_eligibility: "not_evaluated";
}

export interface AdminEventDeliveryException {
  booking_id: string;
  state: "CONFIRMED_TICKET_DELIVERY_PENDING";
  booking_status: "CONFIRMED";
  expected_ticket_count: number;
  recorded_ticket_count: number;
  issuance_receipt_outcome: "NOT_RECORDED" | "issued" | "denied" | "CONFLICT";
  issuance_reason_code: string;
  booking_created_at: string;
}

const EVENT_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const NIL_EVENT_ID = "00000000-0000-0000-0000-000000000000";
const RFC3339_PATTERN = /^(\d{4})-(\d{2})-(\d{2})T([01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(?:Z|[+-](?:0\d|1\d|2[0-3]):[0-5]\d)$/;

export function isAdminEventOperationsId(value: unknown): value is string {
  return typeof value === "string" && value !== NIL_EVENT_ID && EVENT_ID_PATTERN.test(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function safeText(value: unknown, limit: number, required: boolean): value is string {
  if (typeof value !== "string" || value.length > limit * 2 || Array.from(value).length > limit) return false;
  if (/[\u0000-\u001f\u007f]/.test(value)) return false;
  return !required || value.trim().length > 0;
}

function safeTimestamp(value: unknown): value is string {
  if (typeof value !== "string") return false;
  const match = RFC3339_PATTERN.exec(value);
  if (!match || !Number.isFinite(Date.parse(value))) return false;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const calendarDate = new Date(0);
  calendarDate.setUTCFullYear(year, month - 1, day);
  calendarDate.setUTCHours(0, 0, 0, 0);
  return calendarDate.getUTCFullYear() === year &&
    calendarDate.getUTCMonth() === month - 1 &&
    calendarDate.getUTCDate() === day;
}

function safeCount(value: unknown): value is number {
  return Number.isSafeInteger(value) &&
    (value as number) >= 0 &&
    (value as number) <= ADMIN_EVENT_OPERATIONS_RECORD_LIMIT;
}

function parseFailure(): never {
  throw new AdminEventOperationsError("invalid_response");
}

/** Selects the frozen safe DTO fields and rejects malformed or mismatched data. */
export function parseAdminEventTicketOperations(
  value: unknown,
  expectedEventId: string,
): AdminEventTicketOperations {
  if (!isAdminEventOperationsId(expectedEventId) || !isRecord(value)) return parseFailure();

  const eventId = value.event_id;
  const title = value.event_title;
  const venue = value.event_venue;
  const eventDate = value.event_date;
  const observedAt = value.observed_at;
  const total = value.total_recorded_count;
  const issued = value.issued_count;
  const checkedIn = value.checked_in_count;
  const revoked = value.revoked_count;
  const successfulCheckins = value.successful_checkin_count;
  const latestCheckinAt = value.latest_checkin_at;
  const recent = value.recent_checkins;
  const hasDeliveryExceptions = Object.prototype.hasOwnProperty.call(value, "delivery_exceptions");
  const rawDeliveryExceptions = value.delivery_exceptions;

  if (
    typeof eventId !== "string" || eventId.toLowerCase() !== expectedEventId.toLowerCase() ||
    !safeText(title, 240, true) || !safeText(venue, 300, false) ||
    !safeTimestamp(eventDate) || !safeTimestamp(observedAt) ||
    value.source_service !== "booking" || value.source_status !== "available" ||
    value.coverage !== "recorded_ticket_snapshot" ||
    value.source_record_limit !== ADMIN_EVENT_OPERATIONS_RECORD_LIMIT ||
    value.live_admission_eligibility !== "not_evaluated" ||
    !safeCount(total) || !safeCount(issued) || !safeCount(checkedIn) ||
    !safeCount(revoked) || !safeCount(successfulCheckins) ||
    total !== issued + checkedIn + revoked ||
    successfulCheckins > total || successfulCheckins < checkedIn ||
    successfulCheckins > checkedIn + revoked || !Array.isArray(recent) ||
    recent.length !== Math.min(successfulCheckins, 5)
  ) {
    return parseFailure();
  }

  if (successfulCheckins === 0) {
    if (latestCheckinAt !== null || recent.length !== 0) return parseFailure();
  } else if (!safeTimestamp(latestCheckinAt) || recent.length === 0) {
    return parseFailure();
  }

  const recentCheckins: Array<{ checked_in_at: string }> = [];
  let previousTime = Number.POSITIVE_INFINITY;
  for (const item of recent) {
    if (!isRecord(item) || !safeTimestamp(item.checked_in_at)) return parseFailure();
    const checkedInAt = item.checked_in_at;
    const timestamp = Date.parse(checkedInAt);
    if (timestamp > Date.parse(observedAt) || timestamp > previousTime) return parseFailure();
    previousTime = timestamp;
    recentCheckins.push({ checked_in_at: checkedInAt });
  }
  if (successfulCheckins > 0 && Date.parse(recentCheckins[0].checked_in_at) !== Date.parse(latestCheckinAt as string)) {
    return parseFailure();
  }

  let deliveryExceptions: AdminEventDeliveryException[] | undefined;
  if (hasDeliveryExceptions) {
    if (!Array.isArray(rawDeliveryExceptions) || rawDeliveryExceptions.length > ADMIN_EVENT_OPERATIONS_RECORD_LIMIT) return parseFailure();
    const seenBookings = new Set<string>();
    deliveryExceptions = [];
    for (const item of rawDeliveryExceptions) {
      if (!isRecord(item) || !isAdminEventOperationsId(item.booking_id) ||
          item.state !== "CONFIRMED_TICKET_DELIVERY_PENDING" || item.booking_status !== "CONFIRMED" ||
          !Number.isSafeInteger(item.expected_ticket_count) || (item.expected_ticket_count as number) < 0 ||
          !Number.isSafeInteger(item.recorded_ticket_count) || (item.recorded_ticket_count as number) < 0 ||
          (item.issuance_receipt_outcome !== "NOT_RECORDED" && item.issuance_receipt_outcome !== "issued" && item.issuance_receipt_outcome !== "denied" && item.issuance_receipt_outcome !== "CONFLICT") ||
          typeof item.issuance_reason_code !== "string" || !/^[a-z0-9_]{0,80}$/.test(item.issuance_reason_code) ||
          !safeTimestamp(item.booking_created_at) || Date.parse(item.booking_created_at) > Date.parse(observedAt) ||
          seenBookings.has(item.booking_id.toLowerCase())) return parseFailure();
      seenBookings.add(item.booking_id.toLowerCase());
      deliveryExceptions.push({
        booking_id: item.booking_id,
        state: item.state,
        booking_status: item.booking_status,
        expected_ticket_count: item.expected_ticket_count as number,
        recorded_ticket_count: item.recorded_ticket_count as number,
        issuance_receipt_outcome: item.issuance_receipt_outcome,
        issuance_reason_code: item.issuance_reason_code,
        booking_created_at: item.booking_created_at,
      });
    }
  }

  return {
    event_id: eventId,
    event_title: title,
    event_venue: venue,
    event_date: eventDate,
    observed_at: observedAt,
    source_service: "booking",
    source_status: "available",
    coverage: "recorded_ticket_snapshot",
    total_recorded_count: total,
    issued_count: issued,
    checked_in_count: checkedIn,
    revoked_count: revoked,
    successful_checkin_count: successfulCheckins,
    latest_checkin_at: latestCheckinAt as string | null,
    recent_checkins: recentCheckins,
    source_record_limit: ADMIN_EVENT_OPERATIONS_RECORD_LIMIT,
    live_admission_eligibility: "not_evaluated",
    ...(deliveryExceptions !== undefined ? { delivery_exceptions: deliveryExceptions } : {}),
  };
}

export function adminEventOperationsErrorCode(error: unknown): AdminEventOperationsErrorCode {
  if (!isRecord(error)) return "unavailable";
  if (error.code === "ADMIN_EVENT_OPERATIONS_CAPACITY_EXCEEDED") return "capacity_exceeded";
  if (error.status === 401) return "unauthorized";
  if (error.status === 403) return "forbidden";
  if (error.status === 400) return "invalid_request";
  if (error.status === 404) return "not_found";
  return "unavailable";
}

export function adminEventOperationsErrorMessage(
  code: AdminEventOperationsErrorCode,
  locale: "en-US" | "zh-CN" = "en-US",
): string {
  return locale === "zh-CN" ? SAFE_ERROR_COPY_ZH[code] : SAFE_ERROR_COPY[code];
}
