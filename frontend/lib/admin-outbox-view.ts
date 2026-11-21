import type {
  ApiAdminOutboxEventType,
  ApiAdminOutboxEventsResponse,
  ApiAdminOutboxRecord,
  ApiAdminOutboxRecordedStatus,
  ApiAdminOutboxSource,
} from "./api/types";

export type AdminOutboxReadErrorKind = "capacity" | "session" | "forbidden" | "filter" | "unavailable";
export type AdminOutboxLocale = "en-US" | "zh-CN";

function errorCode(error: unknown): string | null {
  if (typeof error !== "object" || error === null || !("code" in error)) return null;
  const code = (error as { code?: unknown }).code;
  return typeof code === "string" ? code : null;
}

function errorStatus(error: unknown): number | undefined {
  return typeof error === "object" && error !== null && "status" in error
    ? (error as { status?: number }).status
    : undefined;
}

export function classifyAdminOutboxReadError(error: unknown): AdminOutboxReadErrorKind {
  const code = errorCode(error);
  if (code === "OUTBOX_CAPACITY_EXCEEDED" || code === "capacity_exceeded" || code === "outbox_capacity_exceeded") {
    return "capacity";
  }

  const status = errorStatus(error);
  if (status === 401) return "session";
  if (status === 403) return "forbidden";
  if (status === 400) return "filter";
  return "unavailable";
}

export function safeAdminOutboxReadErrorMessage(
  error: unknown,
  context: "first-page" | "load-more" = "first-page",
  source: ApiAdminOutboxSource = "booking",
  locale: AdminOutboxLocale = "en-US",
): string {
  const kind = classifyAdminOutboxReadError(error);
  const sourceName = outboxSourceName(source, locale);
  if (locale === "zh-CN") {
    if (kind === "capacity") {
      return context === "load-more"
        ? `已达到查询上限。已显示的${sourceName}记录保持不变，不会显示更多记录。`
        : `已达到查询上限。${sourceName}超出安全读取容量，不会假定结果为空。`;
    }
    if (kind === "session") return "管理员会话已过期。请重新登录以读取这些记录。";
    if (kind === "forbidden") return `当前账号无权读取${sourceName}记录。`;
    if (kind === "filter") return "所选筛选条件被拒绝。请检查时间范围后重新应用。";
    return context === "load-more"
      ? `无法加载更多${sourceName}记录。已显示的记录保持不变。`
      : `${sourceName}暂时不可用。请重试读取。`;
  }
  if (kind === "capacity") {
    return context === "load-more"
      ? `Query limit reached. The ${sourceName} records already shown are unchanged; no additional records are shown.`
      : `Query limit reached. ${sourceName} exceeded the safe read capacity; no empty result is assumed.`;
  }
  if (kind === "session") return "Your admin session has expired. Sign in again to read these records.";
  if (kind === "forbidden") return `Your account cannot read ${sourceName} records.`;
  if (kind === "filter") return "The selected filters were rejected. Check the time range and apply them again.";
  return context === "load-more"
    ? `Could not load more ${sourceName} records. The records already shown are unchanged.`
    : `${sourceName} is temporarily unavailable. Retry the read.`;
}

export function outboxSourceName(source: ApiAdminOutboxSource, locale: AdminOutboxLocale = "en-US"): string {
  if (locale === "zh-CN") return source === "notification" ? "通知意图" : "Booking 发件箱";
  return source === "notification" ? "Notification Intents" : "Booking Outbox";
}

export function outboxRecordedStatusLabel(
  source: ApiAdminOutboxSource,
  status: ApiAdminOutboxRecordedStatus,
  locale: AdminOutboxLocale = "en-US",
): string {
  if (locale === "zh-CN") {
    if (source === "notification" && status === "sent") return "适配器已接受";
    switch (status) {
      case "pending": return "待处理";
      case "published": return "已发布";
      case "delivering": return "投递中";
      case "sent": return "已发送";
      case "retryable": return "可重试";
      case "unknown": return "未知";
      case "attention": return "需关注";
    }
  }
  if (source === "notification" && status === "sent") return "Adapter accepted";
  return status.charAt(0).toUpperCase() + status.slice(1);
}

export function outboxStatusScopeLabel(source: ApiAdminOutboxSource, locale: AdminOutboxLocale = "en-US"): string {
  if (locale === "zh-CN") return source === "notification"
    ? "支持的记录状态：待处理、投递中、适配器已接受、可重试、未知、需关注"
    : "支持的记录状态：待处理和已发布";
  return source === "notification"
    ? "Supported recorded states: Pending, Delivering, Adapter accepted, Retryable, Unknown, Attention"
    : "Supported recorded states: Pending and Published";
}

const BOOKING_EVENT_TYPES = new Set<ApiAdminOutboxEventType>([
  "BookingConfirmed",
  "BookingCancelled",
  "BookingFailed",
  "Other",
]);
const NOTIFICATION_EVENT_TYPES = new Set<ApiAdminOutboxEventType>([
  "booking_confirmation",
  "booking_failure",
  "booking_cancellation",
  "welcome_email",
  "Other",
]);
const BOOKING_STATUSES = new Set<ApiAdminOutboxRecordedStatus>(["pending", "published"]);
const NOTIFICATION_STATUSES = new Set<ApiAdminOutboxRecordedStatus>([
  "pending",
  "delivering",
  "sent",
  "retryable",
  "unknown",
  "attention",
]);

export function isOutboxRecordForSource(
  value: unknown,
  source: ApiAdminOutboxSource,
): value is ApiAdminOutboxRecord {
  if (!value || typeof value !== "object") return false;
  const record = value as Partial<ApiAdminOutboxRecord>;
  const allowedTypes = source === "notification" ? NOTIFICATION_EVENT_TYPES : BOOKING_EVENT_TYPES;
  const allowedStatuses = source === "notification" ? NOTIFICATION_STATUSES : BOOKING_STATUSES;
  return record.source_service === source &&
    typeof record.event_id === "string" &&
    allowedTypes.has(record.event_type as ApiAdminOutboxEventType) &&
    typeof record.created_at === "string" &&
    Number.isFinite(Date.parse(record.created_at)) &&
    allowedStatuses.has(record.recorded_status as ApiAdminOutboxRecordedStatus) &&
    record.consumer_outcome === "not_verified";
}

export function isOutboxResponseForSource(
  value: ApiAdminOutboxEventsResponse,
  source: ApiAdminOutboxSource,
  maximumRecords: number,
): boolean {
  const allowedTypes = source === "notification" ? NOTIFICATION_EVENT_TYPES : BOOKING_EVENT_TYPES;
  return value.source_service === source &&
    value.consumer_outcome === "not_verified" &&
    Array.isArray(value.records) &&
    value.records.length <= maximumRecords &&
    value.records.every((record) => isOutboxRecordForSource(record, source)) &&
    typeof value.has_more === "boolean" &&
    typeof value.next_cursor === "string" &&
    (!value.has_more || value.next_cursor.length > 0) &&
    typeof value.observed_at === "string" &&
    Number.isFinite(Date.parse(value.observed_at)) &&
    Array.isArray(value.supported_event_types) &&
    value.supported_event_types.every((eventType) => allowedTypes.has(eventType)) &&
    value.source_record_limit === 5000;
}

export function formatPendingAge(
  createdAt: string,
  observedAt: string,
  locale: AdminOutboxLocale = "en-US",
): string | null {
  const createdMs = Date.parse(createdAt);
  const observedMs = Date.parse(observedAt);
  if (!Number.isFinite(createdMs) || !Number.isFinite(observedMs) || createdMs > observedMs) return null;

  const totalMinutes = Math.floor((observedMs - createdMs) / 60_000);
  if (locale === "zh-CN") {
    if (totalMinutes < 1) return "不足 1 分钟";

    const formatNumber = (value: number) => new Intl.NumberFormat(locale).format(value);
    const days = Math.floor(totalMinutes / (24 * 60));
    const hours = Math.floor((totalMinutes % (24 * 60)) / 60);
    const minutes = totalMinutes % 60;
    const parts: string[] = [];
    if (days > 0) parts.push(`${formatNumber(days)}天`);
    if (hours > 0) parts.push(`${formatNumber(hours)}小时`);
    if (minutes > 0 || parts.length === 0) parts.push(`${formatNumber(minutes)}分钟`);
    return parts.join(" ");
  }
  if (totalMinutes < 1) return "less than 1m";

  const days = Math.floor(totalMinutes / (24 * 60));
  const hours = Math.floor((totalMinutes % (24 * 60)) / 60);
  const minutes = totalMinutes % 60;
  const parts: string[] = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0 || parts.length === 0) parts.push(`${minutes}m`);
  return parts.join(" ");
}

export type OutboxOverviewFacts = {
  recordCount: number;
  newestRecordAt: string | null;
  observedAt: string;
  sampleLimit: number;
};

export function getOutboxOverviewFacts(
  records: ApiAdminOutboxRecord[],
  observedAt: string,
  sampleLimit = 5,
): OutboxOverviewFacts {
  let newestRecordAt: string | null = null;
  let newestRecordTime = Number.NEGATIVE_INFINITY;
  for (const record of records) {
    const recordTime = Date.parse(record.created_at);
    if (Number.isFinite(recordTime) && recordTime > newestRecordTime) {
      newestRecordTime = recordTime;
      newestRecordAt = record.created_at;
    }
  }

  return {
    recordCount: records.length,
    newestRecordAt,
    observedAt,
    sampleLimit,
  };
}
