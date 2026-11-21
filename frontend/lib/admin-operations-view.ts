import type { ApiAdminOperationsResponse, ApiOperationsItem } from "./api/types";

export type NotificationSourceSnapshot =
  | { state: "available"; items: ApiOperationsItem[]; hasMore: boolean; observedAt: string | null }
  | { state: "unavailable"; items: null; hasMore: false; observedAt: null };

export function notificationSourceSnapshot(response: ApiAdminOperationsResponse): NotificationSourceSnapshot {
  if (response.notification_source_state !== "available") {
    return { state: "unavailable", items: null, hasMore: false, observedAt: null };
  }
  return {
    state: "available",
    items: Array.isArray(response.notification_items) ? response.notification_items : [],
    hasMore: Boolean(response.notification_has_more),
    observedAt: response.notification_observed_at || null,
  };
}
