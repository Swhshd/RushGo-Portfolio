import { apiClient } from "./client";

export type AlertKind = "onsale" | "restock";
export interface EventSubscription {
  eventId: string;
  kind: AlertKind;
  emailOptIn: boolean;
  consentVersion: string;
  status: string;
  createdAt: string;
  updatedAt: string;
}
export interface FavoriteEvent { eventId: string; createdAt: string; }
export interface InterestPage<T> { items: T[]; limit: number; offset: number; hasMore: boolean; }
export interface EventInterests {
  eventId: string;
  favorite: boolean;
  subscriptions: EventSubscription[];
  saleStatus: string;
  onsaleAvailable: boolean;
  restockStatus: string;
}
export interface EngagementCounts { favorites: number; onsale: number; restock: number; }
const eventPath = (id: string) => `/users/me/events/${encodeURIComponent(id)}`;
export const eventInterestsApi = {
  state: (id: string, signal?: AbortSignal) => apiClient.eventInterestsRequest<EventInterests>(`${eventPath(id)}/interests`, { signal }),
  favorites: (offset = 0, signal?: AbortSignal) => apiClient.eventInterestsRequest<InterestPage<FavoriteEvent>>(`/users/me/favorites?limit=20&offset=${offset}`, { signal }),
  favorite: (id: string, enabled: boolean) => apiClient.eventInterestsRequest(`/users/me/favorites/${encodeURIComponent(id)}`, { method: enabled ? "PUT" : "DELETE" }),
  subscriptions: (offset = 0, signal?: AbortSignal) => apiClient.eventInterestsRequest<InterestPage<EventSubscription>>(`/users/me/event-alerts?limit=20&offset=${offset}`, { signal }),
  subscribe: (id: string, kind: AlertKind, emailOptIn: boolean) => apiClient.eventInterestsRequest(`${eventPath(id)}/alerts/${kind}`, { method: "PUT", body: JSON.stringify({ emailOptIn, consentVersion: emailOptIn ? "event-alerts-v1" : "" }) }),
  unsubscribe: (id: string, kind: AlertKind) => apiClient.eventInterestsRequest(`${eventPath(id)}/alerts/${kind}`, { method: "DELETE" }),
  disableCategoryEmails: (kind: AlertKind) => apiClient.eventInterestsRequest<{ updated: number }>(`/users/me/event-alerts/${kind}/email`, { method: "DELETE" }),
  engagement: (id: string, signal?: AbortSignal) => apiClient.eventInterestsRequest<EngagementCounts>(`/admin/events/${encodeURIComponent(id)}/engagement`, { signal }),
};
