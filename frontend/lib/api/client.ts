import { ApiAuthResponse, ApiEvent, ApiListEventsResponse, ApiCreateEventInput, ApiUpdateEventInput, ApiUpdateEventSalesInput, ApiBooking, ApiListBookingsResponse, ApiTicket, ApiTicketCredential, ApiTicketCheckInResult, ApiScannerAssignment, ApiUser, ApiSeat, ApiGetSeatsParams, ApiUpdateSeatStatusParams, ApiListSchedulersResponse, ApiSchedulerJob, ApiUpdateSchedulerJobParams, ApiCreateSchedulerJobParams, ApiAdminOutboxEventsQuery, ApiAdminOutboxEventsResponse, ApiBookingRefund, ApiEventBookingRefundsResponse, ApiEventRefundApprover, ApiUpdateProfileInput, ApiEmailChangeCodeResponse, ApiSignInRequiredResponse, ApiNotificationPage, ApiNotification, ApiAdminOperationsResponse, ApiWorkResponse, ApiAdminFeatureFlagsResponse, ApiFeatureFlag, ApiAdminMetricsResponse, ApiPublicFeaturesResponse } from "./types";
import { ApiError, responseError } from "./errors";
import { normalizeAdminEventTicketsQuery, parseAdminEventTickets } from "../admin-event-tickets";
import { parseBookingRefund, parseEventBookingRefunds, parseEventRefundApprovers } from "../booking-refunds";
import { parseAdminBookingDetail } from "../admin-booking-detail";
import type { ApiAdminBookingDetail } from "./types";
import type { ApiAdminEventTicketsQuery, ApiAdminEventTicketsResponse } from "./types";
import { authResponseError } from "./auth-errors";
import type { ApiRegisterInput, ApiRegistrationResponse, ApiRegistrationCodeResponse, ApiMessageResponse, ApiBookingResumeResponse } from "./types";
import {
  AdminEventOperationsError,
  adminEventOperationsErrorCode,
  isAdminEventOperationsId,
  parseAdminEventTicketOperations,
  type AdminEventTicketOperations,
} from "../admin-event-operations";

class ApiClient {
  private baseUrl: string;
  private sessionExpiredListeners = new Set<() => void>();
  private sessionVersion = 0;
  private refreshInFlight: { version: number; promise: Promise<boolean> } | null = null;
  private terminatingVersion: number | null = null;
  getSessionVersion(): number { return this.sessionVersion; }
  notifyExternalSessionChange(): number {
    this.sessionVersion++;
    return this.sessionVersion;
  }
  clearLocalSession(): void { this.clearTokens(); }

  subscribeSessionExpired(listener: () => void): () => void {
    this.sessionExpiredListeners.add(listener);
    return () => { this.sessionExpiredListeners.delete(listener); };
  }

  private expireSession(): ApiError {
    this.clearTokens();
    this.sessionExpiredListeners.forEach((listener) => listener());
    return new ApiError("SESSION_EXPIRED", "Your session has expired. Please sign in again.", 401);
  }

  constructor() {
    // Use relative URL so Next.js rewrites proxy to backend
    this.baseUrl = "/api";
  }

  private getToken(): string | null {
    if (typeof window === "undefined") return null;
    return localStorage.getItem("access_token");
  }

  private getRefreshToken(): string | null {
    if (typeof window === "undefined") return null;
    return localStorage.getItem("refresh_token");
  }

  private setTokens(access: string, refresh: string, newSession = true) {
    if (newSession) this.sessionVersion++;
    localStorage.setItem("access_token", access);
    localStorage.setItem("refresh_token", refresh);
  }

  private clearTokens() {
    this.sessionVersion++;
    localStorage.removeItem("access_token");
    localStorage.removeItem("refresh_token");
  }

  private async request<T>(path: string, options: RequestInit = {}): Promise<T> {
    const version = this.sessionVersion;
    const authEndpoint = ["/auth/login", "/auth/register", "/auth/registration-code", "/auth/forgot-password", "/auth/reset-password"].includes(path);
    const token = this.getToken();
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      ...((options.headers as Record<string, string>) || {}),
    };
    if (token && !authEndpoint) {
      headers["Authorization"] = `Bearer ${token}`;
    }

    let res = await fetch(`${this.baseUrl}${path}`, {
      ...options,
      headers,
    });

    // An older request must not overwrite or invalidate a newer login/logout.
    const checkSession = () => {
      if (version !== this.sessionVersion) throw new ApiError("REQUEST_ERROR", "Session changed during request. Please try again.");
    };
    const publicRead = (!options.method || options.method === "GET") && (path === "/features" || path === "/events" || path.startsWith("/events?") || path.startsWith("/events/") || path.startsWith("/seats?"));
    if (!authEndpoint && (!publicRead || res.status === 401)) checkSession();
    if (res.status === 401 && !authEndpoint && token && this.terminatingVersion === version) {
      throw new ApiError("REQUEST_ERROR", "Logout is already ending this session.", 401);
    }
    if (res.status === 401 && !authEndpoint && token) {
      // Try refresh
      const refreshed = token !== this.getToken() || await this.tryRefresh(version);
      checkSession();
      if (this.terminatingVersion === version) {
        throw new ApiError("REQUEST_ERROR", "Logout is already ending this session.", 401);
      }
      if (refreshed) {
        headers["Authorization"] = `Bearer ${this.getToken()}`;
        res = await fetch(`${this.baseUrl}${path}`, { ...options, headers });
        checkSession();
        if (res.status === 401) throw this.expireSession();
      } else {
        throw this.expireSession();
      }
    } else if (res.status === 401 && !authEndpoint) {
      this.clearTokens();
      this.sessionExpiredListeners.forEach((listener) => listener());
      throw new ApiError("AUTH_REQUIRED", "Sign in to continue with your booking.", 401);
    }

    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: "Request failed" }));
      if (path.startsWith("/auth/")) throw authResponseError(res.status, err.code);
      throw responseError(path, res.status, err.error || `Request failed with status ${res.status}`, err.code);
    }

    return res.json();
  }

  private async requestBlob(path: string, options: RequestInit = {}): Promise<{ blob: Blob; filename: string }> {
    const version = this.sessionVersion;
    const token = this.getToken();
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      ...((options.headers as Record<string, string>) || {}),
    };
    if (token) headers.Authorization = `Bearer ${token}`;
    let res = await fetch(`${this.baseUrl}${path}`, { ...options, headers });
    const checkSession = () => {
      if (version !== this.sessionVersion) throw new ApiError("REQUEST_ERROR", "Session changed during request. Please try again.");
    };
    checkSession();
    if (res.status === 401 && token && this.terminatingVersion === version) {
      throw new ApiError("REQUEST_ERROR", "Logout is already ending this session.", 401);
    }
    if (res.status === 401 && token) {
      const refreshed = token !== this.getToken() || await this.tryRefresh(version);
      checkSession();
      if (this.terminatingVersion === version) throw new ApiError("REQUEST_ERROR", "Logout is already ending this session.", 401);
      if (!refreshed) throw this.expireSession();
      headers.Authorization = `Bearer ${this.getToken()}`;
      res = await fetch(`${this.baseUrl}${path}`, { ...options, headers });
      checkSession();
      if (res.status === 401) throw this.expireSession();
    } else if (res.status === 401) {
      this.clearTokens();
      this.sessionExpiredListeners.forEach((listener) => listener());
      throw new ApiError("AUTH_REQUIRED", "Sign in to continue.", 401);
    }
    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: "Request failed" }));
      throw responseError(path, res.status, err.error || `Request failed with status ${res.status}`, err.code);
    }
    const disposition = res.headers.get("content-disposition") || "";
    const match = /filename\s*=\s*"?([^";]+)"?/i.exec(disposition);
    const rawFilename = match?.[1]?.trim() || "rushgo-operations.csv";
    const filename = rawFilename.replace(/[\\/\r\n]/g, "_") || "rushgo-operations.csv";
    return { blob: await res.blob(), filename };
  }

  private tryRefresh(version: number): Promise<boolean> {
    if (this.terminatingVersion === version) return Promise.resolve(false);
    // Refresh tokens rotate: simultaneous protected reads must share one refresh.
    if (!this.refreshInFlight || this.refreshInFlight.version !== version) {
      const pending = { version, promise: this.refreshTokens(version) };
      this.refreshInFlight = pending;
      pending.promise = pending.promise.finally(() => { if (this.refreshInFlight === pending) this.refreshInFlight = null; });
    }
    return this.refreshInFlight.promise;
  }

  private async refreshTokens(version: number): Promise<boolean> {
    const refreshToken = this.getRefreshToken();
    if (!refreshToken) return false;
    try {
      const res = await fetch(`${this.baseUrl}/auth/refresh`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ refresh_token: refreshToken }),
      });
      if (!res.ok) return false;
      const data: ApiAuthResponse = await res.json();
      if (version !== this.sessionVersion) return false;
      this.setTokens(data.access_token, data.refresh_token, false);
      return true;
    } catch {
      return false;
    }
  }

  // ── Auth ──────────────────────────────────────────────
  async register(input: ApiRegisterInput): Promise<ApiRegistrationResponse> {
    return this.request<ApiRegistrationResponse>("/auth/register", {
      method: "POST",
      body: JSON.stringify(input),
    });
  }
  requestRegistrationCode(email: string): Promise<ApiRegistrationCodeResponse> { return this.request("/auth/registration-code", { method: "POST", body: JSON.stringify({ email }) }); }
  requestPasswordReset(email: string): Promise<ApiMessageResponse> { return this.request("/auth/forgot-password", { method: "POST", body: JSON.stringify({ email }) }); }
  resetPassword(token: string, newPassword: string): Promise<ApiMessageResponse> { return this.request("/auth/reset-password", { method: "POST", body: JSON.stringify({ token, new_password: newPassword }) }); }

  async login(email: string, password: string): Promise<ApiAuthResponse> {
    const data = await this.request<ApiAuthResponse>("/auth/login", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
    this.setTokens(data.access_token, data.refresh_token);
    return data;
  }

  async logout(): Promise<void> {
    const version = this.sessionVersion;
    this.terminatingVersion = version;
    try {
      if (this.refreshInFlight?.version === version) await this.refreshInFlight.promise;
      if (version !== this.sessionVersion) return;
      const res = await fetch(`${this.baseUrl}/auth/logout`, { method: "POST", headers: { "Content-Type": "application/json", Authorization: `Bearer ${this.getToken() || ""}` }, body: JSON.stringify({ refresh_token: this.getRefreshToken() }) });
      if (!res.ok) { const err = await res.json().catch(() => ({})); throw authResponseError(res.status, err.code); }
    } finally {
      if (version === this.sessionVersion) this.clearTokens();
      if (this.terminatingVersion === version) this.terminatingVersion = null;
    }
  }

  async getProfile(): Promise<ApiUser> {
    return this.request<ApiUser>("/users/me");
  }

  async updateProfile(input: ApiUpdateProfileInput): Promise<ApiUser> {
    return this.request<ApiUser>("/users/me", { method: "PUT", body: JSON.stringify(input) });
  }

  async changePassword(currentPassword: string, newPassword: string): Promise<ApiSignInRequiredResponse> {
    return this.request<ApiSignInRequiredResponse>("/users/me/password", { method: "POST", body: JSON.stringify({ current_password: currentPassword, new_password: newPassword }) });
  }

  async requestEmailChangeCode(currentPassword: string, email: string): Promise<ApiEmailChangeCodeResponse> {
    return this.request<ApiEmailChangeCodeResponse>("/users/me/email/code", { method: "POST", body: JSON.stringify({ current_password: currentPassword, email }) });
  }

  async completeEmailChange(currentPassword: string, verificationId: string, verificationCode: string): Promise<ApiSignInRequiredResponse> {
    return this.request<ApiSignInRequiredResponse>("/users/me/email/confirm", { method: "POST", body: JSON.stringify({ current_password: currentPassword, verification_id: verificationId, verification_code: verificationCode }) });
  }

  async listMyNotifications(page = 1, pageSize = 20, unreadOnly = false, signal?: AbortSignal): Promise<ApiNotificationPage> {
    const query = new URLSearchParams({ page: String(page), page_size: String(pageSize), unread_only: String(unreadOnly) });
    return this.request<ApiNotificationPage>(`/users/me/notifications?${query}`, { cache: "no-store", ...(signal ? { signal } : {}) });
  }

  async markNotificationRead(id: string): Promise<Pick<ApiNotification, "id" | "read_at">> {
    return this.request<Pick<ApiNotification, "id" | "read_at">>(`/users/me/notifications/${encodeURIComponent(id)}/read`, { method: "PATCH", cache: "no-store", body: JSON.stringify({}) });
  }

  async listMyWork(limit = 50, offset = 0, signal?: AbortSignal): Promise<ApiWorkResponse> {
    const query = new URLSearchParams({ limit: String(limit), offset: String(offset) });
    return this.request<ApiWorkResponse>(`/work?${query}`, { cache: "no-store", ...(signal ? { signal } : {}) });
  }

  async listAdminOperations(params: { from: string; to: string; limit?: number; offset?: number }, signal?: AbortSignal): Promise<ApiAdminOperationsResponse> {
    const query = new URLSearchParams({ from: params.from, to: params.to, limit: String(params.limit ?? 50), offset: String(params.offset ?? 0) });
    return this.request<ApiAdminOperationsResponse>(`/admin/operations?${query}`, { cache: "no-store", ...(signal ? { signal } : {}) });
  }

  async getAdminFeatureFlags(signal?: AbortSignal): Promise<ApiAdminFeatureFlagsResponse> {
    return this.request<ApiAdminFeatureFlagsResponse>("/admin/feature-flags", { cache: "no-store", ...(signal ? { signal } : {}) });
  }

  async updateAdminFeatureFlag(key: string, enabled: boolean, expectedVersion: number): Promise<ApiFeatureFlag> {
    return this.request<ApiFeatureFlag>(`/admin/feature-flags/${encodeURIComponent(key)}`, {
      method: "PUT",
      cache: "no-store",
      body: JSON.stringify({ enabled, expected_version: expectedVersion }),
    });
  }

  async getAdminMetrics(from: string, to: string, signal?: AbortSignal): Promise<ApiAdminMetricsResponse> {
    const query = new URLSearchParams({ from, to });
    return this.request<ApiAdminMetricsResponse>(`/admin/metrics?${query}`, { cache: "no-store", ...(signal ? { signal } : {}) });
  }

  async downloadAdminMetricsCsv(from: string, to: string): Promise<{ blob: Blob; filename: string }> {
    const query = new URLSearchParams({ from, to, format: "csv" });
    return this.requestBlob(`/admin/metrics?${query}`, { method: "GET", cache: "no-store" });
  }

  // ── Events ────────────────────────────────────────────
  /** One bounded server page; callers that need a detail must fetch it by ID. */
  async listEventsPage(params?: { category?: string; search?: string; date_from?: string; date_to?: string; location?: string; min_price_cents?: number; max_price_cents?: number; sort?: string; page?: number; page_size?: number }, signal?: AbortSignal): Promise<ApiListEventsResponse> {
    const query = new URLSearchParams();
    if (params?.category) query.set("category", params.category);
    if (params?.search) query.set("search", params.search);
    if (params?.date_from) query.set("date_from", params.date_from);
    if (params?.date_to) query.set("date_to", params.date_to);
    if (params?.location) query.set("location", params.location);
    if (params?.min_price_cents !== undefined) query.set("min_price_cents", String(params.min_price_cents));
    if (params?.max_price_cents !== undefined) query.set("max_price_cents", String(params.max_price_cents));
    if (params?.sort) query.set("sort", params.sort);
    if (params?.page) query.set("page", String(params.page));
    if (params?.page_size) query.set("page_size", String(params.page_size));
    const qs = query.toString();
    return this.request<ApiListEventsResponse>(`/events${qs ? `?${qs}` : ""}`, signal ? { signal } : {});
  }

  async getPublicFeatures(signal?: AbortSignal): Promise<ApiPublicFeaturesResponse> {
    return this.request<ApiPublicFeaturesResponse>("/features", { cache: "no-store", ...(signal ? { signal } : {}) });
  }

  async listEvents(params?: { category?: string; search?: string; date_from?: string; date_to?: string; location?: string; min_price_cents?: number; max_price_cents?: number; sort?: string; page?: number; page_size?: number }): Promise<ApiListEventsResponse> {
    return this.listEventsPage(params);
  }

  async listAdminEventsPage(params?: { category?: string; search?: string; page?: number; page_size?: number }, signal?: AbortSignal): Promise<ApiListEventsResponse> {
    const query = new URLSearchParams();
    if (params?.category) query.set("category", params.category);
    if (params?.search) query.set("search", params.search);
    if (params?.page) query.set("page", String(params.page));
    if (params?.page_size) query.set("page_size", String(params.page_size));
    const qs = query.toString();
    return this.request<ApiListEventsResponse>(`/admin/events${qs ? `?${qs}` : ""}`, { cache: "no-store", ...(signal ? { signal } : {}) });
  }

  async getEvent(id: string): Promise<ApiEvent> {
    return this.request<ApiEvent>(`/events/${encodeURIComponent(id)}`);
  }

  async getAdminEvent(id: string): Promise<ApiEvent> {
    return this.request<ApiEvent>(`/admin/events/${encodeURIComponent(id)}`, { cache: "no-store" });
  }

  async listAdminOutboxEvents(
    params: ApiAdminOutboxEventsQuery,
    signal?: AbortSignal,
  ): Promise<ApiAdminOutboxEventsResponse> {
    const query = new URLSearchParams();
    query.set("source", params.source ?? "booking");
    query.set("status", params.status);
    if (params.event_type) query.set("event_type", params.event_type);
    query.set("from", params.from);
    query.set("to", params.to);
    query.set("limit", String(params.limit));
    if (params.cursor) query.set("cursor", params.cursor);
    return this.request<ApiAdminOutboxEventsResponse>(
      `/admin/outbox-events?${query.toString()}`,
      { method: "GET", cache: "no-store", signal },
    );
  }

  async getAdminEventTicketOperations(
    eventId: string,
    signal?: AbortSignal,
  ): Promise<AdminEventTicketOperations> {
    if (!isAdminEventOperationsId(eventId)) {
      throw new AdminEventOperationsError("invalid_request");
    }
    try {
      const response = await this.request<unknown>(
        `/admin/events/${encodeURIComponent(eventId)}/ticket-operations`,
        { method: "GET", cache: "no-store", signal },
      );
      return parseAdminEventTicketOperations(response, eventId);
    } catch (error) {
      if (error instanceof AdminEventOperationsError) throw error;
      throw new AdminEventOperationsError(adminEventOperationsErrorCode(error));
    }
  }

  // Admin-only event management (gateway enforces AdminOnly server-side).
  async listAdminEventTickets(eventId: string, params?: ApiAdminEventTicketsQuery, signal?: AbortSignal): Promise<ApiAdminEventTicketsResponse> {
    if (!isAdminEventOperationsId(eventId)) throw new AdminEventOperationsError("invalid_request");
    const normalized = normalizeAdminEventTicketsQuery(params);
    const query = new URLSearchParams({ status: normalized.status, page_size: String(normalized.page_size) });
    if (normalized.cursor) query.set("cursor", normalized.cursor);
    try {
      const response = await this.request<unknown>(`/admin/events/${encodeURIComponent(eventId)}/tickets?${query}`, { method: "GET", cache: "no-store", signal });
      return parseAdminEventTickets(response, eventId, normalized);
    } catch (error) {
      if (error instanceof AdminEventOperationsError) throw error;
      throw new AdminEventOperationsError(adminEventOperationsErrorCode(error));
    }
  }

  async createEvent(input: ApiCreateEventInput, idempotencyKey?: string): Promise<ApiEvent> {
    return this.request<ApiEvent>("/events", {
      method: "POST",
      ...(idempotencyKey ? { headers: { "Idempotency-Key": idempotencyKey } } : {}),
      body: JSON.stringify(input),
    });
  }

  async updateEvent(id: string, input: ApiUpdateEventInput): Promise<ApiEvent> {
    return this.request<ApiEvent>(`/events/${id}`, {
      method: "PUT",
      body: JSON.stringify(input),
    });
  }

  async updateEventSales(id: string, input: ApiUpdateEventSalesInput): Promise<ApiEvent> {
    return this.request<ApiEvent>(`/admin/events/${encodeURIComponent(id)}/sales`, { method: "PATCH", cache: "no-store", body: JSON.stringify(input) });
  }

  // ── Bookings ──────────────────────────────────────────
  async createBooking(eventId: string, items: { ticket_tier_id: string; quantity: number; seat_ids: string[] }[], requestId: string): Promise<ApiBooking> {
    return this.request<ApiBooking>("/bookings", {
      method: "POST",
      headers: { "Idempotency-Key": requestId },
      body: JSON.stringify({ event_id: eventId, items }),
    });
  }

  async listBookings(page?: number, pageSize?: number): Promise<ApiListBookingsResponse> {
    const query = new URLSearchParams();
    if (page) query.set("page", String(page));
    if (pageSize) query.set("page_size", String(pageSize));
    const qs = query.toString();
    return this.request<ApiListBookingsResponse>(`/bookings${qs ? `?${qs}` : ""}`);
  }

  async getBooking(id: string): Promise<ApiBooking> {
    return this.request<ApiBooking>(`/bookings/${id}`);
  }

  async getAdminBookingDetail(id: string): Promise<ApiAdminBookingDetail> {
    if (!isAdminEventOperationsId(id)) throw new Error("invalid_request");
    const response = await this.request<unknown>(`/admin/bookings/${encodeURIComponent(id)}`, { cache: "no-store" });
    return parseAdminBookingDetail(response, id);
  }

  async resumeBooking(id: string): Promise<ApiBookingResumeResponse> {
    if (!isAdminEventOperationsId(id)) throw new Error("invalid_request");
    return this.request<ApiBookingResumeResponse>("/bookings/" + encodeURIComponent(id) + "/resume", {
      method: "POST",
      cache: "no-store",
    });
  }
  async cancelBooking(id: string): Promise<ApiBooking> {
    return this.request<ApiBooking>(`/bookings/${id}/cancel`, { method: "POST" });
  }

  async getBookingRefund(bookingId: string): Promise<ApiBookingRefund> {
    const response = await this.request<unknown>(`/bookings/${encodeURIComponent(bookingId)}/refund`, { method: "GET", cache: "no-store" });
    return parseBookingRefund(response, bookingId);
  }

  async requestBookingRefund(bookingId: string, reason: string, idempotencyKey: string): Promise<ApiBookingRefund> {
    if (!isAdminEventOperationsId(bookingId) || !reason.trim() || reason.length > 500 || !isAdminEventOperationsId(idempotencyKey)) throw new Error("invalid_request");
    const response = await this.request<unknown>(`/bookings/${encodeURIComponent(bookingId)}/refund`, {
      method: "POST", cache: "no-store", headers: { "Idempotency-Key": idempotencyKey }, body: JSON.stringify({ reason }),
    });
    return parseBookingRefund(response, bookingId);
  }

  async withdrawBookingRefund(requestId: string, idempotencyKey: string): Promise<ApiBookingRefund> {
    if (!isAdminEventOperationsId(requestId) || !isAdminEventOperationsId(idempotencyKey)) throw new Error("invalid_request");
    const response = await this.request<unknown>(`/refund-requests/${encodeURIComponent(requestId)}/withdraw`, {
      method: "POST", cache: "no-store", headers: { "Idempotency-Key": idempotencyKey }, body: JSON.stringify({}),
    });
    const bookingId = response && typeof response === "object" && "booking_id" in response ? String(response.booking_id) : "";
    return parseBookingRefund(response, bookingId);
  }

  async listEventBookingRefunds(eventId: string, cursor = "", pageSize = 30): Promise<ApiEventBookingRefundsResponse> {
    if (!isAdminEventOperationsId(eventId) || !Number.isInteger(pageSize) || pageSize < 1 || pageSize > 100 || (cursor && !/^[A-Za-z0-9_-]{1,2048}$/.test(cursor))) throw new Error("invalid_request");
    const query = new URLSearchParams({ page_size: String(pageSize) });
    if (cursor) query.set("cursor", cursor);
    const response = await this.request<unknown>(`/events/${encodeURIComponent(eventId)}/refund-requests?${query}`, { method: "GET", cache: "no-store" });
    return parseEventBookingRefunds(response, eventId);
  }

  async decideBookingRefund(eventId: string, requestId: string, decision: "APPROVE" | "REJECT", reason: string, idempotencyKey: string): Promise<ApiBookingRefund> {
    if (!isAdminEventOperationsId(eventId) || !isAdminEventOperationsId(requestId) || (decision !== "APPROVE" && decision !== "REJECT") || !reason.trim() || reason.length > 500 || !isAdminEventOperationsId(idempotencyKey)) throw new Error("invalid_request");
    const response = await this.request<unknown>(`/events/${encodeURIComponent(eventId)}/refund-requests/${encodeURIComponent(requestId)}/decision`, {
      method: "POST", cache: "no-store", headers: { "Idempotency-Key": idempotencyKey }, body: JSON.stringify({ decision, reason }),
    });
    const bookingId = response && typeof response === "object" && "booking_id" in response ? String(response.booking_id) : "";
    return parseBookingRefund(response, bookingId);
  }

  async listEventRefundApprovers(eventId: string): Promise<{ assignments: ApiEventRefundApprover[] }> {
    if (!isAdminEventOperationsId(eventId)) throw new Error("invalid_request");
    const response = await this.request<unknown>(`/admin/events/${encodeURIComponent(eventId)}/refund-approvers`, { method: "GET", cache: "no-store" });
    return parseEventRefundApprovers(response, eventId);
  }

  async setEventRefundApprover(eventId: string, staffUserId: string, active: boolean): Promise<ApiEventRefundApprover> {
    if (!isAdminEventOperationsId(eventId) || !isAdminEventOperationsId(staffUserId)) throw new Error("invalid_request");
    const response = await this.request<unknown>(`/admin/events/${encodeURIComponent(eventId)}/refund-approvers/${encodeURIComponent(staffUserId)}`, {
      method: active ? "PUT" : "DELETE", cache: "no-store", body: active ? JSON.stringify({}) : undefined,
    });
    return parseEventRefundApprovers({ assignments: [response] }, eventId).assignments[0];
  }

  async listOwnedTickets(): Promise<{ tickets: ApiTicket[] }> {
    return this.request<{ tickets: ApiTicket[] }>("/tickets", { cache: "no-store" });
  }

  async rotateTicketCredential(ticketId: string, idempotencyKey: string): Promise<ApiTicketCredential> {
    return this.request<ApiTicketCredential>(`/tickets/${encodeURIComponent(ticketId)}/credential`, {
      method: "POST",
      cache: "no-store",
      headers: { "Idempotency-Key": idempotencyKey },
    });
  }

  async listEventScannerAssignments(eventId: string): Promise<{ assignments: ApiScannerAssignment[] }> {
    return this.request<{ assignments: ApiScannerAssignment[] }>(`/events/${encodeURIComponent(eventId)}/scanners`, { cache: "no-store" });
  }

  async grantEventScanner(eventId: string, staffUserId: string): Promise<ApiScannerAssignment> {
    return this.request<ApiScannerAssignment>(`/events/${encodeURIComponent(eventId)}/scanners/${encodeURIComponent(staffUserId)}`, { method: "PUT", cache: "no-store" });
  }

  async revokeEventScanner(eventId: string, staffUserId: string): Promise<ApiScannerAssignment> {
    return this.request<ApiScannerAssignment>(`/events/${encodeURIComponent(eventId)}/scanners/${encodeURIComponent(staffUserId)}`, { method: "DELETE", cache: "no-store" });
  }

  async checkInTicket(eventId: string, credential: string, idempotencyKey: string): Promise<ApiTicketCheckInResult> {
    return this.request<ApiTicketCheckInResult>(`/events/${encodeURIComponent(eventId)}/check-in`, {
      method: "POST", cache: "no-store", headers: { "Idempotency-Key": idempotencyKey }, body: JSON.stringify({ credential }),
    });
  }

  // ── Seats ───────────────────────────────────────────────
  async getSeats(params: ApiGetSeatsParams): Promise<{ seats: ApiSeat[] }> {
    const query = new URLSearchParams();
    query.set("event_id", params.event_id);
    if (params.ticket_tier_id) {
      query.set("ticket_tier_id", params.ticket_tier_id);
    }
    const qs = query.toString();
    return this.request<{ seats: ApiSeat[] }>(`/seats${qs ? `?${qs}` : ""}`);
  }

  async updateSeatStatus(params: ApiUpdateSeatStatusParams): Promise<ApiSeat> {
    const body: Record<string, string> = {
      seat_id: params.seat_id,
      status: params.status,
    };
    if (params.booking_id) {
      body.booking_id = params.booking_id;
    }
    return this.request<ApiSeat>("/seats/status", {
      method: "PATCH",
      body: JSON.stringify(body),
    });
  }

  // ── Scheduler (admin) ─────────────────────────────────
  async listSchedulerJobs(signal?: AbortSignal): Promise<ApiListSchedulersResponse> {
    return this.request<ApiListSchedulersResponse>("/scheduler/jobs", {
      cache: "no-store",
      ...(signal ? { signal } : {}),
    });
  }

  async createSchedulerJob(params: ApiCreateSchedulerJobParams): Promise<ApiSchedulerJob> {
    const response = await this.request<{ scheduler: ApiSchedulerJob }>("/scheduler/jobs", {
      method: "POST",
      body: JSON.stringify(params),
    });
    return response.scheduler;
  }

  async updateSchedulerJob(params: ApiUpdateSchedulerJobParams): Promise<void> {
    await this.request<{ message: string }>(`/scheduler/jobs/${params.id}`, {
      method: "PATCH",
      body: JSON.stringify({
        is_enable: params.is_enable,
        cron_expression: params.cron_expression,
        timeout: params.timeout,
        expected_version: params.expected_version,
      }),
    });
  }

  // ── Token state check ─────────────────────────────────
  eventInterestsRequest<T>(path: string, options: RequestInit = {}): Promise<T> {
    return this.request<T>(path, { cache: "no-store", ...options });
  }

  isLoggedIn(): boolean {
    return !!this.getToken();
  }
}

export const apiClient = new ApiClient();
