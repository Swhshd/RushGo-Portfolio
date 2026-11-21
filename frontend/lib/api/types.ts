// Raw API response types (match backend JSON exactly)

export interface ApiUser {
  id: string;
  email: string;
  name: string;
  role: string;
  locale?: "en" | "zh" | string;
  created_at: string;
}

export interface ApiUpdateProfileInput { name: string; locale: "en" | "zh"; }
export interface ApiEmailChangeCodeResponse {
  message: string;
  verification_id: string;
  expires_at: string;
  next_resend_at: string;
}
export interface ApiSignInRequiredResponse { message: string; sign_in_required: true; }
export interface ApiNotification {
  id: string;
  type: string;
  title: string;
  body: string;
  created_at: string;
  read_at: string | null;
  detail_path: string;
  source: string;
  locale: "en" | "zh" | string;
}
export interface ApiNotificationPage {
  notifications: ApiNotification[];
  total: number;
  page: number;
  page_size: number;
  unread_count: number;
}

export interface ApiOperationsIssue {
  kind: string;
  source: string;
  state: string;
  observed_at: string;
  next_action: string;
}
export interface ApiOperationsItem {
  booking_id: string;
  event_id: string;
  booking_status: string;
  created_at: string;
  ticket_count: number;
  refund_status: string;
  payment_source_state: "available" | "unavailable" | "not_recorded" | string;
  issues: ApiOperationsIssue[];
  detail_path: string;
}
export interface ApiAdminOperationsResponse {
  items: ApiOperationsItem[];
  has_more: boolean;
  offset: number;
  limit: number;
  from: string;
  to: string;
  observed_at: string;
  scope: string;
  source: string;
  notification_path: string;
  notification_source_state: "available" | "unavailable";
  notification_items: ApiOperationsItem[];
  notification_has_more: boolean;
  notification_observed_at: string | null;
}
export interface ApiWorkItem {
  event_id: string;
  access: "scanner" | "approver" | "admin" | string;
  pending_refunds: number;
  title: string;
  starts_at: string | null;
  event_state: string;
  event_source_state: string;
}
export interface ApiWorkResponse {
  items: ApiWorkItem[];
  has_more: boolean;
  observed_at: string;
}

export interface ApiAuthResponse {
  access_token: string;
  refresh_token: string;
  user: ApiUser;
}

export interface ApiTicketTier {
  id: string;
  event_id: string;
  name: string;
  price_cents: number;
  total_quantity: number;
  available_quantity: number;
  version: number;
  // Read-only accounting readiness projection from the Event service. Missing
  // or false means the tier must not be offered for sale.
  accounting_ready?: boolean;
}

export interface ApiEvent {
  id: string;
  title: string;
  description: string;
  category: string;
  venue: string;
  location: string;
  date: string; // RFC3339
  date_version?: number;
  image_url: string;
  status: string;
  publication_status?: "draft" | "published" | string;
  sales_status?: "scheduled" | "selling" | "paused" | "ended" | "cancelled" | string;
  sales_start_at?: string | null;
  sales_end_at?: string | null;
  tiers: ApiTicketTier[];
  created_at: string;
}

export interface ApiListEventsResponse {
  events: ApiEvent[];
  total: number;
  page: number;
  page_size: number;
}

export interface ApiPublicFeaturesResponse {
  source_state: "available" | "unavailable" | string;
  flags: { discovery_advanced_filters: boolean };
}

export interface ApiFeatureFlag {
  key: string;
  description: string;
  default_enabled: boolean;
  enabled: boolean;
  version: number;
  updated_at: string | null;
  actor_id: string;
}

export interface ApiFeatureFlagAudit {
  key: string;
  before: boolean;
  after: boolean;
  version: number;
  actor_id: string;
  changed_at: string;
}

export interface ApiAdminFeatureFlagsResponse {
  flags: ApiFeatureFlag[];
  audit: ApiFeatureFlagAudit[];
  audit_limit: number;
  observed_at: string;
  source: string;
}

export interface ApiAdminMetricsMoneyGroup {
  currency: string;
  mode: string;
  state: "recorded_complete" | "partial_known" | string;
  payments: number;
  unknown_payments: number;
  confirmed_refunds: number;
  collected_known_minor: number;
  refunded_known_minor: number;
  net_minor: number | null;
}

export interface ApiAdminMetricsResponse {
  from: string;
  to: string;
  record_limit: number;
  scope: string;
  booking: {
    source: string;
    observed_at: string;
    orders: number;
    issued_tickets: number;
    checked_in_tickets: number;
    refund_requests: number;
    confirmed_refunds: number;
  };
  payment: {
    source: string;
    source_state: "available" | "unavailable" | string;
    unavailable_reason: string;
    observed_at: string | null;
    groups: ApiAdminMetricsMoneyGroup[];
  };
}

export type ApiBookingOutboxEventType =
  | "BookingConfirmed"
  | "BookingCancelled"
  | "BookingFailed"
  | "Other";

export type ApiNotificationOutboxEventType =
  | "booking_confirmation"
  | "booking_failure"
  | "booking_cancellation"
  | "welcome_email";

export type ApiAdminOutboxSource = "booking" | "notification";
export type ApiAdminOutboxEventType = ApiBookingOutboxEventType | ApiNotificationOutboxEventType;
export type ApiAdminOutboxFilterEventType = ApiAdminOutboxEventType;
export type ApiBookingOutboxRecordedStatus = "pending" | "published";
export type ApiNotificationOutboxRecordedStatus =
  | "pending"
  | "delivering"
  | "sent"
  | "retryable"
  | "unknown"
  | "attention";
export type ApiAdminOutboxRecordedStatus = ApiBookingOutboxRecordedStatus | ApiNotificationOutboxRecordedStatus;

type ApiAdminOutboxRecordFields = {
  event_id: string;
  created_at: string;
  consumer_outcome: "not_verified";
};

export type ApiBookingOutboxRecord = ApiAdminOutboxRecordFields & {
  source_service: "booking";
  event_type: ApiBookingOutboxEventType;
  recorded_status: ApiBookingOutboxRecordedStatus;
};

export type ApiNotificationOutboxRecord = ApiAdminOutboxRecordFields & {
  source_service: "notification";
  event_type: ApiNotificationOutboxEventType | "Other";
  recorded_status: ApiNotificationOutboxRecordedStatus;
};

export type ApiAdminOutboxRecord = ApiBookingOutboxRecord | ApiNotificationOutboxRecord;

export interface ApiAdminOutboxEventsQuery {
  source?: ApiAdminOutboxSource;
  status: "all" | ApiAdminOutboxRecordedStatus;
  event_type?: ApiAdminOutboxFilterEventType;
  from: string;
  to: string;
  limit: number;
  cursor?: string;
}

export interface ApiAdminOutboxEventsResponse {
  source_service: ApiAdminOutboxSource;
  records: ApiAdminOutboxRecord[];
  next_cursor: string;
  has_more: boolean;
  observed_at: string;
  window: { from: string; to: string };
  supported_event_types: ApiAdminOutboxEventType[];
  source_record_limit: number;
  consumer_outcome: "not_verified";
}

export interface ApiCreateEventTierInput {
  name: string;
  price_cents: number;
  total_quantity: number;
}

export interface ApiCreateEventInput {
  title: string;
  description: string;
  category: string;
  venue: string;
  location: string;
  date: string; // RFC3339
  image_url: string;
  publication_status?: "draft" | "published";
  sales_status?: "scheduled" | "selling" | "paused" | "ended" | "cancelled";
  sales_start_at?: string;
  sales_end_at?: string;
  tiers: ApiCreateEventTierInput[];
}

export interface ApiUpdateEventSalesInput {
  publication_status: "draft" | "published";
  sales_status: "scheduled" | "selling" | "paused" | "ended" | "cancelled";
  sales_start_at?: string;
  sales_end_at?: string;
  clear_sales_window: boolean;
}

export interface ApiUpdateEventInput {
  title: string;
  description: string;
  category: string;
  venue: string;
  location: string;
  date?: string; // RFC3339; omitted for metadata-only edits
  expected_date_version?: number;
  image_url: string;
}

export interface ApiBookingItem {
  id: string;
  ticket_tier_id: string;
  tier_name: string;
  quantity: number;
  unit_price_cents: number;
  seat_ids: string[];
}

export interface ApiBooking {
  id: string;
  user_id: string;
  event_id: string;
  status: string;
  total_amount_cents: number;
  currency?: string;
  items: ApiBookingItem[];
  created_at: string;
  paymentIntentClientSecret?: string;
  creation_state?: "starting" | "ready" | "terminal";
  retry_after_seconds?: number;
  lifecycle_generation?: number;
}

export type ApiBookingCheckoutState = "ready" | "processing" | "expired" | "completed" | "unavailable";

export interface ApiBookingResumeResponse extends ApiBooking {
  checkout_state: ApiBookingCheckoutState;
  reservation_deadline: string | null;
  can_cancel: boolean;
}
export interface ApiListBookingsResponse {
  bookings: ApiBooking[];
  total: number;
}

export interface ApiTicket {
  id: string;
  booking_id: string;
  event_id: string;
  seat_id: string;
  ticket_tier_id: string;
  status: "ISSUED" | "CHECKED_IN" | "REVOKED" | string;
  booking_status?: string;
  issued_at: string;
  checked_in_at: string | null;
}

export interface ApiTicketCredential {
  outcome: "ROTATED" | "ALREADY_APPLIED";
  credential?: string;
  expires_at: string;
  credential_version: number;
}

export interface ApiTicketCheckInResult {
  result: string;
  reason_code: string;
  checked_in_at: string | null;
  replayed: boolean;
  evidence_source: string;
  observed_at: string | null;
}

export interface ApiScannerAssignment {
  event_id: string;
  staff_user_id: string;
  active: boolean;
  changed_at: string;
}

export type ApiBookingRefundStatus =
  | "REQUESTED" | "WAITING_CHECKIN_RESOLUTION" | "APPROVED"
  | "REFUND_PROCESSING" | "REFUNDED" | "REJECTED" | "WITHDRAWN" | "NEEDS_ATTENTION";

export interface ApiBookingRefund {
  request_id: string;
  booking_id: string;
  event_id: string;
  status: ApiBookingRefundStatus | string;
  reason: string;
  decision_reason: string;
  refund_decision_id: string;
  amount_cents: number;
  currency: string;
  requested_at: string;
  decided_at: string;
  payment_refund_state: string;
  ticket_revocation_state: string;
  can_withdraw: boolean;
  approval_enabled: boolean;
  eligibility_code: string;
}

export interface ApiEventBookingRefundsResponse {
  refunds: ApiBookingRefund[];
  next_cursor: string;
  approval_enabled: boolean;
  approval_block_reason: string;
}

export interface ApiEventRefundApprover {
  event_id: string;
  staff_user_id: string;
  active: boolean;
  changed_at: string;
}

export interface ApiError {
  error: string;
}

// ── Seats ────────────────────────────────────────────────
export type ApiAdminEventTicketStatus = "ALL" | "ISSUED" | "CHECKED_IN" | "REVOKED";
export interface ApiAdminEventTicketsQuery {
  status?: ApiAdminEventTicketStatus;
  page_size?: number;
  cursor?: string;
}
export interface ApiAdminEventTicketRecord {
  ticket_id: string;
  status: Exclude<ApiAdminEventTicketStatus, "ALL">;
  issued_at: string;
  checked_in_at: string | null;
  revoked_at: string | null;
}
export interface ApiAdminEventTicketsResponse {
  event_id: string;
  observed_at: string;
  source_service: "booking";
  source_status: "available";
  coverage: "recorded_ticket_snapshot";
  source_record_limit: 5000;
  status_filter: ApiAdminEventTicketStatus;
  page_size: number;
  has_more: boolean;
  next_cursor: string | null;
  tickets: ApiAdminEventTicketRecord[];
}

export interface ApiSeatPosition {
  sectionId: string;
  row: string;
  seat: number;
  x: number;
  y: number;
}

export interface ApiSeat {
  id: string;
  event_id: string;
  ticket_tier_id: string;
  status: 'available' | 'reserved' | 'booked';
  position: ApiSeatPosition;
  created_at: string;
  updated_at: string;
  seat_revision: string;
}

export interface ApiSeatUpdate {
  seat_id: string;
  ticket_tier_id: string;
  status: 'available' | 'reserved' | 'booked';
  seat_revision: string;
  updated_at: string;
}

export interface ApiSeatUpdateBatch {
  event_id: string;
  message_id: string;
  published_at: string;
  seat_updates: ApiSeatUpdate[];
}

export interface ApiGetSeatsParams {
  event_id: string;
  ticket_tier_id?: string;
}

export interface ApiUpdateSeatStatusParams {
  seat_id: string;
  status: 'available' | 'reserved' | 'booked';
  booking_id?: string;
}

// ── Scheduler ────────────────────────────────────────────
export interface ApiSchedulerJob {
  id: string;
  name: string;
  job_type: string;
  cron_expression: string;
  is_enabled: boolean;
  valid: boolean;
  validation_error: string;
  version: number;
  timeout: number; // seconds
  created_at: string; // RFC3339
  updated_at: string; // RFC3339
}

export interface ApiListSchedulersResponse {
  schedulers: ApiSchedulerJob[];
}

export interface ApiUpdateSchedulerJobParams {
  id: string;
  is_enable: boolean;
  cron_expression: string;
  timeout: number; // seconds (1–600)
  expected_version: number;
}

export interface ApiCreateSchedulerJobParams {
  name: string;
  job_type: "reservation-cleaner-job";
  cron_expression: string;
  timeout: number; // seconds (1–600); new configs start disabled
}
export interface ApiRegisterInput { email: string; password: string; name: string; verification_id: string; verification_code: string; }
export interface ApiRegistrationResponse { account_created: boolean; message: string; }
export interface ApiRegistrationCodeResponse { verification_id: string; expires_at: string; next_resend_at: string; message: string; }
export interface ApiMessageResponse { message: string; }


export interface ApiAdminBookingDetailItem {
  id: string;
  ticket_tier_id: string;
  tier_name: string;
  quantity: number;
  unit_price_cents: number;
  seat_ids: string[];
}

export interface ApiAdminBookingDetailTicket {
  ticket_id: string;
  status: string;
  seat_id: string;
  ticket_tier_id: string;
  issued_at: string;
  checked_in_at: string | null;
  revoked_at: string | null;
}

export interface ApiAdminBookingDetailReceipt {
  outcome: string;
  reason_code: string;
  ticket_count: number;
  receipt_count: number;
  observed_at: string | null;
  created_at: string | null;
}

export interface ApiAdminBookingDetailRefund {
  request_id: string;
  status: string;
  reason: string;
  decision_reason: string;
  refund_decision_id: string;
  amount_cents: number;
  currency: string;
  requested_at: string | null;
  decided_at: string | null;
  payment_refund_state: string;
  ticket_revocation_state: string;
  can_withdraw: boolean;
  approval_enabled: boolean;
}

export interface ApiAdminBookingDetail {
  booking_id: string;
  user_id: string;
  event_id: string;
  booking_status: string;
  total_amount_cents: number;
  currency: string;
  booking_mode: string;
  lifecycle_generation: number;
  lifecycle_phase: string;
  created_at: string;
  observed_at: string;
  items: ApiAdminBookingDetailItem[];
  tickets: ApiAdminBookingDetailTicket[];
  receipt: ApiAdminBookingDetailReceipt | null;
  refund: ApiAdminBookingDetailRefund | null;
  source_service: "booking";
  coverage: "booking_owned_exact";
}
