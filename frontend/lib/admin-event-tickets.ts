import { AdminEventOperationsError, isAdminEventOperationsId } from "./admin-event-operations";
import type { ApiAdminEventTicketsQuery, ApiAdminEventTicketsResponse, ApiAdminEventTicketRecord, ApiAdminEventTicketStatus } from "./api/types";

const statuses = new Set<string>(["ALL", "ISSUED", "CHECKED_IN", "REVOKED"]);
const cursorPattern = /^[A-Za-z0-9_-]{1,2048}$/;
type NormalizedQuery = { status: ApiAdminEventTicketStatus; page_size: number; cursor?: string };
export function normalizeAdminEventTicketsQuery(query: ApiAdminEventTicketsQuery = {}): NormalizedQuery {
  const status = query.status ?? "ALL";
  const page_size = query.page_size ?? 20;
  if (!statuses.has(status) || !Number.isInteger(page_size) || page_size < 1 || page_size > 100 ||
      (query.cursor !== undefined && !cursorPattern.test(query.cursor))) throw new AdminEventOperationsError("invalid_request");
  return { status, page_size, ...(query.cursor === undefined ? {} : { cursor: query.cursor }) };
}
function record(value: unknown): value is Record<string, unknown> { return !!value && typeof value === "object" && !Array.isArray(value); }
function timestamp(value: unknown): value is string {
  if (typeof value !== "string" || value.length > 40) return false;
  const match = /^(\d{4})-(\d{2})-(\d{2})T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,9})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.exec(value);
  if (!match || !Number.isFinite(Date.parse(value))) return false;
  const date = new Date(0); date.setUTCFullYear(+match[1], +match[2] - 1, +match[3]);
  return date.getUTCFullYear() === +match[1] && date.getUTCMonth() === +match[2] - 1 && date.getUTCDate() === +match[3];
}
function invalid(): never { throw new AdminEventOperationsError("invalid_response"); }
/** Allowlist only safe recorded-ticket fields; never expose unexpected source payload. */
export function parseAdminEventTickets(value: unknown, eventId: string, query: ApiAdminEventTicketsQuery = {}): ApiAdminEventTicketsResponse {
  const expected = normalizeAdminEventTicketsQuery(query);
  if (!record(value) || !isAdminEventOperationsId(eventId) || typeof value.event_id !== "string" ||
      value.event_id.toLowerCase() !== eventId.toLowerCase() || !timestamp(value.observed_at) ||
      value.source_service !== "booking" || value.source_status !== "available" || value.coverage !== "recorded_ticket_snapshot" ||
      value.source_record_limit !== 5000 || value.status_filter !== expected.status || value.page_size !== expected.page_size ||
      typeof value.has_more !== "boolean" || !Array.isArray(value.tickets) || value.tickets.length > expected.page_size ||
      (value.has_more ? typeof value.next_cursor !== "string" || !cursorPattern.test(value.next_cursor) || value.tickets.length !== expected.page_size : value.next_cursor !== null)) return invalid();
  const seen = new Set<string>();
  const tickets: ApiAdminEventTicketRecord[] = [];
  for (const item of value.tickets) {
    if (!record(item) || !isAdminEventOperationsId(item.ticket_id) || seen.has(item.ticket_id.toLowerCase()) ||
        typeof item.status !== "string" || item.status === "ALL" || !statuses.has(item.status) ||
        (expected.status !== "ALL" && item.status !== expected.status) || !timestamp(item.issued_at) ||
        !(item.checked_in_at === null || timestamp(item.checked_in_at)) || !(item.revoked_at === null || timestamp(item.revoked_at)) ||
        Date.parse(item.issued_at) > Date.parse(value.observed_at) ||
        (item.checked_in_at !== null && (Date.parse(item.checked_in_at) < Date.parse(item.issued_at) || Date.parse(item.checked_in_at) > Date.parse(value.observed_at))) ||
        (item.revoked_at !== null && (Date.parse(item.revoked_at) < Date.parse(item.issued_at) || Date.parse(item.revoked_at) > Date.parse(value.observed_at))) ||
        (item.status === "ISSUED" && (item.checked_in_at !== null || item.revoked_at !== null)) ||
        (item.status === "CHECKED_IN" && (item.checked_in_at === null || item.revoked_at !== null)) ||
        (item.status === "REVOKED" && item.revoked_at === null)) return invalid();
    seen.add(item.ticket_id.toLowerCase());
    tickets.push({ ticket_id: item.ticket_id, status: item.status as ApiAdminEventTicketRecord["status"], issued_at: item.issued_at, checked_in_at: item.checked_in_at, revoked_at: item.revoked_at });
  }
  return { event_id: value.event_id, observed_at: value.observed_at, source_service: "booking", source_status: "available", coverage: "recorded_ticket_snapshot", source_record_limit: 5000, status_filter: expected.status, page_size: expected.page_size, has_more: value.has_more, next_cursor: value.next_cursor as string | null, tickets };
}
