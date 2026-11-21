import type { ApiCreateEventInput } from "./api/types";

export const PENDING_EVENT_CREATE_KEY = "rushgo:admin:event-create:v1";

export interface PendingEventCreate {
  version: 1;
  idempotencyKey: string;
  payload: ApiCreateEventInput;
}

interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

function browserStorage(): StorageLike {
  if (typeof window === "undefined") throw new Error("event_create_storage_unavailable");
  return window.sessionStorage;
}

function validPayload(value: unknown): value is ApiCreateEventInput {
  if (!value || typeof value !== "object") return false;
  const payload = value as Partial<ApiCreateEventInput>;
  return typeof payload.title === "string" &&
    typeof payload.description === "string" &&
    typeof payload.category === "string" &&
    typeof payload.venue === "string" &&
    typeof payload.location === "string" &&
    typeof payload.date === "string" &&
    typeof payload.image_url === "string" &&
    (payload.publication_status === "draft" || payload.publication_status === "published") &&
    ["scheduled", "selling", "paused", "ended", "cancelled"].includes(String(payload.sales_status)) &&
    typeof payload.sales_start_at === "string" &&
    typeof payload.sales_end_at === "string" &&
    Array.isArray(payload.tiers) && payload.tiers.length > 0 &&
    payload.tiers.every((tier) => tier && typeof tier.name === "string" && Number.isInteger(tier.price_cents) && Number.isInteger(tier.total_quantity));
}

function validUuid(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

export function savePendingEventCreate(
  payload: ApiCreateEventInput,
  storage: StorageLike = browserStorage(),
  idempotencyKey: string = crypto.randomUUID(),
): PendingEventCreate {
  if (!validPayload(payload) || !validUuid(idempotencyKey)) throw new Error("invalid_event_create_draft");
  const pending: PendingEventCreate = { version: 1, idempotencyKey, payload };
  storage.setItem(PENDING_EVENT_CREATE_KEY, JSON.stringify(pending));
  return pending;
}

export function readPendingEventCreate(storage?: StorageLike): PendingEventCreate | null {
  let resolved: StorageLike;
  try { resolved = storage ?? browserStorage(); } catch { return null; }
  let raw: string | null;
  try {
    raw = resolved.getItem(PENDING_EVENT_CREATE_KEY);
  } catch {
    return null;
  }
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as Partial<PendingEventCreate>;
    if (parsed.version === 1 && validUuid(parsed.idempotencyKey) && validPayload(parsed.payload)) {
      return parsed as PendingEventCreate;
    }
  } catch {
    // A malformed local draft must not be replayed as an API request.
  }
  try { resolved.removeItem(PENDING_EVENT_CREATE_KEY); } catch { /* best effort */ }
  return null;
}

export function clearPendingEventCreate(storage: StorageLike = browserStorage()): void {
  storage.removeItem(PENDING_EVENT_CREATE_KEY);
}
