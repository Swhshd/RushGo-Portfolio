import type { SelectedSeat, VenueLayout } from "./types";

export interface GuestSeatDraftSeat {
  seatId: string;
  sectionId: string;
}

export interface GuestSeatDraft {
  version: 1;
  eventId: string;
  returnTo: string;
  seats: GuestSeatDraftSeat[];
}

interface GuestSeatDraftInput {
  eventId: string;
  seats: Array<GuestSeatDraftSeat | { seatId: string; sectionId: string }>;
  returnTo?: string;
}

const STORAGE_KEY = "rushgo.guest-seat-draft.v1";

interface DraftStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

function browserStorage(): DraftStorage | null {
  if (typeof window === "undefined") return null;
  try {
    return window.sessionStorage;
  } catch {
    return null;
  }
}

export function guestSeatReturnPath(eventId: string): string {
  return eventId ? "/events/" + encodeURIComponent(eventId) + "/seats" : "/";
}

function normalizeSeats(value: unknown): GuestSeatDraftSeat[] {
  if (!Array.isArray(value)) return [];
  const seen = new Set<string>();
  const seats: GuestSeatDraftSeat[] = [];
  for (const candidate of value) {
    if (!candidate || typeof candidate !== "object") continue;
    const seat = candidate as Partial<GuestSeatDraftSeat>;
    if (typeof seat.seatId !== "string" || !seat.seatId.trim()) continue;
    if (typeof seat.sectionId !== "string" || !seat.sectionId.trim()) continue;
    const key = seat.sectionId + "\u0000" + seat.seatId;
    if (seen.has(key)) continue;
    seen.add(key);
    seats.push({ seatId: seat.seatId, sectionId: seat.sectionId });
  }
  return seats;
}

export function saveGuestSeatDraft(
  input: GuestSeatDraftInput,
  storage: DraftStorage | null = browserStorage(),
): boolean {
  if (!storage || typeof input.eventId !== "string" || !input.eventId.trim()) return false;
  const seats = normalizeSeats(input.seats);
  if (seats.length === 0) {
    clearGuestSeatDraft(storage);
    return true;
  }
  const draft: GuestSeatDraft = {
    version: 1,
    eventId: input.eventId,
    returnTo: guestSeatReturnPath(input.eventId),
    seats,
  };
  try {
    storage.setItem(STORAGE_KEY, JSON.stringify(draft));
    return true;
  } catch {
    return false;
  }
}

export function readGuestSeatDraft(
  eventId: string,
  storage: DraftStorage | null = browserStorage(),
): GuestSeatDraft | null {
  if (!storage) return null;
  try {
    const raw = storage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const value = JSON.parse(raw) as Partial<GuestSeatDraft>;
    if (value.version !== 1 || value.eventId !== eventId) return null;
    const seats = normalizeSeats(value.seats);
    if (seats.length === 0) return null;
    return {
      version: 1,
      eventId,
      returnTo: guestSeatReturnPath(eventId),
      seats,
    };
  } catch {
    return null;
  }
}

export function clearGuestSeatDraft(storage: DraftStorage | null = browserStorage()): void {
  try {
    storage?.removeItem(STORAGE_KEY);
  } catch {
    // Draft cleanup is best-effort; it never affects a booking request.
  }
}

export function restoreGuestSeatSelection(
  draft: Pick<GuestSeatDraft, "seats">,
  layout: VenueLayout,
): { seats: SelectedSeat[]; invalidSeatIds: string[] } {
  const sections = new Map(layout.sections.map((section) => [section.id, section]));
  const restored: SelectedSeat[] = [];
  const invalidSeatIds: string[] = [];
  for (const choice of normalizeSeats(draft.seats)) {
    const section = sections.get(choice.sectionId);
    const row = section?.rows.find((candidate) => candidate.seats.some((seat) => seat.id === choice.seatId));
    const seat = row?.seats.find((candidate) => candidate.id === choice.seatId);
    if (!section || !row || !seat || seat.status !== "available") {
      invalidSeatIds.push(choice.seatId);
      continue;
    }
    restored.push({
      sectionId: section.id,
      sectionName: section.name,
      rowLabel: row.label,
      seatLabel: seat.label,
      seatId: seat.id,
      price: section.price,
      tierId: section.tierId || "",
    });
  }
  return { seats: restored, invalidSeatIds };
}