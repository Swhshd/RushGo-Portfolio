import type { CartItem } from "./types";

const requestSlot = (userId: string) => `ticketbox.booking-request.v1:${userId}`;
const cartSlot = (userId: string) => `ticketbox.booking-cart.v1:${userId}`;

export function bookingCartFingerprint(cart: CartItem): string {
  const tiers = new Map<string, string[]>();
  const seen = new Set<string>();
  for (const seat of cart.seats) {
    if (!seat.seatId || !seat.tierId || seen.has(seat.seatId)) throw new Error("Invalid selected seats");
    seen.add(seat.seatId);
    const seats = tiers.get(seat.tierId) ?? [];
    seats.push(seat.seatId.toLowerCase());
    tiers.set(seat.tierId, seats);
  }
  return JSON.stringify(["v1", cart.eventId.toLowerCase(), "pessimistic", Array.from(tiers.entries()).map(([tier, seats]) => [tier.toLowerCase(), seats.sort()]).sort((a, b) => String(a[0]).localeCompare(String(b[0])))]);
}

export function getOrCreateBookingRequest(userId: string, fingerprint: string): string {
  if (!userId || !fingerprint) throw new Error("Booking identity is required");
  const slot = requestSlot(userId);
  const raw = sessionStorage.getItem(slot);
  if (raw) {
    try {
      const saved = JSON.parse(raw);
      if (saved.fingerprint === fingerprint && typeof saved.key === "string" && /^[A-Za-z0-9_-]{1,128}$/.test(saved.key)) return saved.key;
    } catch { /* Replace malformed local data before sending any request. */ }
  }
  const key = crypto.randomUUID();
  // If persistence fails, fail before POST: allocating a new key on retry
  // after an unknown network result would create a second purchase.
  sessionStorage.setItem(slot, JSON.stringify({ fingerprint, key }));
  return key;
}

export function clearBookingRequest(userId: string): void {
  sessionStorage.removeItem(requestSlot(userId));
  sessionStorage.removeItem(cartSlot(userId));
}

export function saveBookingCart(userId: string, cart: CartItem): void {
  sessionStorage.setItem(cartSlot(userId), JSON.stringify(cart));
}

export function restoreBookingCart(userId: string): CartItem | null {
  try {
    const raw = sessionStorage.getItem(cartSlot(userId));
    if (!raw) return null;
    const cart = JSON.parse(raw) as CartItem;
    if (!cart.eventId || !Array.isArray(cart.seats) || cart.seats.length === 0) return null;
    bookingCartFingerprint(cart);
    return cart;
  } catch { return null; }
}
