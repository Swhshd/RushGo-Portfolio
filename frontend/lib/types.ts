export interface TicketTier {
  id: string;
  name: string;
  description: string;
  price: number;
  available: number;
  // Server-projected sellability. Missing or false means not sellable.
  accountingReady?: boolean;
}

export interface VenueLayout {
  id: string;
  name: string;
  sections: VenueSection[];
  stage?: {
    label: string;
    shape: "rectangle" | "circle" | "polygon";
    position: { x: number; y: number; width: number; height: number };
  };
}

export interface VenueSection {
  id: string;
  name: string;
  tier: string;
  tierId?: string;
  price: number;
  color: string;
  path: string;
  labelPosition: { x: number; y: number };
  totalSeats: number;
  availableSeats: number;
  // Server-projected sellability for this section's tier.
  accountingReady?: boolean;
  rows: SeatRow[];
}

export interface SeatRow {
  id: string;
  label: string;
  seats: Seat[];
}

export interface Seat {
  id: string;
  label: string;
  status: "available" | "reserved" | "booked" | "selected";
  seatRevision: string;
  updatedAt?: string;
}

export interface SelectedSeat {
  sectionId: string;
  sectionName: string;
  rowLabel: string;
  seatLabel: string;
  seatId: string;
  price: number;
  tierId: string;
}

export interface Event {
  id: string;
  title: string;
  date: string;
  time: string;
  startsAt?: string; // Original API RFC3339 timestamp; mock-only events may omit it.
  venue: string;
  location: string;
  category: "Concerts" | "Sports" | "Films";
  genre: string;
  description: string;
  image: string;
  featured?: boolean;
  tiers: TicketTier[];
  venueLayout?: VenueLayout;
}

export interface CartItem {
  eventId: string;
  seats: SelectedSeat[];
}

export interface PurchasedTicket {
  id: string;
  eventId: string;
  tierName: string;
  quantity: number;
  totalPrice: number;
  purchasedAt: string;
  status: "Confirmed";
  seats?: { section: string; row: string; seat: string }[];
}
