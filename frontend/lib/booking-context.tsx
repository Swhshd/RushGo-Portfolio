"use client";

import React, { createContext, useContext, useState, useCallback, useEffect, useMemo, useRef, useSyncExternalStore } from "react";
import { Event, CartItem, PurchasedTicket } from "./types";
import { apiClient } from "./api/client";
import { transformEvent, transformBookingToTicket, isConfirmedBooking } from "./api/transformers";
import { useAuth } from "./auth-context";
import { ApiError, bookingFailure, bookingFailureMessage } from "./api/errors";
import { bookingCartFingerprint, getOrCreateBookingRequest, clearBookingRequest, saveBookingCart, restoreBookingCart } from "./booking-request";

interface BookingState {
  events: Event[];
  cart: CartItem | null;
  purchasedTickets: PurchasedTicket[];
  lastPurchaseSuccess: boolean;
  lastPurchaseSuccessUserId: string | null;
  isLoading: boolean;
  error: string | null;
  paymentClientSecret: string | null;
  pendingBookingId: string | null;
  pendingBookingTotalCents: number | null;
  creationState: "starting" | "ready" | "terminal" | "confirmed" | null;
  setCart: (item: CartItem | null) => void;
  initiateBooking: (cartOverride?: CartItem) => Promise<{ clientSecret: string; bookingId: string } | null>;
  confirmPayment: (paymentIntentId: string) => Promise<boolean>;
  clearPaymentState: () => void;
  purchaseTickets: (name: string, email: string) => Promise<boolean>;
  clearSuccessFlag: () => void;
  getEvent: (id: string) => Event | undefined;
  ensureEvent: (id: string, refresh?: boolean) => Promise<Event>;
  refreshEvents: (eventId?: string) => Promise<void>;
}

const BookingContext = createContext<BookingState | null>(null);

export function BookingProvider({ children }: { children: React.ReactNode }) {
  const { user, isLoading: authLoading } = useAuth();
  const [events, setEvents] = useState<Event[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const eventsRef = useRef<Event[]>([]);
  const eventLoads = useRef(new Map<string, Promise<Event>>());
  const pendingEventLoads = useRef(0);
  const eventsMounted = useRef(true);

  useEffect(() => {
    eventsMounted.current = true;
    return () => { eventsMounted.current = false; };
  }, []);

  const ensureEvent = useCallback((id: string, refresh = false): Promise<Event> => {
    const cached = eventsRef.current.find((event) => event.id === id);
    if (cached && !refresh) return Promise.resolve(cached);
    const activeLoad = eventLoads.current.get(id);
    if (activeLoad) return activeLoad;

    if (pendingEventLoads.current++ === 0) setIsLoading(true);
    const request = apiClient.getEvent(id)
      .then((response) => {
        const event = transformEvent(response);
        if (eventsMounted.current) {
          setEvents((current) => {
            const index = current.findIndex((item) => item.id === event.id);
            const next = index < 0
              ? [...current, event]
              : current.map((item) => item.id === event.id ? event : item);
            eventsRef.current = next;
            return next;
          });
        }
        return event;
      })
      .finally(() => {
        eventLoads.current.delete(id);
        pendingEventLoads.current = Math.max(0, pendingEventLoads.current - 1);
        if (eventsMounted.current && pendingEventLoads.current === 0) setIsLoading(false);
      });
    eventLoads.current.set(id, request);
    return request;
  }, []);

  const refreshEvents = useCallback(async (eventId?: string) => {
    if (!eventId || !eventsMounted.current) return;
    try {
      await ensureEvent(eventId, true);
    } catch {
      // Keep the last authoritative event. Callers provide a bounded retry state.
    }
  }, [ensureEvent]);

  // Keep event data shared, while the stable keyed child owns only the current
  // identity's cart, order, payment, and success state. Profile refreshes with
  // the same id do not remount it; a real owner change does intentionally
  // remount its children so prior-owner checkout state cannot cross the boundary.
  const ownerKey = user ? `user:${user.id}` : "anonymous";
  return (
    <BookingSession
      key={ownerKey}
      user={user}
      authLoading={authLoading}
      events={events}
      isLoading={isLoading}
      ensureEvent={ensureEvent}
      refreshEvents={refreshEvents}
    >
      {children}
    </BookingSession>
  );
}

interface BookingSessionProps {
  children: React.ReactNode;
  user: { id: string } | null;
  authLoading: boolean;
  events: Event[];
  isLoading: boolean;
  ensureEvent: (id: string, refresh?: boolean) => Promise<Event>;
  refreshEvents: (eventId?: string) => Promise<void>;
}

const emptySubscribe = () => () => {};

function BookingSession({ children, user, authLoading, events, isLoading, ensureEvent, refreshEvents }: BookingSessionProps) {
  const hydrated = useSyncExternalStore(emptySubscribe, () => true, () => false);
  const userId = user?.id;
  const [cartSelection, setCartSelection] = useState<{ value: CartItem | null } | null>(null);
  const restoredCart = useMemo(
    () => hydrated && userId ? restoreBookingCart(userId) : null,
    [hydrated, userId],
  );
  const cart = cartSelection
    ? cartSelection.value
    : restoredCart;
  const [purchasedTickets, setPurchasedTickets] = useState<PurchasedTicket[]>([]);
  const [lastPurchaseSuccess, setLastPurchaseSuccess] = useState(false);
  const [lastPurchaseSuccessUserId, setLastPurchaseSuccessUserId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [paymentClientSecret, setPaymentClientSecret] = useState<string | null>(null);
  const [pendingBookingId, setPendingBookingId] = useState<string | null>(null);
  const [pendingBookingTotalCents, setPendingBookingTotalCents] = useState<number | null>(null);
  const [creationState, setCreationState] = useState<"starting" | "ready" | "terminal" | "confirmed" | null>(null);
  const flight = useRef<{ userId: string; fingerprint: string; promise: Promise<{ clientSecret: string; bookingId: string } | null> } | null>(null);
  const epoch = useRef(0);
  const selectedFingerprint = useRef<string | null>(null);
  const mounted = useRef(true);

  useEffect(() => {
    // StrictMode replays cleanup/setup on the same mounted instance. Keep the
    // request epoch and flight intact across that replay so the second setup
    // shares the in-flight request instead of issuing another creation POST.
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const setCart = useCallback((item: CartItem | null) => {
    const fingerprint = item ? bookingCartFingerprint(item) : null;
    if (selectedFingerprint.current !== fingerprint) {
      epoch.current++;
      flight.current = null;
      setPaymentClientSecret(null);
      setPendingBookingId(null);
      setPendingBookingTotalCents(null);
      setCreationState(null);
    }
    selectedFingerprint.current = fingerprint;
    if (userId && item) saveBookingCart(userId, item);
    else if (userId) clearBookingRequest(userId);
    setCartSelection({ value: item });
  }, [userId]);

  const fetchEvent = useCallback(async (eventId: string) => {
    setError(null);
    await refreshEvents(eventId);
  }, [refreshEvents]);

  useEffect(() => {
    if (!cart || events.some((event) => event.id === cart.eventId)) return;
    void ensureEvent(cart.eventId).catch(() => {});
  }, [cart?.eventId, ensureEvent, events]);

  const clearSuccessFlag = useCallback(() => {
    setLastPurchaseSuccess(false);
    setLastPurchaseSuccessUserId(null);
  }, []);

  const clearPaymentState = useCallback(() => {
    setPaymentClientSecret(null);
    setPendingBookingId(null);
    setPendingBookingTotalCents(null);
    setError(null);
  }, []);

  const getEvent = useCallback(
    (id: string) => events.find((e) => e.id === id),
    [events]
  );

  const initiateBooking = useCallback((cartOverride?: CartItem): Promise<{ clientSecret: string; bookingId: string } | null> => {
    if (!user || authLoading) return Promise.reject(new ApiError("AUTH_REQUIRED", "Sign in to continue with your booking."));
    const bookingCart = cartOverride ?? cart;
    if (!bookingCart || bookingCart.seats.length === 0) return Promise.resolve(null);

    const event = events.find((e) => e.id === bookingCart.eventId);
    if (!event) return Promise.resolve(null);

    const fingerprint = bookingCartFingerprint(bookingCart);
    if (flight.current?.userId === user.id && flight.current.fingerprint === fingerprint) return flight.current.promise;
    const version = apiClient.getSessionVersion();
    const requestEpoch = ++epoch.current;
    selectedFingerprint.current = fingerprint;
    const current = () => mounted.current && requestEpoch === epoch.current && version === apiClient.getSessionVersion();

    // Clear previous error and payment state
    setError(null);
    clearPaymentState();
    setCreationState("starting");

    const promise = (async () => {
    try {
      const requestId = getOrCreateBookingRequest(user.id, fingerprint);
      saveBookingCart(user.id, bookingCart);
      // Group seats by tier ID using the tierId from cart seats
      const itemMap = new Map<string, { ticket_tier_id: string; quantity: number; seat_ids: string[] }>();
      for (const seat of bookingCart.seats) {
        if (!seat.tierId) continue;
        const existing = itemMap.get(seat.tierId);
        if (existing) {
          existing.quantity += 1;
          existing.seat_ids.push(seat.seatId);
        } else {
          itemMap.set(seat.tierId, { ticket_tier_id: seat.tierId, quantity: 1, seat_ids: [seat.seatId] });
        }
      }

      const items = Array.from(itemMap.values());
      if (items.length === 0) return null;

      let booking = await apiClient.createBooking(bookingCart.eventId, items, requestId);
      for (let attempt = 1; attempt < 10 && booking.creation_state === "starting"; attempt++) {
        if (!current()) return null;
        setPendingBookingId(booking.id);
        await new Promise((resolve) => setTimeout(resolve, 1000));
        if (!current()) return null;
        const next = await apiClient.createBooking(bookingCart.eventId, items, requestId);
        if (next.id !== booking.id) throw new Error("Booking recovery identity changed");
        booking = next;
      }
      if (!current()) return null;
      if (booking.creation_state === "terminal") {
        if (booking.status?.toUpperCase() === "CONFIRMED") {
          setPendingBookingId(null);
          setCreationState("confirmed");
          setError("This purchase is already confirmed. View it in My Tickets.");
        } else {
          setPendingBookingId(null);
          setCreationState("terminal");
          setError("This booking has ended. Choose seats to start a new purchase.");
        }
        return null;
      }
      if (booking.creation_state === "starting") {
        setPendingBookingId(booking.id);
        setError("Your booking is still being prepared. Retry to recover this purchase.");
        return null;
      }
      if (booking.creation_state !== "ready") throw new Error("Unknown booking creation state");

      if (!booking.paymentIntentClientSecret) {
        throw new ApiError("PAYMENT_INIT_ERROR", "Payment client secret not returned from server");
      }
      if (!Number.isSafeInteger(booking.total_amount_cents) || booking.total_amount_cents <= 0) {
        throw new Error("Authoritative booking total not returned from server");
      }

      // Store pending booking info for after payment confirmation
      setPendingBookingId(booking.id);
      setPendingBookingTotalCents(booking.total_amount_cents);
      setPaymentClientSecret(booking.paymentIntentClientSecret);
      setCreationState("ready");

      return {
        clientSecret: booking.paymentIntentClientSecret,
        bookingId: booking.id,
      };
    } catch (err) {
      if (!current()) return null;
      const failure = bookingFailure(err);
      setError(bookingFailureMessage(failure));
      throw failure;
    }
    })();
    flight.current = { userId: user.id, fingerprint, promise };
    const release = () => { if (flight.current?.promise === promise) flight.current = null; };
    void promise.then(release, release);
    return promise;
  }, [cart, events, clearPaymentState, user, authLoading]);

  const confirmPayment = useCallback(async (): Promise<boolean> => {
    if (!pendingBookingId || !cart) return false;

    const event = events.find((e) => e.id === cart.eventId);
    if (!event) return false;
    const requestEpoch = epoch.current;
    const version = apiClient.getSessionVersion();
    const current = () => mounted.current && requestEpoch === epoch.current && version === apiClient.getSessionVersion();

    try {
      // Stripe success precedes webhook/Saga completion. Bound reconciliation
      // and never project a pending or terminal failed booking into a ticket.
      let booking = await apiClient.getBooking(pendingBookingId);
      if (!current()) return false;
      for (let attempt = 1; attempt < 10 && !isConfirmedBooking(booking); attempt++) {
        if (typeof booking.status !== "string" || !["PENDING", "WAIT_FOR_PAYMENT", "PROCESSING"].includes(booking.status.toUpperCase())) break;
        await new Promise((resolve) => setTimeout(resolve, 500));
        if (!current()) return false;
        booking = await apiClient.getBooking(pendingBookingId);
        if (!current()) return false;
      }
      const ticket = transformBookingToTicket(booking);
      if (!ticket) {
        setError("Your booking is not confirmed. Please check My Tickets shortly.");
        return false;
      }

      // Add seat details from cart
      ticket.seats = cart.seats.map((s) => ({
        section: s.sectionName,
        row: s.rowLabel,
        seat: s.seatLabel,
      }));

      setPurchasedTickets((prev) => [ticket, ...prev.filter((existing) => existing.id !== ticket.id)]);
      setCart(null);
      setLastPurchaseSuccess(true);
      setLastPurchaseSuccessUserId(userId ?? null);

      // Clear payment state
      clearPaymentState();

      // Refresh events to get updated availability
      void fetchEvent(cart.eventId);
      return true;
    } catch (err) {
      if (!current()) return false;
      console.error("Payment confirmation failed:", err);
      setError(err instanceof Error ? err.message : "Failed to confirm payment");
      return false;
    }
  }, [pendingBookingId, cart, events, clearPaymentState, fetchEvent, setCart, userId]);

  const purchaseTickets = useCallback(
    async (): Promise<boolean> => {
      // Legacy method - redirects to new flow
      // For backwards compatibility
      try { return (await initiateBooking()) !== null; } catch { return false; }
    },
    [initiateBooking]
  );

  return (
    <BookingContext.Provider
      value={{
        events,
        cart,
        purchasedTickets,
        lastPurchaseSuccess,
        lastPurchaseSuccessUserId,
        isLoading,
        error,
        paymentClientSecret,
        pendingBookingId,
        pendingBookingTotalCents,
        creationState,
        setCart,
        initiateBooking,
        confirmPayment,
        clearPaymentState,
        purchaseTickets,
        clearSuccessFlag,
        getEvent,
        ensureEvent,
        refreshEvents,
      }}
    >
      {children}
    </BookingContext.Provider>
  );
}

export function useBooking() {
  const ctx = useContext(BookingContext);
  if (!ctx) throw new Error("useBooking must be used within BookingProvider");
  return ctx;
}
