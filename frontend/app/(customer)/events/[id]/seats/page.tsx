"use client";

import { useState, useMemo, useEffect, useCallback, useRef } from "react";
import { useParams, useRouter } from "next/navigation";
import { Check, ChevronRight, X, Armchair, Loader2 } from "lucide-react";
import { useBooking } from "@/lib/booking-context";
import { useAuth } from "@/lib/auth-context";
import { loginHref } from "@/lib/auth-navigation";
import { clearGuestSeatDraft, guestSeatReturnPath, readGuestSeatDraft, restoreGuestSeatSelection, saveGuestSeatDraft } from "@/lib/guest-seat-draft";
import { bookingFailure, bookingFailureMessage } from "@/lib/api/errors";
import { VenueMap } from "@/components/venue-map";
import SectionPicker from "@/components/section-picker";
import { apiClient } from "@/lib/api/client";
import { generateVenueLayoutFromSeats } from "@/lib/api/transformers";
import { applyUpdatesToVenueLayout, canContinueWithSeatInventory, createSeatRealtimeSession, invalidateUnavailableSelections } from "@/lib/api/seat-realtime.mjs";
import type { Event, Seat, SeatRow, VenueSection, SelectedSeat, VenueLayout } from "@/lib/types";
import type { ApiSeat } from "@/lib/api/types";
import { useI18n, type TranslationKey } from "@/lib/i18n/index";

type SeatsError = { kind: "message"; value: string } | { kind: "invalidated"; count: number } | { kind: "inventory" };

function layoutFromSnapshot(seats: ApiSeat[], event: Event, eventId: string): VenueLayout {
  return generateVenueLayoutFromSeats(
    seats,
    event.tiers.map((tier) => ({
      id: tier.id,
      event_id: eventId,
      name: tier.name,
      price_cents: Math.round(tier.price * 100),
      total_quantity: tier.available,
      available_quantity: tier.available,
      version: 0,
      accounting_ready: tier.accountingReady,
    })),
    event.category.toLowerCase(),
  );
}

export default function SeatsPage() {
  const { t, text, date, number, usdCents } = useI18n();
  const params = useParams();
  const router = useRouter();
  const { getEvent, ensureEvent, setCart, isLoading, initiateBooking, clearPaymentState } = useBooking();
  const { user, isLoading: authLoading } = useAuth();

  const eventId = params.id as string;
  const event = getEvent(eventId);
  const [eventLoadErrorId, setEventLoadErrorId] = useState<string | null>(null);
  const eventLoadError = eventLoadErrorId === eventId;

  useEffect(() => {
    if (event) return;
    let active = true;
    void ensureEvent(eventId).catch(() => { if (active) setEventLoadErrorId(eventId); });
    return () => { active = false; };
  }, [ensureEvent, event, eventId]);

  const [selectedSectionId, setSelectedSectionId] = useState<string | null>(null);
  const [selectedSeats, setSelectedSeats] = useState<SelectedSeat[]>([]);
  const [venueLayout, setVenueLayout] = useState<VenueLayout | null>(null);
  const [seatLoadState, setSeatLoadState] = useState<{ eventId: string; state: "loading" | "ready" | "error" }>({ eventId: "", state: "loading" });
  const [loadedEventId, setLoadedEventId] = useState<string | null>(null);
  const [seatPhaseState, setSeatPhaseState] = useState<{ eventId: string; phase: string }>({ eventId: "", phase: "CONNECTING" });
  const seatPhase = seatPhaseState.eventId === eventId ? seatPhaseState.phase : "CONNECTING";
  const isLoadingSeats = seatLoadState.eventId !== eventId || seatLoadState.state === "loading";
  const currentVenueLayout = loadedEventId === eventId ? venueLayout : null;
  const seatPhaseRef = useRef("CONNECTING");
  const [seatRetryKey, setSeatRetryKey] = useState(0);
  const guestDraftRestored = useRef(false);
  const loadedEventRef = useRef<string | null>(null);
  const [isInitiatingBooking, setIsInitiatingBooking] = useState(false);
  const [initError, setInitError] = useState<SeatsError | null>(null);
  const bookingInFlight = useRef(false);
  const latestAuthoritativeSeats = useRef<Array<{ id: string; status: string }>>([]);
  const preserveBookingAttempt = useRef(false);

  const reconcileSelectedSeats = useCallback((authoritativeSeats: Array<{ id: string; status: string }>) => {
    latestAuthoritativeSeats.current = authoritativeSeats;
    setSelectedSeats((current) => {
      const { remaining, invalidated } = invalidateUnavailableSelections(current, authoritativeSeats, bookingInFlight.current || preserveBookingAttempt.current);
      if (!invalidated.length) return current;
      setCart(remaining.length ? { eventId, seats: remaining } : null);
      setInitError({ kind: "invalidated", count: invalidated.length });
      return remaining;
    });
  }, [eventId, setCart]);

  // Continue is available only while the live stream and its authoritative snapshot agree.
  const inventoryFresh = canContinueWithSeatInventory(seatPhase) && loadedEventId === eventId;

  useEffect(() => {
    if (!event) return;
    let disposed = false;
    seatPhaseRef.current = "CONNECTING";
    if (loadedEventRef.current !== eventId) {
      latestAuthoritativeSeats.current = [];
      guestDraftRestored.current = false;
    }
    const session = createSeatRealtimeSession({
      createSource: () => new EventSource("/api/events/" + encodeURIComponent(eventId) + "/seats/stream"),
      fetchSnapshot: async () => apiClient.getSeats({ event_id: eventId }),
      onState: (state: { phase: string; seats?: ApiSeat[]; updates?: Array<{ seat_id: string; ticket_tier_id: string; status: "available" | "reserved" | "booked"; seat_revision: string; updated_at: string }>; snapshot?: boolean; invalidatedSeatIds?: string[]; reason?: string }) => {
        if (disposed) return;
        seatPhaseRef.current = state.phase;
        setSeatPhaseState({ eventId, phase: state.phase });
        if (state.phase === "RESYNC_REQUIRED") {
          setSeatLoadState({ eventId, state: "error" });
          console.warn("Seat stream requires authoritative resync", state.reason);
          return;
        }
        if (state.phase !== "LIVE" || !state.seats) return;
        setInitError((current) => current?.kind === "inventory" ? null : current);

        if (state.snapshot) {
          try {
            const nextLayout = layoutFromSnapshot(state.seats, event, eventId);
            if (nextLayout.sections.length === 0) throw new Error("No authoritative seats are available.");
            setVenueLayout(nextLayout);
            if (!guestDraftRestored.current) {
              guestDraftRestored.current = true;
              const draft = readGuestSeatDraft(eventId);
              if (draft) {
                const restored = restoreGuestSeatSelection(draft, nextLayout);
                setSelectedSeats(restored.seats);
                setInitError(restored.invalidSeatIds.length
                  ? { kind: "invalidated", count: restored.invalidSeatIds.length }
                  : null);
              } else {
                reconcileSelectedSeats(state.seats);
              }
            } else {
              reconcileSelectedSeats(state.seats);
            }
          } catch (error) {
            seatPhaseRef.current = "RESYNC_REQUIRED";
            setSeatPhaseState({ eventId, phase: "RESYNC_REQUIRED" });
            setSeatLoadState({ eventId, state: "error" });
            setInitError({ kind: "inventory" });
            console.warn("Authoritative seat snapshot could not be displayed", error);
            return;
          }
        } else if (state.updates?.length) {
          setVenueLayout((current) => current ? applyUpdatesToVenueLayout(current, state.updates!).layout : current);
          reconcileSelectedSeats(state.seats);
        }
        latestAuthoritativeSeats.current = state.seats;
        setSeatLoadState({ eventId, state: "ready" });
        setLoadedEventId(eventId);
        loadedEventRef.current = eventId;
      },
      onError: (error: unknown) => {
        console.error("Seat snapshot/realtime error:", error);
        if (!disposed) {
          seatPhaseRef.current = "RESYNC_REQUIRED";
          setSeatPhaseState({ eventId, phase: "RESYNC_REQUIRED" });
          setSeatLoadState({ eventId, state: "error" });
          setInitError((current) => current?.kind === "invalidated" ? current : { kind: "inventory" });
        }
      },
    });
    return () => {
      disposed = true;
      session.close();
    };
  }, [event, eventId, reconcileSelectedSeats, seatRetryKey]);

  useEffect(() => {
    if (!guestDraftRestored.current) return;
    let active = true;
    void Promise.resolve().then(() => {
      if (!active) return;
      const saved = saveGuestSeatDraft({
        eventId,
        seats: selectedSeats.map((seat) => ({ seatId: seat.seatId, sectionId: seat.sectionId })),
      });
      if (!saved && !user) setInitError({ kind: "message", value: "Your seat choices could not be saved for sign in. Keep this page open and try again." });
    });
    return () => { active = false; };
  }, [eventId, selectedSeats, user]);

  function retrySeatInventory() {
    setInitError(null);
    seatPhaseRef.current = "CONNECTING";
    setSeatPhaseState({ eventId, phase: "CONNECTING" });
    setSeatLoadState({ eventId, state: "loading" });
    setSeatRetryKey((current) => current + 1);
  }
  const selectedSection = currentVenueLayout?.sections.find((s) => s.id === selectedSectionId);

  const totalPrice = useMemo(
    () => selectedSeats.reduce((sum, s) => sum + s.price, 0),
    [selectedSeats]
  );

  // Build legend from unique tier + price combos
  const legend = useMemo(() => {
    if (!currentVenueLayout) return [];
    const seen = new Map<string, { tier: string; price: number; color: string }>();
    for (const section of currentVenueLayout.sections) {
      const key = `${section.tier}-${section.price}`;
      if (!seen.has(key)) {
        seen.set(key, { tier: section.tier, price: section.price, color: section.color });
      }
    }
    return Array.from(seen.values());
  }, [currentVenueLayout]);

  if (isLoading || (!event && !eventLoadError)) {
    return (
      <div className="flex items-center justify-center h-96">
        <Loader2 size={32} className="animate-spin text-primary" />
      </div>
    );
  }

  if (!event) {
    return (
      <div className="flex flex-col items-center justify-center gap-3 h-96 px-5 text-center">
        <p className="text-muted text-lg">{t("customer.seats.eventUnavailable")}</p>
        <button
          type="button"
          onClick={() => { setEventLoadErrorId(null); void ensureEvent(eventId, true).catch(() => setEventLoadErrorId(eventId)); }}
          className="mt-2 min-h-11 rounded-lg border border-border px-5 font-semibold"
        >
          {t("customer.home.tryAgain")}
        </button>
        <button
          onClick={() => router.push("/")}
          className="mt-2 bg-primary hover:bg-primary-hover text-graphite font-semibold rounded-lg px-6 h-12 transition-colors"
        >
          {t("customer.seats.backToEvents")}
        </button>
      </div>
    );
  }

  if (loadedEventId !== eventId && (isLoadingSeats || currentVenueLayout)) {
    return (
      <div className="flex items-center justify-center h-96">
        <Loader2 size={32} className="animate-spin text-primary" />
      </div>
    );
  }

  if (!currentVenueLayout) {
    return (
      <div className="flex flex-col items-center justify-center gap-4 px-5 py-16 text-center">
        <p role="alert" className="text-muted text-lg">{t("customer.seats.inventoryUnavailable")}</p>
        <button type="button" onClick={retrySeatInventory} className="min-h-11 rounded-lg bg-primary px-5 font-semibold text-graphite">
          {t("customer.seats.retryInventory")}
        </button>
      </div>
    );
  }

  // Sellability is a server projection; a tier without it is never offered for
  // sale here. The Booking/Payment guards remain authoritative.
  const hasUnsellableTier = event.tiers.some((tier) => !tier.accountingReady);
  const hasSellableTier = event.tiers.some((tier) => tier.accountingReady === true && tier.available > 0);
  const eventDate = event.startsAt
    ? date(event.startsAt, { month: "short", day: "numeric", year: "numeric" })
    : event.date;
  if (!hasSellableTier) {
    const allTiersSoldOut = event.tiers.length > 0 && event.tiers.every(
      (tier) => tier.accountingReady === true && tier.available === 0,
    );
    return (
      <div className="flex flex-col items-center justify-center gap-3 h-96 px-5 text-center">
        <p className="text-lg font-semibold">
          {allTiersSoldOut
            ? t("customer.seats.allTiersSoldOut")
            : event.tiers.length === 0
              ? t("customer.seats.noTiersAvailable")
              : t("customer.seats.noTiersOnSale")}
        </p>
        {hasUnsellableTier && (
          <p className="text-sm text-muted">
            {t("customer.seats.unsellableHelp")}
          </p>
        )}
        <button
          onClick={() => router.push(`/events/${event.id}`)}
          className="mt-2 bg-primary hover:bg-primary-hover text-graphite font-semibold rounded-lg px-6 h-12 transition-colors"
        >
          {t("customer.seats.backToEvent")}
        </button>
      </div>
    );
  }

  function handleSeatToggle(seat: Seat, row: SeatRow, section: VenueSection) {
    if (!event) return;
    preserveBookingAttempt.current = false;
    setSelectedSeats((prev) => {
      const exists = prev.find((selected) => selected.seatId === seat.id);
      if (exists) return prev.filter((selected) => selected.seatId !== seat.id);
      if (!canContinueWithSeatInventory(seatPhaseRef.current) || seat.status !== "available") return prev;

      const tier = event.tiers.find((candidate) => candidate.id === section.tierId || candidate.name === section.tier);
      return [
        ...prev,
        {
          sectionId: section.id,
          sectionName: section.name,
          rowLabel: row.label,
          seatLabel: seat.label,
          seatId: seat.id,
          price: section.price,
          tierId: section.tierId || tier?.id || "",
        },
      ];
    });
  }

  async function handleContinue() {
    if (!event) return;
    if (authLoading || bookingInFlight.current || selectedSeats.length === 0 || initError?.kind === "invalidated") return;
    if (!canContinueWithSeatInventory(seatPhaseRef.current) || loadedEventId !== eventId) {
      setInitError({ kind: "inventory" });
      return;
    }
    const returnTo = guestSeatReturnPath(eventId);
    if (!user) {
      const saved = saveGuestSeatDraft({
        eventId,
        seats: selectedSeats.map((seat) => ({ seatId: seat.seatId, sectionId: seat.sectionId })),
      });
      if (!saved) {
        setInitError({ kind: "message", value: "Your seat choices could not be saved for sign in. Keep this page open and try again." });
        return;
      }
      router.push(loginHref(returnTo, "AUTH_REQUIRED"));
      return;
    }
    bookingInFlight.current = true;
    preserveBookingAttempt.current = true;
    setIsInitiatingBooking(true);
    setInitError(null);
    let nextCart: { eventId: string; seats: SelectedSeat[] } | null = null;

    try {
      let snapshot: Awaited<ReturnType<typeof apiClient.getSeats>>;
      try {
        snapshot = await apiClient.getSeats({ event_id: eventId });
        if (!canContinueWithSeatInventory(seatPhaseRef.current)) throw new Error("Seat stream disconnected during validation.");
        const freshLayout = layoutFromSnapshot(snapshot.seats, event, eventId);
        if (freshLayout.sections.length === 0) throw new Error("No authoritative seats are available.");
        setVenueLayout(freshLayout);
        latestAuthoritativeSeats.current = snapshot.seats;
        const revalidated = restoreGuestSeatSelection({
          seats: selectedSeats.map((seat) => ({ seatId: seat.seatId, sectionId: seat.sectionId })),
        }, freshLayout);
        if (revalidated.invalidSeatIds.length > 0) {
          preserveBookingAttempt.current = false;
          setSelectedSeats(revalidated.seats);
          setInitError({ kind: "invalidated", count: revalidated.invalidSeatIds.length });
          return;
        }
        if (revalidated.seats.length === 0) {
          preserveBookingAttempt.current = false;
          setInitError({ kind: "inventory" });
          return;
        }
        setSelectedSeats(revalidated.seats);
        nextCart = { eventId, seats: revalidated.seats };
      } catch {
        preserveBookingAttempt.current = false;
        seatPhaseRef.current = "RESYNC_REQUIRED";
        setSeatPhaseState({ eventId, phase: "RESYNC_REQUIRED" });
        setSeatLoadState({ eventId, state: "error" });
        setInitError({ kind: "inventory" });
        return;
      }

      if (!canContinueWithSeatInventory(seatPhaseRef.current) || !nextCart) {
        preserveBookingAttempt.current = false;
        setInitError({ kind: "inventory" });
        return;
      }
      clearPaymentState();
      setCart(nextCart);
      await initiateBooking(nextCart);
      clearGuestSeatDraft();
      router.push("/checkout");
    } catch (error) {
      const failure = bookingFailure(error);
      if (failure.code === "AUTH_REQUIRED" || failure.code === "SESSION_EXPIRED") {
        preserveBookingAttempt.current = false;
        bookingInFlight.current = false;
        clearPaymentState();
        const saved = saveGuestSeatDraft({
          eventId,
          seats: selectedSeats.map((seat) => ({ seatId: seat.seatId, sectionId: seat.sectionId })),
        });
        if (!saved) {
          setInitError({ kind: "message", value: "Your seat choices could not be saved for sign in. Keep this page open and try again." });
          return;
        }
        setCart(null);
        router.push(loginHref(returnTo, "SESSION_EXPIRED"));
      } else if (failure.code === "BOOKING_CONFLICT") {
        preserveBookingAttempt.current = false;
        bookingInFlight.current = false;
        reconcileSelectedSeats(latestAuthoritativeSeats.current);
        setInitError({ kind: "message", value: bookingFailureMessage(failure) });
      } else {
        // The server may have accepted the existing request before its response was lost.
        setInitError({ kind: "message", value: bookingFailureMessage(failure) });
      }
    } finally {
      bookingInFlight.current = false;
      setIsInitiatingBooking(false);
    }
  }
  const steps = [
      { id: "select", labelKey: "customer.seats.stepSelect", active: true, done: false },
    { id: "checkout", labelKey: "customer.seats.stepCheckout", active: false, done: false },
    { id: "confirm", labelKey: "customer.seats.stepConfirm", active: false, done: false },
  ];

  return (
    <div className="flex flex-col min-h-screen">
      {/* Event Summary Bar */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-3 min-h-14 px-5 md:px-12 py-3 bg-card border-b border-border">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
          <span className="text-sm font-semibold">{event.title}</span>
          <span className="text-sm text-muted">
            {eventDate} &middot; {event.venue}
          </span>
        </div>
        <div className="flex items-center gap-2">
          {steps.map((step, i) => (
            <div key={step.id} className="flex items-center gap-2">
              <div className="flex items-center gap-1.5">
                <div
                  className={`w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold ${
                    step.done
                      ? "bg-primary text-graphite"
                      : step.active
                        ? "bg-primary text-graphite"
                        : "bg-tag-bg text-muted"
                  }`}
                >
                  {step.done && !step.active ? <Check size={12} /> : i + 1}
                </div>
                <span
                  className={`text-sm ${
                    step.active ? "font-semibold" : "text-muted"
                  }`}
                >
                  {t(step.labelKey as TranslationKey)}
                </span>
              </div>
              {i < steps.length - 1 && (
                <div
                  className={`w-8 h-0.5 ${
                    step.done ? "bg-primary" : "bg-border"
                  }`}
                />
              )}
            </div>
          ))}
        </div>
      </div>

      {!inventoryFresh && (
        <div
          role={seatPhase === "RESYNC_REQUIRED" ? "alert" : "status"}
          aria-live="polite"
          className="flex flex-wrap items-center justify-between gap-3 border-b border-border bg-tag-bg px-5 py-3 text-sm text-muted md:px-12"
        >
          <span>
            {seatPhase === "CONNECTING"
              ? t("customer.seats.inventoryConnecting")
              : seatPhase === "SNAPSHOT_LOADING" || seatPhase === "STREAM_READY" || seatPhase === "RECONCILING"
                ? t("customer.seats.inventoryResyncing")
                : t("customer.seats.inventoryUnavailable")}
          </span>
          <button
            type="button"
            onClick={retrySeatInventory}
            className="min-h-10 shrink-0 rounded-lg border border-border px-4 font-semibold text-foreground"
          >
            {t("customer.seats.retryInventory")}
          </button>
        </div>
      )}

      {/* Main Content */}
      <div className="flex flex-col lg:flex-row gap-8 px-5 md:px-12 py-8 flex-1">
        {/* Left: Map or Section Picker */}
        <div className="flex-1 min-w-0 flex flex-col gap-6">
          <div className={!inventoryFresh ? "pointer-events-none opacity-60" : ""} aria-disabled={!inventoryFresh}>
            {selectedSectionId && selectedSection ? (
              <SectionPicker
                section={selectedSection}
                selectedSeats={selectedSeats}
                onSeatToggle={handleSeatToggle}
                onBack={() => setSelectedSectionId(null)}
              />
            ) : (
              <>
                <VenueMap
                  layout={currentVenueLayout}
                  onSectionClick={(id) => setSelectedSectionId(id)}
                  selectedSectionId={selectedSectionId ?? undefined}
                />

                {/* Legend */}
              <div className="flex items-center gap-6 flex-wrap">
                {legend.map((item) => (
                  <div key={`${item.tier}-${item.price}`} className="flex items-center gap-2">
                    <div className="w-3 h-3 rounded-sm bg-tag-bg border border-muted" />
                    <span className="text-xs text-muted">{item.tier} &mdash; {usdCents(Math.round(item.price * 100))}</span>
                  </div>
                ))}
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 rounded-sm bg-border border border-muted" />
                  <span className="text-xs text-muted">{t("common.status.unavailable")}</span>
                </div>
                {hasUnsellableTier && (
                  <div className="flex items-center gap-2">
                    <div className="w-3 h-3 rounded-sm bg-border border border-dashed border-muted" />
                    <span className="text-xs text-muted">{t("common.status.temporarilyUnavailable")}</span>
                  </div>
                )}
                </div>
              </>
            )}
          </div>
        </div>

        {/* Right Sidebar */}
        <div className="w-full lg:w-90 shrink-0">
          <div className="bg-card border border-border rounded-lg shadow-sm flex flex-col sticky top-8">
            <div className="p-6 border-b border-border">
              <h3 className="font-bold text-lg">{t("customer.seats.yourSeats")}</h3>
            </div>

            <div className="p-6 flex flex-col gap-4">
              {selectedSeats.length === 0 ? (
                <div className="flex flex-col items-center gap-3 py-8 text-center">
                  <Armchair size={32} className="text-muted opacity-40" />
                  <p className="text-sm text-muted">{t("customer.seats.selectFromMap")}</p>
                </div>
              ) : (
                <>
                  <div className="flex flex-col gap-2 max-h-72 overflow-y-auto">
                    {selectedSeats.map((seat) => (
                      <div
                        key={seat.seatId}
                        className="flex items-center justify-between bg-background rounded-lg px-3 py-2"
                      >
                        <div className="flex flex-col">
                          <span className="text-sm font-medium">
                            {seat.sectionName}
                          </span>
                          <span className="text-xs text-muted">
                            {t("customer.seats.rowSeat", {
                              row: t("common.seat.row", { row: seat.rowLabel.replace(/^Row\s+/i, "") }),
                              seat: t("common.seat.seat", { seat: seat.seatLabel }),
                            })}
                          </span>
                        </div>
                        <div className="flex items-center gap-3">
                          <span className="text-sm font-semibold">
                            {usdCents(Math.round(seat.price * 100))}
                          </span>
                          <button
                            onClick={() =>
                              setSelectedSeats((prev) =>
                                prev.filter((s) => s.seatId !== seat.seatId)
                              )
                            }
                            className="text-muted hover:text-foreground transition-colors"
                            aria-label={t("customer.seats.removeSeatAria", { seat: t("common.seat.seat", { seat: seat.seatLabel }) })}
                          >
                            <X size={14} />
                          </button>
                        </div>
                      </div>
                    ))}
                  </div>

                  <div className="flex items-center justify-between pt-4 border-t border-border">
                    <span className="text-sm font-medium">{t("customer.seats.total")}</span>
                    <span className="text-xl font-bold">
                      {usdCents(Math.round(totalPrice * 100))}
                    </span>
                  </div>
                </>
              )}

              {initError && (
                <div role="alert" className="bg-danger-bg text-danger-ink text-sm p-3 rounded-lg">
                  {initError.kind === "invalidated"
                    ? t(initError.count === 1 ? "customer.seats.selectionInvalidatedOne" : "customer.seats.selectionInvalidatedMany", { count: number(initError.count) })
                    : initError.kind === "inventory" ? t("customer.seats.inventoryUnavailable") : text(initError.value)}
                  {initError.kind === "invalidated" && selectedSeats.length > 0 && (
                    <button type="button" onClick={() => setInitError(null)} className="mt-2 min-h-10 rounded-lg border border-current px-3 font-semibold">
                      {t("customer.seats.continueWithRemainingSeats")}
                    </button>
                  )}
                </div>
              )}

              <button
                onClick={handleContinue}
                disabled={selectedSeats.length === 0 || isInitiatingBooking || authLoading || !inventoryFresh || initError?.kind === "invalidated"}
                className="flex items-center justify-center gap-2 bg-primary hover:bg-primary-hover disabled:opacity-40 disabled:cursor-not-allowed text-graphite font-semibold rounded-lg h-12 transition-colors"
              >
                {isInitiatingBooking ? (
                  <>
                    <Loader2 size={16} className="animate-spin" />
                    {t("customer.seats.initializing")}
                  </>
                ) : (
                  <>
                    {t("customer.seats.continueCheckout")}
                    <ChevronRight size={16} />
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
