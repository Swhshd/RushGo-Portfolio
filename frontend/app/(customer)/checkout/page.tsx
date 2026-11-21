"use client";

import { useRouter, useSearchParams } from "next/navigation";
import Link from "next/link";
import { Check, Lock, Loader2, RefreshCw } from "lucide-react";
import { useBooking } from "@/lib/booking-context";
import { useAuth } from "@/lib/auth-context";
import { StripePaymentForm } from "@/components/stripe-payment-form";
import { loginHref } from "@/lib/auth-navigation";
import { Suspense, useCallback, useEffect, useRef, useState } from "react";
import { useI18n, type TranslationKey } from "@/lib/i18n/index";
import { apiClient } from "@/lib/api/client";
import { isAdminEventOperationsId } from "@/lib/admin-event-operations";
import { isConfirmedBooking } from "@/lib/api/transformers";
import { parseBookingResume, type ParsedBookingResume } from "@/lib/booking-resume";

function CheckoutFallback() {
  const { t } = useI18n();
  return <div role="status" className="flex min-h-96 items-center justify-center text-muted">{t("common.loading")}</div>;
}

export default function CheckoutPage() {
  return <Suspense fallback={<CheckoutFallback />}><CheckoutRoute /></Suspense>;
}

function CheckoutRoute() {
  const params = useSearchParams();
  const bookingId = params.get("booking_id");
  if (!bookingId) return <NewBookingCheckout />;
  if (!isAdminEventOperationsId(bookingId)) {
    return <ResumeBookingError />;
  }
  return <ResumeBookingCheckout key={bookingId} bookingId={bookingId} />;
}

function ResumeBookingError() {
  const { t } = useI18n();
  return (
    <div className="flex min-h-96 flex-col items-center justify-center gap-4 px-5 text-center">
      <p role="alert" className="text-muted">{t("customer.checkout.resumeUnavailable")}</p>
      <Link href="/my-tickets" className="text-primary underline underline-offset-4">{t("customer.checkout.viewMyTickets")}</Link>
    </div>
  );
}

function ResumeBookingCheckout({ bookingId }: { bookingId: string }) {
  const { t, date, usdCents } = useI18n();
  const router = useRouter();
  const { user, isLoading: authLoading } = useAuth();
  const [resume, setResume] = useState<ParsedBookingResume | null>(null);
  const [loading, setLoading] = useState(true);
  const [failed, setFailed] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [paymentReported, setPaymentReported] = useState(false);
  const requestSequence = useRef(0);

  const reconcilePaidBooking = useCallback(async () => {
    const request = ++requestSequence.current;
    setLoading(true);
    setFailed(false);
    try {
      for (let attempt = 0; attempt < 10; attempt++) {
        const booking = await apiClient.getBooking(bookingId);
        if (request !== requestSequence.current) return;
        if (booking.id !== bookingId) throw new Error("Booking identity mismatch.");
        if (isConfirmedBooking(booking)) {
          router.push("/my-tickets/orders/" + encodeURIComponent(bookingId));
          return;
        }
        const status = booking.status.toUpperCase();
        if (["CANCELLED", "EXPIRED", "FAILED"].includes(status)) {
          setResume((current) => current ? {
            ...current,
            booking: { ...booking, paymentIntentClientSecret: undefined },
            checkout_state: status === "EXPIRED" ? "expired" : "completed",
            can_cancel: false,
          } : current);
          setLoading(false);
          return;
        }
        if (attempt < 9) await new Promise((resolve) => setTimeout(resolve, 500));
      }
      setResume((current) => current ? {
        ...current,
        booking: current.booking ? { ...current.booking, paymentIntentClientSecret: undefined } : null,
        checkout_state: "processing",
        can_cancel: false,
      } : current);
    } catch {
      if (request === requestSequence.current) setFailed(true);
    } finally {
      if (request === requestSequence.current) setLoading(false);
    }
  }, [bookingId, router]);

  const refresh = useCallback(async () => {
    if (!user) return;
    if (paymentReported) {
      await reconcilePaidBooking();
      return;
    }
    const request = ++requestSequence.current;
    setLoading(true);
    setFailed(false);
    try {
      const response = await apiClient.resumeBooking(bookingId);
      if (request !== requestSequence.current) return;
      const checked = parseBookingResume(response, bookingId);
      setResume(checked);
    } catch {
      if (request === requestSequence.current) {
        setResume(null);
        setFailed(true);
      }
    } finally {
      if (request === requestSequence.current) setLoading(false);
    }
  }, [bookingId, paymentReported, reconcilePaidBooking, user]);

  useEffect(() => {
    if (authLoading || !user) return;
    let active = true;
    const sequence = requestSequence;
    void Promise.resolve().then(() => {
      if (active) void refresh();
    });
    return () => {
      active = false;
      sequence.current++;
    };
  }, [authLoading, refresh, user]);

  if (authLoading) {
    return <div role="status" className="flex min-h-96 items-center justify-center text-muted">{t("common.loading")}</div>;
  }
  if (!user) {
    const returnTo = "/checkout?booking_id=" + encodeURIComponent(bookingId);
    return (
      <div className="flex min-h-96 flex-col items-center justify-center gap-4 px-5 text-center">
        <p className="text-muted">{t("customer.checkout.resumeSignIn")}</p>
        <Link href={loginHref(returnTo, "AUTH_REQUIRED")} className="inline-flex min-h-11 items-center rounded-lg bg-primary px-5 font-semibold text-graphite">
          {t("customer.auth.signIn")}
        </Link>
      </div>
    );
  }

  if (loading && !resume) {
    return <div role="status" className="flex min-h-96 items-center justify-center text-muted">{t("customer.checkout.resumeChecking")}</div>;
  }

  const booking = resume?.booking ?? null;
  const stateKey: TranslationKey = resume?.checkout_state === "ready"
    ? "customer.checkout.resumeReady"
    : resume?.checkout_state === "processing"
      ? "customer.checkout.resumeProcessing"
      : resume?.checkout_state === "expired"
        ? "customer.checkout.resumeExpired"
        : resume?.checkout_state === "completed"
          ? booking?.status.toUpperCase() === "CANCELLED"
            ? "customer.checkout.resumeCancelled"
            : "customer.checkout.resumeCompleted"
          : "customer.checkout.resumeUnavailable";
  const canPay = !paymentReported && resume?.checkout_state === "ready" && typeof booking?.paymentIntentClientSecret === "string";

  async function cancelOriginalBooking() {
    if (!resume?.can_cancel || cancelling || paymentReported) return;
    setCancelling(true);
    setFailed(false);
    try {
      const cancelledBooking = await apiClient.cancelBooking(bookingId);
      setResume((current) => current ? {
        ...current,
        booking: { ...cancelledBooking, paymentIntentClientSecret: undefined },
        checkout_state: "completed",
        can_cancel: false,
      } : current);
    } catch {
      setFailed(true);
      await refresh();
    } finally {
      setCancelling(false);
    }
  }

  async function handlePaymentSuccess() {
    setPaymentReported(true);
    setResume((current) => current ? {
      ...current,
      booking: current.booking ? { ...current.booking, paymentIntentClientSecret: undefined } : null,
      checkout_state: "processing",
      can_cancel: false,
    } : current);
    await reconcilePaidBooking();
  }

  return (
    <main className="mx-auto flex max-w-4xl flex-col gap-5 px-5 py-8 md:px-10">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h1 className="text-2xl font-semibold">{t("customer.checkout.resumeTitle")}</h1>
          <p className="mt-2 break-all font-mono text-xs text-muted">{t("customer.checkout.resumeOrderId")}: {bookingId}</p>
        </div>
      </div>

      <section className="rounded-xl border border-border bg-card p-5">
        {loading && <p role="status" className="mb-3 text-sm text-muted">{t("customer.checkout.resumeChecking")}</p>}
        {failed && <p role="alert" className="mb-3 text-sm text-danger-ink">{t("customer.checkout.resumeUnavailable")}</p>}
        {booking ? (
          <div className="mb-5 grid gap-2 text-sm sm:grid-cols-2">
            <p><span className="text-muted">{t("customer.checkout.orderSummary")}: </span>{booking.id}</p>
            <p><span className="text-muted">{t("customer.checkout.total")}: </span>{usdCents(booking.total_amount_cents)} {(booking.currency || "USD").toUpperCase()}</p>
          </div>
        ) : null}
        <p role={resume?.checkout_state === "unavailable" || failed ? "alert" : "status"} className="text-sm text-muted">{t(stateKey)}</p>
        {resume?.reservation_deadline ? (
          <p className="mt-2 text-xs text-muted">{t("customer.checkout.resumeReservationDeadline", {
            date: date(resume.reservation_deadline, { dateStyle: "medium", timeStyle: "short" }),
          })}</p>
        ) : null}

        {canPay && booking ? (
          <div className="mt-5 flex flex-col gap-4">
            <div className="flex items-center gap-2 text-sm text-muted">
              <Lock size={14} /> <span>{t("customer.checkout.secureStripe")}</span>
            </div>
            <StripePaymentForm
              clientSecret={booking.paymentIntentClientSecret!}
              amount={booking.total_amount_cents / 100}
              onSuccess={handlePaymentSuccess}
              onError={() => {}}
              disabled={loading || paymentReported}
            />
          </div>
        ) : null}

        <div className="mt-5 flex flex-wrap gap-2">
          {canPay && booking ? null : (
            <button type="button" onClick={refresh} disabled={loading || cancelling} className="inline-flex min-h-11 items-center gap-2 rounded-lg border border-border px-4 text-sm font-semibold disabled:opacity-60">
              <RefreshCw size={14} /> {t("customer.checkout.resumeRetry")}
            </button>
          )}
          {resume?.can_cancel && !paymentReported ? (
            <button type="button" onClick={cancelOriginalBooking} disabled={cancelling || loading} className="inline-flex min-h-11 items-center gap-2 rounded-lg border border-danger/40 px-4 text-sm font-semibold text-danger-ink disabled:opacity-60">
              {cancelling ? <Loader2 size={14} className="animate-spin" /> : null}
              {t("customer.checkout.resumeCancel")}
            </button>
          ) : null}
          <Link href={"/my-tickets/orders/" + encodeURIComponent(bookingId)} className="inline-flex min-h-11 items-center rounded-lg border border-border px-4 text-sm font-semibold">
            {t("customer.checkout.viewMyTickets")}
          </Link>
        </div>
      </section>
    </main>
  );
}
function NewBookingCheckout() {
  const { t, text, date, usdCents } = useI18n();
  const router = useRouter();
  const {
    cart,
    getEvent,
    confirmPayment,
    paymentClientSecret,
    pendingBookingTotalCents,
    refreshEvents,
    error,
    creationState,
    initiateBooking,
    setCart,
    isLoading,
  } = useBooking();
  const { user, isLoading: authLoading } = useAuth();
  const bookingRequestInFlight = useRef(false);
  const [isBookingRequestInFlight, setIsBookingRequestInFlight] = useState(false);

  const initiateBookingOnce = useCallback(() => {
    if (bookingRequestInFlight.current) return;
    bookingRequestInFlight.current = true;
    setIsBookingRequestInFlight(true);
    void initiateBooking().catch(() => {}).finally(() => {
      bookingRequestInFlight.current = false;
      setIsBookingRequestInFlight(false);
    });
  }, [initiateBooking]);

  useEffect(() => {
    if (!authLoading && !isLoading && user && cart && creationState === null && !paymentClientSecret) {
      initiateBookingOnce();
    }
  }, [authLoading, isLoading, user, cart, creationState, paymentClientSecret, initiateBookingOnce]);

  // An inventory refresh must not remount a payment form already in use.
  if (authLoading || (isLoading && !paymentClientSecret)) {
    return (
      <div className="flex items-center justify-center h-96 px-5 text-center">
        <p role="status" className="text-muted text-lg">{t("common.loading")}</p>
      </div>
    );
  }

  if (!cart) {
    return (
      <div className="flex items-center justify-center h-96 px-5 text-center">
        <div className="flex flex-col items-center gap-4">
          <p className="text-muted text-lg">{t("customer.checkout.emptyCart")}</p>
          <Link href="/" className="text-primary underline underline-offset-4">{t("customer.home.discoverAll")}</Link>
          <Link href="/my-tickets" className="text-primary underline underline-offset-4">{t("customer.checkout.viewMyTickets")}</Link>
        </div>
      </div>
    );
  }

  if (!user) {
    return (
      <div className="flex items-center justify-center h-96 px-5 text-center">
        <div className="flex flex-col items-center gap-4">
          <p className="text-muted text-lg">{t("customer.checkout.signInPrompt")}</p>
          <Link
            href={loginHref(`/events/${encodeURIComponent(cart.eventId)}/seats`, "AUTH_REQUIRED")}
            className="bg-primary hover:bg-primary-hover text-graphite font-semibold rounded-lg px-6 h-12 flex items-center transition-colors"
          >
            {t("customer.auth.signIn")}
          </Link>
        </div>
      </div>
    );
  }

  const event = getEvent(cart.eventId);
  if (!event) {
    return (
      <div className="flex items-center justify-center min-h-96 px-5 text-center">
        <div className="flex flex-col items-center gap-4">
          <p role="status" className="text-muted text-lg">{t("customer.event.notFound")}</p>
          <button type="button" onClick={() => void refreshEvents(cart.eventId)} className="min-h-11 rounded-lg border border-border px-5 font-semibold">{t("customer.home.tryAgain")}</button>
          <Link href="/my-tickets" className="text-primary underline underline-offset-4">{t("customer.checkout.viewMyTickets")}</Link>
          <Link href="/" className="text-primary underline underline-offset-4">{t("customer.home.discoverAll")}</Link>
        </div>
      </div>
    );
  }

  const total = pendingBookingTotalCents === null ? null : pendingBookingTotalCents / 100;
  const isConfirmed = creationState === "confirmed";
  const eventDate = event.startsAt
    ? date(event.startsAt, { month: "short", day: "numeric", year: "numeric" })
    : event.date;

  function handlePaymentSuccess(paymentIntentId: string) {
    confirmPayment(paymentIntentId).then((success) => {
      if (success) {
        router.push("/my-tickets");
      }
    });
  }

  // The payment form displays its own safe error text.
  function handlePaymentError() {}

  const steps = [
    { id: "select", labelKey: "customer.seats.stepSelect", done: true },
    { id: "checkout", labelKey: "customer.seats.stepCheckout", active: true },
    { id: "confirm", labelKey: "customer.seats.stepConfirm", done: false },
  ];

  // Payment not initialized - redirect back to seats
  if (!paymentClientSecret || total === null) {
    return (
      <div className="flex items-center justify-center h-96 px-5 text-center">
        <div className="flex w-full max-w-xl min-w-0 flex-col items-center gap-4">
          <p role="status" className="w-full min-w-0 text-muted text-lg [overflow-wrap:anywhere]">{error
            ? text(error)
            : isConfirmed
              ? t("customer.checkout.alreadyConfirmed")
              : creationState === "terminal"
                ? t("customer.checkout.bookingEnded")
                : t("customer.checkout.preparingBooking")}</p>
          {!isConfirmed && creationState !== "terminal" && (
            <>
              <p className="text-sm text-muted">{t("customer.checkout.recoveryGuidance")}</p>
              <p className="text-sm text-muted">{t("customer.checkout.returnToSeatsNotice")}</p>
            </>
          )}
          {isConfirmed ? (
            <Link
              href="/my-tickets"
              className="bg-primary hover:bg-primary-hover text-graphite font-semibold rounded-lg px-6 h-12 flex items-center transition-colors"
            >
              {t("customer.checkout.viewMyTickets")}
            </Link>
          ) : (
            <>
              {creationState !== "terminal" && <button
                type="button"
                onClick={initiateBookingOnce}
                disabled={isBookingRequestInFlight}
                aria-busy={isBookingRequestInFlight}
                className="bg-primary hover:bg-primary-hover disabled:opacity-60 disabled:cursor-wait text-graphite font-semibold rounded-lg px-6 h-12 transition-colors"
              >{isBookingRequestInFlight ? t("common.loading") : t("customer.checkout.retryBooking")}</button>}
              <Link
                href={`/events/${cart.eventId}/seats`}
                onClick={() => { if (creationState === "terminal") setCart(null); }}
                className="bg-primary hover:bg-primary-hover text-graphite font-semibold rounded-lg px-6 h-12 flex items-center transition-colors"
              >
                {t("customer.checkout.backToSeatSelection")}
              </Link>
              <Link href="/my-tickets" className="text-primary underline underline-offset-4">
                {t("customer.checkout.viewMyTickets")}
              </Link>
            </>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="flex min-w-0 flex-col">
      {/* Step Bar */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-3 min-h-14 px-5 md:px-12 py-3 bg-card border-b border-border">
        <div className="flex min-w-0 flex-wrap items-center gap-x-4 gap-y-1">
          <span className="min-w-0 text-sm font-semibold [overflow-wrap:anywhere]">{event.title}</span>
          <span className="min-w-0 text-sm text-muted [overflow-wrap:anywhere]">
            {eventDate} · {event.venue}
          </span>
        </div>
        <div className="flex w-full min-w-0 max-w-full flex-wrap items-center gap-2 md:w-auto md:flex-nowrap">
          {steps.map((step, i) => (
            <div key={step.id} className="flex min-w-0 items-center gap-2">
              <div className="flex items-center gap-1.5">
                <div
                  className={`w-6 h-6 shrink-0 rounded-full flex items-center justify-center text-xs font-bold ${
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
                  className={`whitespace-nowrap text-sm ${
                    step.active ? "font-semibold" : "text-muted"
                  }`}
                >
                  {t(step.labelKey as TranslationKey)}
                </span>
              </div>
              {i < steps.length - 1 && (
                <div
                  className={`w-8 h-0.5 shrink-0 ${
                    step.done ? "bg-primary" : "bg-border"
                  }`}
                />
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Content */}
      <div className="flex min-w-0 flex-col lg:flex-row gap-8 px-5 md:px-12 py-8">
        {/* Booking Info & Payment */}
        <div className="flex-1 min-w-0">
          <div className="bg-card border border-border rounded-lg p-6 md:p-8 shadow-sm flex flex-col gap-6">
            <h2 className="text-lg font-bold">{t("customer.checkout.paymentDetails")}</h2>

            {/* User Info */}
            <div className="flex flex-col gap-4 pb-6 border-b border-border">
              <div className="flex flex-col gap-1.5">
                <span className="text-xs font-medium text-muted">{t("customer.checkout.name")}</span>
                <span className="min-w-0 text-sm font-medium [overflow-wrap:anywhere]">{user.name}</span>
              </div>
              <div className="flex flex-col gap-1.5">
                <span className="text-xs font-medium text-muted">{t("customer.checkout.email")}</span>
                <span className="min-w-0 text-sm font-medium [overflow-wrap:anywhere]">{user.email}</span>
              </div>
              <div className="flex flex-col gap-1.5">
                <span className="text-xs font-medium text-muted">{t("customer.checkout.selectedSeats")}</span>
                <div className="flex flex-col gap-1">
                  {cart.seats.map((seat) => (
                    <span key={seat.seatId} className="min-w-0 text-sm [overflow-wrap:anywhere]">
                      {t("customer.checkout.seatDisplay", {
                        section: seat.sectionName,
                        row: t("common.seat.row", { row: seat.rowLabel.replace(/^Row\s+/i, "") }),
                        seat: t("common.seat.seat", { seat: seat.seatLabel }),
                      })}
                    </span>
                  ))}
                </div>
              </div>
            </div>

            {/* Payment Form */}
            <div className="flex min-w-0 flex-col gap-4">
              <div className="flex items-center gap-2 text-sm text-muted">
                <Lock size={14} />
                <span>{t("customer.checkout.secureStripe")}</span>
              </div>
              <StripePaymentForm
                clientSecret={paymentClientSecret}
                amount={total}
                onSuccess={handlePaymentSuccess}
                onError={handlePaymentError}
              />
              {error && <p role="status" className="min-w-0 text-sm text-muted [overflow-wrap:anywhere]">{text(error)}</p>}
              <Link href="/my-tickets" className="self-start text-sm text-primary underline underline-offset-4">
                {t("customer.checkout.viewMyTickets")}
              </Link>
            </div>
          </div>
        </div>

        {/* Order Summary */}
        <div className="w-full min-w-0 lg:w-100 shrink-0">
          <div className="bg-card border border-border rounded-lg shadow-sm flex flex-col">
            <div className="flex min-w-0 items-center gap-4 p-6 border-b border-border">
              <div className="w-16 h-16 rounded-lg overflow-hidden shrink-0">
                <img src={event.image} alt={event.title} className="w-full h-full object-cover" />
              </div>
              <div className="flex min-w-0 flex-1 flex-col gap-0.5">
                <h3 className="min-w-0 font-semibold text-sm [overflow-wrap:anywhere]">{event.title}</h3>
                <p className="min-w-0 text-xs text-muted [overflow-wrap:anywhere]">
                  {eventDate} · {event.venue}, {event.location}
                </p>
              </div>
            </div>
            <div className="flex flex-col gap-4 p-6">
              <h3 className="text-lg font-bold">{t("customer.checkout.orderSummary")}</h3>
              {cart.seats.map((seat) => (
                <div key={seat.seatId} className="flex min-w-0 items-center justify-between text-sm">
                  <span className="min-w-0 flex-1 text-muted [overflow-wrap:anywhere]">
                    {t("customer.checkout.seatDisplay", {
                      section: seat.sectionName,
                      row: t("common.seat.row", { row: seat.rowLabel.replace(/^Row\s+/i, "") }),
                      seat: t("common.seat.seat", { seat: seat.seatLabel }),
                    })}
                  </span>
                </div>
              ))}
              <div className="flex min-w-0 flex-wrap items-center justify-between gap-x-3 gap-y-1 pt-4 border-t border-border">
                <span className="min-w-0 font-medium">{t("customer.checkout.total")}</span>
                <span className="min-w-0 text-right text-2xl font-bold [overflow-wrap:anywhere]">{usdCents(Math.round(total * 100))}</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
