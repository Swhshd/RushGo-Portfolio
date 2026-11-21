"use client";

import { Suspense, useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";
import Link from "next/link";
import { useParams, useSearchParams } from "next/navigation";
import { AlertCircle, ArrowLeft, CalendarDays, Loader2, RefreshCw, Ticket } from "lucide-react";
import { useAuth } from "@/lib/auth-context";
import { apiClient } from "@/lib/api/client";
import { ApiError } from "@/lib/api/errors";
import type { ApiBooking, ApiEvent, ApiTicket } from "@/lib/api/types";
import { loginHref } from "@/lib/auth-navigation";
import { isAdminEventOperationsId } from "@/lib/admin-event-operations";
import type { BuyerRefundEntryAccess } from "@/lib/booking-refunds";
import { WalletRefundPanel } from "@/components/wallet-refund-panel";
import { WalletTicketCard } from "@/components/wallet-ticket-card";
import { PendingBookingActions } from "@/components/pending-booking-actions";
import {
  formatUsdCents,
  readWalletTab,
  walletHref,
  walletOrderHref,
  type WalletTab,
} from "@/lib/wallet";
import { useI18n, type TranslationKey } from "@/lib/i18n/index";

const emptySubscribe = () => () => {};

const STATUS_KEYS: Record<string, TranslationKey> = {
  PENDING: "customer.wallet.statusPending",
  CONFIRMED: "customer.wallet.statusConfirmed",
  FAILED: "customer.wallet.statusFailed",
  CANCELLED: "customer.wallet.statusCancelled",
  EXPIRED: "customer.wallet.statusExpired",
};

const TICKET_STATUS_KEYS: Record<string, TranslationKey> = {
  ISSUED: "customer.wallet.ticketStatus.issued",
  CHECKED_IN: "customer.wallet.ticketStatus.checkedIn",
  REVOKED: "customer.wallet.ticketStatus.revoked",
};

type LoadState = "loading" | "ready" | "not_found" | "forbidden" | "error";

export default function OrderDetailPage() {
  return (
    <Suspense fallback={<OrderDetailFallback />}>
      <OrderDetailPageContent />
    </Suspense>
  );
}

function OrderDetailFallback() {
  const { t } = useI18n();
  return <PageLoading label={t("customer.orderDetail.loading")} />;
}

function OrderDetailPageContent() {
  const params = useParams<{ id: string }>();
  const searchParams = useSearchParams();
  const tab = readWalletTab(searchParams.get("tab"));
  const rawId = Array.isArray(params?.id) ? params.id[0] : params?.id;
  const bookingId = isAdminEventOperationsId(rawId ?? "") ? rawId! : null;
  const hydrated = useSyncExternalStore(emptySubscribe, () => true, () => false);
  const { user, isLoading } = useAuth();
  const { t } = useI18n();
  const backHref = walletHref(tab);

  if (!hydrated || isLoading) {
    return <PageLoading label={t("customer.orderDetail.loading")} />;
  }
  if (!user) {
    const redirect = bookingId ? walletOrderHref(bookingId, tab) : backHref;
    return (
      <main className="mx-auto flex max-w-3xl flex-col items-center gap-4 px-5 py-16 text-center">
        <p className="text-lg text-muted">{t("customer.wallet.signInPrompt")}</p>
        <Link href={loginHref(redirect, "AUTH_REQUIRED")} className="inline-flex min-h-11 items-center rounded-lg bg-primary px-6 font-semibold text-graphite">
          {t("customer.wallet.signInButton")}
        </Link>
      </main>
    );
  }
  if (!bookingId) {
    return <OrderProblem title={t("customer.orderDetail.notFoundTitle")} body={t("customer.orderDetail.notFoundBody")} backHref={backHref} />;
  }
  return <OrderDetail key={`${user.id}:${bookingId}:${tab}`} bookingId={bookingId} tab={tab} backHref={backHref} />;
}

function PageLoading({ label }: { label: string }) {
  return (
    <main role="status" className="flex min-h-[50vh] items-center justify-center gap-2 text-sm text-muted">
      <Loader2 size={16} className="animate-spin" /> {label}
    </main>
  );
}

function OrderProblem({ title, body, retry, backHref = "/my-tickets" }: { title: string; body: string; retry?: () => void; backHref?: string }) {
  const { t } = useI18n();
  return (
    <main className="mx-auto flex max-w-2xl flex-col items-center gap-3 px-5 py-16 text-center">
      <AlertCircle size={24} className="text-danger-ink" />
      <h1 className="text-xl font-semibold">{title}</h1>
      <p className="text-sm text-muted">{body}</p>
      <div className="mt-2 flex flex-wrap items-center justify-center gap-2">
        <Link href={backHref} className="inline-flex min-h-11 items-center gap-2 rounded-full border border-border px-4 text-sm font-semibold">
          <ArrowLeft size={14} /> {t("customer.orderDetail.backToWallet")}
        </Link>
        {retry ? <button type="button" onClick={retry} className="inline-flex min-h-11 items-center gap-2 rounded-full bg-primary px-4 text-sm font-semibold text-graphite"><RefreshCw size={14} /> {t("customer.orderDetail.retry")}</button> : null}
      </div>
    </main>
  );
}

function OrderDetail({ bookingId, tab, backHref }: { bookingId: string; tab: WalletTab; backHref: string }) {
  const [refreshKey, setRefreshKey] = useState(0);
  return <OrderDetailData key={`${bookingId}:${refreshKey}`} bookingId={bookingId} tab={tab} backHref={backHref} onRefresh={() => setRefreshKey((key) => key + 1)} />;
}

function OrderDetailData({ bookingId, tab, backHref, onRefresh }: { bookingId: string; tab: WalletTab; backHref: string; onRefresh: () => void }) {
  const { t, locale, date, number } = useI18n();
  const [state, setState] = useState<LoadState>("loading");
  const [booking, setBooking] = useState<ApiBooking | null>(null);
  const [event, setEvent] = useState<ApiEvent | null | undefined>(undefined);
  const [tickets, setTickets] = useState<ApiTicket[] | null>(null);
  const [ticketsLoading, setTicketsLoading] = useState(true);
  const [refundAccess, setRefundAccess] = useState<BuyerRefundEntryAccess>("UNKNOWN");
  const [observedAt, setObservedAt] = useState<string | null>(null);
  const ticketSequence = useRef(0);

  const loadTickets = useCallback(async (cancelled: () => boolean) => {
    const sequence = ++ticketSequence.current;
    try {
      const result = await apiClient.listOwnedTickets();
      if (cancelled() || sequence !== ticketSequence.current) return;
      setTickets(result.tickets.filter((ticket) => ticket.booking_id === bookingId));
    } catch {
      if (!cancelled() && sequence === ticketSequence.current) setTickets(null);
    } finally {
      if (!cancelled() && sequence === ticketSequence.current) setTicketsLoading(false);
    }
  }, [bookingId]);

  // Revocation refreshes the ticket projection without remounting the refund
  // panel. A remount would forget its observed request and trigger this again.
  const refreshTickets = useCallback(() => {
    void loadTickets(() => false);
  }, [loadTickets]);

  useEffect(() => {
    let stopped = false;
    const cancelled = () => stopped;
    void (async () => {
      try {
        const loaded = await apiClient.getBooking(bookingId);
        if (stopped) return;
        setBooking(loaded);
        setState("ready");
        void apiClient.getEvent(loaded.event_id).then((value) => {
          if (!stopped) setEvent(value);
        }).catch(() => {
          if (!stopped) setEvent(null);
        });
        await loadTickets(cancelled);
        if (!stopped) setObservedAt(new Date().toISOString());
      } catch (cause) {
        if (stopped) return;
        if (cause instanceof ApiError && cause.status === 404) setState("not_found");
        else if (cause instanceof ApiError && (cause.status === 401 || cause.status === 403)) setState("forbidden");
        else setState("error");
      }
    })();
    return () => { stopped = true; ticketSequence.current += 1; };
  }, [bookingId, loadTickets]);

  const statusKey = booking ? STATUS_KEYS[booking.status.toUpperCase()] ?? "customer.wallet.statusUnknown" : "customer.wallet.statusUnknown";
  const knownTickets = tickets !== null && !ticketsLoading;
  const hasIssuedTicket = Boolean(tickets?.some((ticket) => ticket.status === "ISSUED"));
  const hasCheckedInTicket = Boolean(tickets?.some((ticket) => ticket.status === "CHECKED_IN"));
  const eventTitle = event === undefined
    ? t("customer.orderDetail.eventLoading")
    : event === null
      ? t("customer.orderDetail.eventUnavailable")
      : event.title || t("customer.orderDetail.untitledEvent");
  const progressKey: TranslationKey = booking?.status.toUpperCase() === "PENDING"
    ? "customer.orderDetail.progressPending"
    : booking?.status.toUpperCase() === "CONFIRMED"
      ? !knownTickets
        ? "customer.orderDetail.progressTicketsUnavailable"
        : tickets!.length === 0
          ? "customer.orderDetail.progressConfirmedNoTicket"
          : "customer.orderDetail.progressConfirmedTicket"
      : booking?.status.toUpperCase() === "FAILED"
        ? "customer.orderDetail.progressFailed"
        : booking?.status.toUpperCase() === "CANCELLED"
          ? "customer.orderDetail.progressCancelled"
          : booking?.status.toUpperCase() === "EXPIRED"
            ? "customer.orderDetail.progressExpired"
            : "customer.orderDetail.progressUnknown";

  if (state === "loading") return <PageLoading label={t("customer.orderDetail.loading")} />;
  if (state === "not_found") return <OrderProblem title={t("customer.orderDetail.notFoundTitle")} body={t("customer.orderDetail.notFoundBody")} backHref={backHref} />;
  if (state === "forbidden") return <OrderProblem title={t("customer.orderDetail.forbiddenTitle")} body={t("customer.orderDetail.forbiddenBody")} backHref={backHref} />;
  if (state === "error" || !booking) return <OrderProblem title={t("customer.orderDetail.errorTitle")} body={t("customer.orderDetail.errorBody")} retry={onRefresh} backHref={backHref} />;

  return (
    <main className="mx-auto flex max-w-5xl flex-col gap-5 px-5 py-8 md:px-10">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <Link href={backHref} className="inline-flex items-center gap-1.5 text-xs font-medium text-muted hover:text-foreground">
            <ArrowLeft size={13} /> {t("customer.orderDetail.backToWallet")}
          </Link>
          <h1 className="mt-2 text-2xl font-semibold tracking-tight">{t("customer.orderDetail.title")}</h1>
          <p className="mt-1 break-all font-mono text-[11px] text-muted">{booking.id}</p>
        </div>
        <button type="button" onClick={onRefresh} className="inline-flex min-h-10 items-center gap-1.5 rounded-full border border-border px-3 text-xs font-medium text-muted hover:bg-tag-bg">
          <RefreshCw size={13} /> {t("customer.orderDetail.refresh")}
        </button>
      </div>

      <section className="grid gap-3 rounded-xl border border-border bg-card p-4 sm:grid-cols-2 lg:grid-cols-4">
        <Fact label={t("customer.orderDetail.bookingStatus")} value={t(statusKey)} />
        <Fact label={t("customer.orderDetail.amount")} value={`${formatUsdCents(booking.total_amount_cents, locale)} ${(booking.currency || "USD").toUpperCase()}`} />
        <Fact label={t("customer.orderDetail.createdAt")} value={date(booking.created_at, { dateStyle: "medium", timeStyle: "short" }) || t("customer.orderDetail.timeUnavailable")} />
        <Fact label={t("customer.orderDetail.lastLoaded")} value={observedAt ? date(observedAt, { dateStyle: "medium", timeStyle: "short" }) : t("customer.orderDetail.timeUnavailable")} />
      </section>

      <section className="rounded-xl border border-border bg-card p-4">
        <div className="flex items-start gap-3">
          <CalendarDays size={18} className="mt-0.5 shrink-0 text-primary" />
          <div className="min-w-0">
            <h2 className="break-words text-base font-semibold">{eventTitle}</h2>
            {event ? <p className="mt-1 text-sm text-muted">{date(event.date, { dateStyle: "medium", timeStyle: "short" }) || t("customer.orderDetail.timeUnavailable")}</p> : null}
            {event ? <p className="mt-1 break-words text-xs text-muted">{[event.venue, event.location].filter(Boolean).join(", ")}</p> : null}
            <p className="mt-2 break-all font-mono text-[11px] text-muted">{t("customer.orderDetail.eventId")}: {booking.event_id}</p>
            {event ? <Link href={`/events/${booking.event_id}`} className="mt-2 inline-flex text-xs font-semibold text-primary underline underline-offset-4">{t("customer.orderDetail.openEvent")}</Link> : null}
          </div>
        </div>
      </section>

      <section aria-labelledby="order-progress" className="rounded-xl border border-border bg-card p-4">
        <h2 id="order-progress" className="text-sm font-semibold">{t("customer.orderDetail.progress")}</h2>
        <p className="mt-2 text-sm text-muted">{t(progressKey)}</p>
        <p className="mt-1 text-[11px] text-muted">{t("customer.orderDetail.progressBoundary")}</p>
      </section>

      {booking.status.toUpperCase() === "PENDING" && (
        <section aria-labelledby="order-payment-options" className="rounded-xl border border-border bg-card p-4">
          <h2 id="order-payment-options" className="text-sm font-semibold">{t("customer.checkout.resumeTitle")}</h2>
          <PendingBookingActions bookingId={booking.id} bookingStatus={booking.status} onBookingChanged={(updated) => setBooking(updated)} />
        </section>
      )}
      <section aria-labelledby="order-items" className="overflow-x-auto rounded-xl border border-border bg-card">
        <h2 id="order-items" className="px-4 py-3 text-sm font-semibold">{t("customer.orderDetail.items")}</h2>
        <table className="w-full min-w-[560px] border-collapse text-left text-sm">
          <thead><tr className="border-y border-border text-[11px] uppercase tracking-wider text-muted">
            <th className="px-4 py-2 font-semibold">{t("customer.orderDetail.tier")}</th>
            <th className="px-4 py-2 font-semibold">{t("customer.orderDetail.quantity")}</th>
            <th className="px-4 py-2 font-semibold">{t("customer.orderDetail.unitPrice")}</th>
            <th className="px-4 py-2 font-semibold">{t("customer.orderDetail.seats")}</th>
          </tr></thead>
          <tbody>
            {booking.items.map((item) => {
              const tierName = item.tier_name || event?.tiers.find((tier) => tier.id === item.ticket_tier_id)?.name || t("customer.orderDetail.tierUnavailable");
              return (
              <tr key={item.id} className="border-b border-border last:border-0">
                <td className="break-words px-4 py-3">{tierName}</td>
                <td className="px-4 py-3 tabular-nums">{number(item.quantity)}</td>
                <td className="px-4 py-3 font-mono tabular-nums">{formatUsdCents(item.unit_price_cents, locale)}</td>
                <td className="break-all px-4 py-3 font-mono text-[11px] text-muted">{item.seat_ids.length ? item.seat_ids.join(", ") : t("customer.orderDetail.noSeats")}</td>
              </tr>
              );
            })}
          </tbody>
        </table>
      </section>

      <section aria-labelledby="order-tickets" className="flex flex-col gap-3">
        <h2 id="order-tickets" className="text-lg font-semibold">{t("customer.orderDetail.tickets")}</h2>
        {ticketsLoading ? <p role="status" className="text-sm text-muted">{t("customer.orderDetail.ticketsLoading")}</p> : null}
        {!ticketsLoading && tickets === null ? (
          <p role="alert" className="rounded-lg border border-danger/30 bg-danger-bg px-3 py-2 text-sm text-danger-ink">{t("customer.orderDetail.ticketsUnavailable")}</p>
        ) : null}
        {knownTickets && tickets!.length === 0 ? (
          <div className="rounded-lg border border-dashed border-border bg-card/60 px-4 py-4 text-sm text-muted">
            <p className="flex items-center gap-2 font-medium"><Ticket size={14} /> {t("customer.orderDetail.noTickets")}</p>
            <p className="mt-1 text-xs">{t("customer.orderDetail.noTicketsBoundary")}</p>
          </div>
        ) : null}
        {knownTickets && tickets!.length > 0 ? (
          <div className="grid gap-4">
            {tickets!.map((ticket) => {
              const ticketStatusKey = TICKET_STATUS_KEYS[ticket.status] ?? "customer.orderDetail.ticketStatusUnknown";
              return (
                <div key={ticket.id} className="flex flex-col gap-3">
                  <div className="flex flex-wrap items-center gap-2 text-xs text-muted">
                    <span className="rounded-full bg-tag-bg px-2.5 py-1 font-semibold text-foreground">{t(ticketStatusKey)}</span>
                    <span>{t("customer.orderDetail.issuedAt")}: {date(ticket.issued_at, { dateStyle: "medium", timeStyle: "short" }) || t("customer.orderDetail.timeUnavailable")}</span>
                    {ticket.checked_in_at ? <span>{t("customer.orderDetail.checkedInAt")}: {date(ticket.checked_in_at, { dateStyle: "medium", timeStyle: "short" })}</span> : null}
                    <span className="break-all font-mono">{ticket.id}</span>
                  </div>
                  <WalletTicketCard ticket={ticket} eventTitle={eventTitle} eventDate={event?.date} refundAccess={refundAccess} tab={tab} />
                </div>
              );
            })}
          </div>
        ) : null}
      </section>

      <section id="refund" aria-labelledby="order-refund" className="scroll-mt-6 rounded-xl border border-border bg-card p-4">
        <h2 id="order-refund" className="text-lg font-semibold">{t("customer.orderDetail.refund")}</h2>
        {knownTickets ? (
          <WalletRefundPanel
            userId={booking.user_id}
            bookingId={booking.id}
            bookingStatus={booking.status}
            amountCents={booking.total_amount_cents}
            hasIssuedTicket={hasIssuedTicket}
            hasCheckedInTicket={hasCheckedInTicket}
            onRefundAccessChange={setRefundAccess}
            onTicketsChanged={refreshTickets}
          />
        ) : (
          <p role="status" className="mt-2 text-sm text-muted">{t("customer.orderDetail.refundNeedsTickets")}</p>
        )}
      </section>

      <section className="rounded-xl border border-border bg-card p-4">
        <h2 className="text-sm font-semibold">{t("customer.orderDetail.safeIdentifiers")}</h2>
        <dl className="mt-2 grid gap-2 text-xs sm:grid-cols-2">
          <div><dt className="text-muted">{t("customer.orderDetail.bookingId")}</dt><dd className="break-all font-mono">{booking.id}</dd></div>
          <div><dt className="text-muted">{t("customer.orderDetail.eventId")}</dt><dd className="break-all font-mono">{booking.event_id}</dd></div>
        </dl>
        <p className="mt-3 text-[11px] leading-relaxed text-muted">{t("customer.orderDetail.sourceBoundary")}</p>
      </section>
    </main>
  );
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <dt className="text-[10px] font-semibold uppercase tracking-wider text-muted">{label}</dt>
      <dd className="mt-1 break-words text-sm font-medium">{value}</dd>
    </div>
  );
}
