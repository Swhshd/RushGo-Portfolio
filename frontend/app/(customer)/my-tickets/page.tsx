"use client";

import {
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import {
  AlertCircle,
  Calendar,
  CheckCircle,
  Loader2,
  LogIn,
  RefreshCw,
} from "lucide-react";
import { useAuth } from "@/lib/auth-context";
import { useBooking } from "@/lib/booking-context";
import { apiClient } from "@/lib/api/client";
import { ApiError } from "@/lib/api/errors";
import type { ApiBooking, ApiEvent, ApiTicket } from "@/lib/api/types";
import { buyerRefundEntryAccess, currentBuyerRefundEntryAccess, type BuyerRefundAccessEntry, type BuyerRefundEntryAccess } from "@/lib/booking-refunds";
import { loginHref } from "@/lib/auth-navigation";
import { WalletOrderCard } from "@/components/wallet-order-card";
import { WalletTicketCard } from "@/components/wallet-ticket-card";
import {
  buildWalletOrder,
  filterWalletOrders,
  hasMoreBookings,
  mergeBookings,
  readWalletTab,
  walletHref,
  type WalletTab,
} from "@/lib/wallet";
import { useI18n } from "@/lib/i18n/index";

const PAGE_SIZE = 20;

const emptySubscribe = () => () => {};

function SessionLoading() {
  const { t } = useI18n();
  return (
    <div className="flex h-96 flex-col items-center justify-center gap-3">
      <Loader2 size={24} className="animate-spin text-primary" />
      <p className="text-muted text-sm">{t("customer.wallet.sessionChecking")}</p>
    </div>
  );
}

function errorMessage(err: unknown): string {
  const status =
    typeof err === "object" && err !== null && "status" in err
      ? (err as { status?: number }).status
      : undefined;
  if (status === 401) {
    return "Your session has expired. Please sign in again.";
  }
  if (status === 403) {
    return "You do not have access to these bookings.";
  }
  if (err instanceof Error && err.message) return err.message;
  return "Your bookings could not be loaded. Please try again.";
}

export default function MyTicketsPage() {
  return (
    <Suspense fallback={<SessionLoading />}>
      <MyTicketsPageContent />
    </Suspense>
  );
}

function MyTicketsPageContent() {
  const { t } = useI18n();
  const searchParams = useSearchParams();
  const tab = readWalletTab(searchParams.get("tab"));
  // localStorage/session restoration differs between the server snapshot and
  // the browser. Render a neutral shell until hydration has completed so the
  // server and first client render agree.
  const hydrated = useSyncExternalStore(
    emptySubscribe,
    () => true,
    () => false,
  );
  const { user, isLoading: authLoading } = useAuth();
  const { lastPurchaseSuccess, lastPurchaseSuccessUserId, clearSuccessFlag } = useBooking();

  useEffect(() => {
    if (
      hydrated &&
      !authLoading &&
      lastPurchaseSuccess &&
      (!user || lastPurchaseSuccessUserId !== user.id)
    ) {
      clearSuccessFlag();
    }
  }, [authLoading, clearSuccessFlag, hydrated, lastPurchaseSuccess, lastPurchaseSuccessUserId, user]);

  if (!hydrated || authLoading) {
    return <SessionLoading />;
  }

  if (!user) {
    return (
      <div className="flex h-96 flex-col items-center justify-center gap-4 px-6 text-center">
        <LogIn size={24} className="text-muted" />
        <p className="text-muted text-lg">{t("customer.wallet.signInPrompt")}</p>
        <Link
          href={loginHref(walletHref(tab), "AUTH_REQUIRED")}
          className="h-12 rounded-lg bg-primary px-6 font-semibold leading-[48px] text-graphite transition-colors hover:bg-primary-hover"
        >
          {t("customer.wallet.signInButton")}
        </Link>
      </div>
    );
  }

  return (
    <MyTicketsForUser
      key={user.id}
      userId={user.id}
      tab={tab}
      showSuccess={lastPurchaseSuccess && lastPurchaseSuccessUserId === user.id}
      clearSuccessFlag={clearSuccessFlag}
    />
  );
}

function MyTicketsForUser({
  userId,
  tab,
  showSuccess,
  clearSuccessFlag,
}: {
  userId: string;
  tab: WalletTab;
  showSuccess: boolean;
  clearSuccessFlag: () => void;
}) {
  const { t, text, number } = useI18n();
  const router = useRouter();
  const [bookings, setBookings] = useState<ApiBooking[]>([]);
  const [tickets, setTickets] = useState<ApiTicket[]>([]);
  const [eventsById, setEventsById] = useState<
    Record<string, ApiEvent | null>
  >({});
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [ticketsLoading, setTicketsLoading] = useState(true);
  const [ticketsError, setTicketsError] = useState<string | null>(null);
  const [refundAccessByBookingId, setRefundAccessByBookingId] = useState<Record<string, BuyerRefundAccessEntry>>({});
  const [error, setError] = useState<string | null>(null);
  const [loadMoreError, setLoadMoreError] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [reachedEnd, setReachedEnd] = useState(false);
  const [purchaseNoticeVisible, setPurchaseNoticeVisible] = useState(showSuccess);

  const handleRefundAccessChange = useCallback((bookingId: string, access: BuyerRefundEntryAccess) => {
    setRefundAccessByBookingId((current) => {
      const existing = current[bookingId];
      if (existing?.access === access && existing.fallbackCycle === null) return current;
      return { ...current, [bookingId]: { access, fallbackCycle: null } };
    });
  }, []);

  const listSequence = useRef(0);
  const ticketSequence = useRef(0);
  const sessionSequence = useRef(0);
  const knownEventIds = useRef<Set<string>>(new Set());
  const bookingIds = useRef<Set<string>>(new Set());

  if (showSuccess && !purchaseNoticeVisible) setPurchaseNoticeVisible(true);

  useEffect(() => {
    if (showSuccess) clearSuccessFlag();
  }, [showSuccess, clearSuccessFlag]);

  const loadEventDetails = useCallback(
    async (eventIds: string[], session: number) => {
      const targets = Array.from(new Set(eventIds.filter(Boolean))).filter(
        (id) => !knownEventIds.current.has(id),
      );
      if (targets.length === 0) return;
      targets.forEach((id) => knownEventIds.current.add(id));

      const settled = await Promise.allSettled(
        targets.map(async (id) => ({
          id,
          event: await apiClient.getEvent(id),
        })),
      );
      if (session !== sessionSequence.current) return;

      // A network blip must not be cached as a permanently deleted event;
      // allow a later refresh to try the failed ids again.
      targets.forEach((id, index) => {
        if (settled[index]?.status !== "fulfilled") {
          knownEventIds.current.delete(id);
        }
      });

      setEventsById((current) => {
        const next = { ...current };
        targets.forEach((id, index) => {
          const result = settled[index];
          next[id] =
            result?.status === "fulfilled" ? result.value.event : null;
        });
        return next;
      });
    },
    [],
  );

  const loadBookings = useCallback(
    async (
      targetPage: number,
      mode: "replace" | "append",
      session: number,
    ) => {
      const sequence = ++listSequence.current;
      if (mode === "replace") {
        setLoading(true);
        setError(null);
      } else {
        setLoadingMore(true);
        setLoadMoreError(null);
      }

      try {
        const result = await apiClient.listBookings(targetPage, PAGE_SIZE);
        if (
          sequence !== listSequence.current ||
          session !== sessionSequence.current
        ) {
          return;
        }
        const incoming = result.bookings ?? [];
        if (mode === "replace") {
          bookingIds.current = new Set(incoming.map((booking) => booking.id));
          setReachedEnd(false);
        } else {
          const newRows = incoming.filter(
            (booking) => !bookingIds.current.has(booking.id),
          ).length;
          incoming.forEach((booking) => bookingIds.current.add(booking.id));
          if (newRows === 0) {
            // Offset pagination can return only duplicates or an empty page;
            // stop offering Load more instead of looping forever.
            setReachedEnd(true);
          }
        }
        setBookings((current) =>
          mode === "replace" ? incoming : mergeBookings(current, incoming),
        );
        setTotal(result.total ?? 0);
        setPage(targetPage);
        void loadEventDetails(
          incoming.map((booking) => booking.event_id),
          session,
        );
      } catch (err) {
        if (
          sequence !== listSequence.current ||
          session !== sessionSequence.current
        ) {
          return;
        }
        if (mode === "replace") {
          // A failed refresh must not leave a stale wallet looking current.
          setBookings([]);
          setTotal(0);
          setError(errorMessage(err));
        } else {
          setLoadMoreError(errorMessage(err));
        }
      } finally {
        if (
          sequence === listSequence.current &&
          session === sessionSequence.current
        ) {
          if (mode === "replace") setLoading(false);
          else setLoadingMore(false);
        }
      }
    },
    [loadEventDetails],
  );

  const loadTickets = useCallback(
    async (session: number) => {
      const sequence = ++ticketSequence.current;
      setTicketsLoading(true);
      setTicketsError(null);
      try {
        const result = await apiClient.listOwnedTickets();
        if (session !== sessionSequence.current || sequence !== ticketSequence.current) return;
        const incoming = Array.isArray(result.tickets) ? result.tickets : [];
        setTickets(incoming);
        void loadEventDetails(incoming.map((ticket) => ticket.event_id), session);
      } catch {
        if (session !== sessionSequence.current || sequence !== ticketSequence.current) return;
        setTickets([]);
        setTicketsError("Issued tickets could not be loaded. Refresh to try again.");
      } finally {
        if (session === sessionSequence.current && sequence === ticketSequence.current) setTicketsLoading(false);
      }
    },
    [loadEventDetails],
  );

  const refreshTicketsAfterRefund = useCallback(() => {
    void loadTickets(sessionSequence.current);
  }, [loadTickets]);

  function refreshWallet() {
    sessionSequence.current += 1;
    listSequence.current += 1;
    knownEventIds.current = new Set();
    bookingIds.current = new Set();
    setBookings([]);
    setTickets([]);
    setTicketsError(null);
    setRefundAccessByBookingId({});
    setEventsById({});
    setTotal(0);
    setPage(1);
    setError(null);
    setLoadMoreError(null);
    setExpandedId(null);
    setReachedEnd(false);
    setLoading(true);
    setLoadingMore(false);
    setTicketsLoading(true);
    setRefreshKey((key) => key + 1);
  }

  useEffect(() => {
    sessionSequence.current += 1;
    listSequence.current += 1;
    const session = sessionSequence.current;
    knownEventIds.current = new Set();
    bookingIds.current = new Set();
    void loadBookings(1, "replace", session);
    void loadTickets(session);
    return () => {
      // Ignore responses that belong to a previous user/session.
      sessionSequence.current += 1;
    };
  }, [refreshKey, loadBookings, loadTickets]);

  const orders = useMemo(
    () =>
      bookings.map((booking) =>
        buildWalletOrder(
          booking,
          booking.event_id ? eventsById[booking.event_id] : null,
        ),
      ),
    [bookings, eventsById],
  );

  const visibleOrders = useMemo(
    () => filterWalletOrders(orders, tab),
    [orders, tab],
  );

  const refundPanelBookingIds = useMemo(
    () => new Set(visibleOrders.filter((order) => order.isConfirmedBooking).map((order) => order.id)),
    [visibleOrders],
  );
  const fallbackRefundBookingIds = useMemo(
    () => Array.from(new Set(tickets
      .filter((ticket) => ticket.status === "ISSUED" && ticket.booking_status?.toUpperCase() === "CONFIRMED")
      .map((ticket) => ticket.booking_id)
      .filter((bookingId) => !refundPanelBookingIds.has(bookingId)))),
    [refundPanelBookingIds, tickets],
  );
  const fallbackRefundBookingIdSet = useMemo(
    () => new Set(fallbackRefundBookingIds),
    [fallbackRefundBookingIds],
  );

  useEffect(() => {
    if (fallbackRefundBookingIds.length === 0) return;
    let cancelled = false;

    void (async () => {
      for (let index = 0; index < fallbackRefundBookingIds.length; index += 8) {
        const batch = fallbackRefundBookingIds.slice(index, index + 8);
        const results = await Promise.all(batch.map(async (bookingId) => {
          try {
            const refund = await apiClient.getBookingRefund(bookingId);
            return [bookingId, buyerRefundEntryAccess(true, refund)] as const;
          } catch (cause) {
            return [bookingId, cause instanceof ApiError && cause.status === 404 ? "ALLOWED" : "UNKNOWN"] as const;
          }
        }));
        if (cancelled) return;
        setRefundAccessByBookingId((current) => {
          const next = { ...current };
          results.forEach(([bookingId, access]) => {
            next[bookingId] = { access, fallbackCycle: fallbackRefundBookingIds };
          });
          return next;
        });
      }
    })();

    return () => { cancelled = true; };
  }, [fallbackRefundBookingIds]);

  function loadMore() {
    if (!userId || loading || loadingMore) return;
    void loadBookings(page + 1, "append", sessionSequence.current);
  }

  const hasMore = hasMoreBookings(bookings.length, total) && !reachedEnd;
  const eventsPending = orders.some((order) => order.event === undefined);
  const ordersAnchorReached = useRef(false);

  // Next may resolve the anchor before tickets/event data have expanded the
  // page. Honour the explicit orders link once that initial layout is ready.
  useEffect(() => {
    if (ordersAnchorReached.current || loading || ticketsLoading || eventsPending ||
        window.location.hash !== "#orders") return;
    const frame = window.requestAnimationFrame(() => {
      document.getElementById("orders")?.scrollIntoView({ block: "start" });
      ordersAnchorReached.current = true;
    });
    return () => window.cancelAnimationFrame(frame);
  }, [loading, ticketsLoading, eventsPending]);

  return (
    <div className="flex flex-col gap-6 px-5 py-8 md:px-12">
      {purchaseNoticeVisible && !error && (
        <div
          role="status"
          className="flex items-start gap-3 rounded-lg bg-success-bg p-4"
        >
          <CheckCircle size={20} className="mt-0.5 shrink-0 text-success-ink" />
          <div className="flex flex-col gap-1">
            <span className="font-medium text-success-ink">
              {t("customer.wallet.bookingConfirmed")}
            </span>
            <span className="text-sm text-success-ink">
              {t("customer.wallet.bookingConfirmedNote")}
            </span>
          </div>
        </div>
      )}

      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold">{t("customer.wallet.heading")}</h1>
          <p className="mt-1 text-xs text-muted">
            {t("customer.wallet.authorityNote")}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <div
            role="group"
            aria-label={t("customer.wallet.filterByDate")}
            className="flex items-center gap-1 rounded-lg bg-tag-bg p-1"
          >
            {(
              [
                ["all", t("customer.wallet.all")],
                ["upcoming", t("customer.wallet.upcoming")],
                ["past", t("customer.wallet.past")],
              ] as const
            ).map(([value, label]) => (
              <button
                key={value}
                type="button"
                onClick={() => router.replace(walletHref(value), { scroll: false })}
                aria-pressed={tab === value}
                className={`rounded-sm px-4 py-1.5 text-sm font-medium transition-colors ${
                  tab === value
                    ? "bg-card text-foreground font-semibold shadow-sm"
                    : "text-muted hover:text-foreground"
                }`}
              >
                {label}
              </button>
            ))}
          </div>
          <button
            type="button"
            onClick={refreshWallet}
            disabled={loading || loadingMore}
            className="inline-flex h-[34px] items-center gap-1.5 rounded-full border border-border px-3 text-xs font-medium text-muted transition-colors hover:bg-tag-bg hover:text-foreground disabled:opacity-50"
          >
            <RefreshCw
              size={13}
              className={loading || loadingMore ? "animate-spin" : ""}
            />
            {t("common.refresh")}
          </button>
        </div>
      </div>

      <section aria-labelledby="issued-tickets-heading" className="flex flex-col gap-4">
        <div className="flex flex-wrap items-end justify-between gap-3 border-b border-border pb-3">
          <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.2em] text-primary">{t("customer.wallet.admissionWallet")}</p>
            <h2 id="issued-tickets-heading" className="mt-1 text-xl font-semibold">{t("customer.wallet.issuedTickets")}</h2>
          </div>
          <p className="max-w-md text-xs leading-relaxed text-muted">{t("customer.wallet.entryCodeEligibility")}</p>
        </div>

        {ticketsLoading && tickets.length === 0 ? (
          <div role="status" className="rounded-xl border border-border bg-card px-5 py-8 text-sm text-muted">{t("customer.wallet.checkingTickets")}</div>
        ) : null}
        {ticketsError ? (
          <div role="alert" className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-danger/30 bg-danger-bg px-4 py-3 text-[13px] text-danger-ink">
            <span>{text(ticketsError)}</span>
            <button type="button" onClick={refreshWallet} className="rounded-full border border-danger/30 px-3 py-1.5 text-xs font-semibold">{t("customer.wallet.retry")}</button>
          </div>
        ) : null}
        {!ticketsLoading && !ticketsError && tickets.length === 0 ? (
          <div className="rounded-xl border border-dashed border-border bg-card/60 px-5 py-6 text-sm text-muted">
            {t("customer.wallet.ticketsEmpty")}
          </div>
        ) : null}
        {tickets.length > 0 ? (
          <div className="grid gap-4">
            {tickets.map((ticket) => {
              const event = eventsById[ticket.event_id];
              const refundAccess = currentBuyerRefundEntryAccess(
                refundAccessByBookingId[ticket.booking_id],
                fallbackRefundBookingIdSet.has(ticket.booking_id),
                fallbackRefundBookingIds,
              );
              return <WalletTicketCard key={ticket.id} ticket={ticket} eventTitle={event?.title ?? t("customer.wallet.eventIdentifier", { id: ticket.event_id.slice(0, 8) })} eventDate={event?.date} refundAccess={refundAccess} tab={tab} />;
            })}
          </div>
        ) : null}
      </section>

      <div id="orders" className="scroll-mt-6 flex flex-wrap items-center gap-3 border-b border-border pb-3 pt-2">
        <h2 className="text-xl font-semibold">{t("aftersales.orders")}</h2>
        <span className="font-mono text-[10px] uppercase tracking-[0.16em] text-muted">{t("customer.wallet.bookingHistory")}</span>
      </div>

      {loading && bookings.length === 0 && (
        <div
          role="status"
          className="flex flex-col items-center justify-center gap-3 py-16"
        >
          <Loader2 size={24} className="animate-spin text-primary" />
          <p className="text-sm text-muted">{t("customer.wallet.loadingOrders")}</p>
        </div>
      )}

      {error && (
        <div
          role="alert"
          className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-danger/30 bg-danger-bg px-4 py-3 text-[13px] text-danger-ink"
        >
          <span className="flex items-center gap-2">
            <AlertCircle size={15} />
            {text(error)}
          </span>
          <button
            type="button"
            onClick={refreshWallet}
            className="inline-flex h-[32px] items-center gap-1.5 rounded-full border border-danger/30 px-3 text-xs font-semibold"
          >
            <RefreshCw size={13} />
            {t("customer.wallet.retry")}
          </button>
        </div>
      )}

      {!loading &&
        !error &&
        bookings.length === 0 && (
          <div className="flex flex-col items-center justify-center gap-4 py-20">
            <div className="grid h-16 w-16 place-items-center rounded-full bg-tag-bg">
              <Calendar size={24} className="text-muted" />
            </div>
            <p className="text-muted">
              {t("customer.wallet.noOrdersYet")}
            </p>
            <Link
              href="/"
              className="rounded-full border border-border px-4 py-2 text-sm font-medium text-foreground transition-colors hover:bg-tag-bg"
            >
              {t("customer.wallet.browseEvents")}
            </Link>
          </div>
        )}

      {!error && bookings.length > 0 && visibleOrders.length === 0 && (
        eventsPending ? (
          <div
            role="status"
            className="flex items-center justify-center gap-2 py-8 text-muted"
          >
            <Loader2 size={16} className="animate-spin" />
            {t("customer.wallet.loadingEventDetails")}
          </div>
        ) : (
          <p className="py-8 text-center text-muted">
            {t(tab === "upcoming" ? "customer.wallet.noUpcomingOrders" : "customer.wallet.noPastOrders")}
          </p>
        )
      )}

      {visibleOrders.length > 0 && (
        <div className="flex flex-col gap-4">
          {visibleOrders.map((order) => (
            <WalletOrderCard
              key={order.id}
              userId={userId}
              order={order}
              hasIssuedTicket={tickets.some((ticket) => ticket.booking_id === order.id && ticket.status === "ISSUED")}
              hasCheckedInTicket={tickets.some((ticket) => ticket.booking_id === order.id && ticket.status === "CHECKED_IN")}
              ticketsKnown={!ticketsLoading && !ticketsError}
              hasAnyTicket={tickets.some((ticket) => ticket.booking_id === order.id)}
              onRefundAccessChange={handleRefundAccessChange}
              onTicketsChanged={refreshTicketsAfterRefund}
              tab={tab}
              expanded={expandedId === order.id}
              onToggle={() =>
                setExpandedId((current) =>
                  current === order.id ? null : order.id,
                )
              }
            />
          ))}
        </div>
      )}

      {!error && bookings.length > 0 && (
        <div className="flex flex-wrap items-center justify-between gap-3 border-t border-border pt-4">
          <span className="text-xs text-muted">
            {t(total === 1 ? "customer.wallet.showingOneOrder" : "customer.wallet.showingOrders", {
              shown: number(bookings.length),
              total: number(total),
            })}
          </span>
          {hasMore ? (
            <button
              type="button"
              onClick={loadMore}
              disabled={loadingMore}
              className="inline-flex h-[36px] items-center gap-2 rounded-full border border-border px-4 text-sm font-medium transition-colors hover:bg-tag-bg disabled:opacity-50"
            >
              {loadingMore && <Loader2 size={14} className="animate-spin" />}
              {loadingMore ? t("customer.wallet.loadingMore") : t("customer.wallet.loadMore")}
            </button>
          ) : (
            <span className="text-xs text-muted">{t("customer.wallet.endOfOrders")}</span>
          )}
        </div>
      )}

      {loadMoreError && (
        <div
          role="alert"
          className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-danger/30 bg-danger-bg px-4 py-3 text-[13px] text-danger-ink"
        >
          <span>{text(loadMoreError)}</span>
          <button
            type="button"
            onClick={loadMore}
            className="rounded-full border border-danger/30 px-3 py-1 text-xs font-semibold"
          >
            {t("customer.wallet.retry")}
          </button>
        </div>
      )}

      <p className="text-[11px] text-muted">{t("customer.wallet.localTimezoneNote")}</p>
    </div>
  );
}
