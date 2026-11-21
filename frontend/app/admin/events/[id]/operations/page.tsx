"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import {
  Activity,
  AlertCircle,
  ArrowLeft,
  Check,
  Clock3,
  Copy,
  RefreshCw,
  ShieldCheck,
  Ticket,
} from "lucide-react";
import { AdminEventEngagement } from "@/components/admin-event-engagement";
import { apiClient } from "@/lib/api/client";
import type {
  ApiAdminEventTicketRecord,
  ApiAdminEventTicketStatus,
} from "@/lib/api/types";
import { useAuth } from "@/lib/auth-context";
import { useI18n, type TranslationKey } from "@/lib/i18n";
import {
  ADMIN_EVENT_OPERATIONS_RECORD_LIMIT,
  AdminEventOperationsError,
  adminEventOperationsErrorCode,
  isAdminEventOperationsId,
  type AdminEventOperationsErrorCode,
  type AdminEventTicketOperations,
} from "@/lib/admin-event-operations";
import { withRequestDeadline } from "@/lib/request-deadline";

type OperationsState =
  | { key: string; status: "loading" }
  | { key: string; status: "ready"; data: AdminEventTicketOperations }
  | { key: string; status: "error"; code: AdminEventOperationsErrorCode };

function formatTimestamp(
  value: string,
  formatDate: (value: string | number | Date, options?: Intl.DateTimeFormatOptions) => string,
): string {
  return formatDate(value, { dateStyle: "medium", timeStyle: "short" });
}

function deliveryReasonKey(code: string): TranslationKey {
  return code ? "admin.eventOperations.deliveryReasonOther" : "admin.eventOperations.deliveryReasonUnavailable";
}

const OPERATIONS_ERROR_KEYS: Record<AdminEventOperationsErrorCode, TranslationKey> = {
  invalid_request: "admin.eventOperations.invalid_request",
  unauthorized: "admin.eventOperations.unauthorized",
  forbidden: "admin.eventOperations.forbidden",
  not_found: "admin.eventOperations.not_found",
  capacity_exceeded: "admin.eventOperations.capacity_exceeded",
  unavailable: "admin.eventOperations.unavailable",
  invalid_response: "admin.eventOperations.invalid_response",
};

const TICKET_ERROR_KEYS: Record<AdminEventOperationsErrorCode, TranslationKey> = {
  invalid_request: "admin.ticketList.invalidRequest",
  unauthorized: "admin.ticketList.sessionExpired",
  forbidden: "admin.ticketList.forbidden",
  not_found: "admin.ticketList.notFound",
  capacity_exceeded: "admin.ticketList.capacity",
  unavailable: "admin.ticketList.unavailable",
  invalid_response: "admin.ticketList.unavailable",
};

type TicketListState = {
  ownerKey: string;
  requestKey: string;
  tickets: ApiAdminEventTicketRecord[];
  usedCursors: string[];
  observedAt: string | null;
  hasMore: boolean;
  nextCursor: string | null;
  loading: boolean;
  loadingMore: boolean;
  errorCode: AdminEventOperationsErrorCode | null;
  errorMode: "first-page" | "load-more" | null;
};

type TicketCursorState = {
  ownerKey: string;
  cursor: string | null;
};

type TicketRequestIdentity = {
  ownerKey: string;
  requestKey: string;
};

function emptyTicketListState(ownerKey: string): TicketListState {
  return {
    ownerKey,
    requestKey: "",
    tickets: [],
    usedCursors: [],
    observedAt: null,
    hasMore: false,
    nextCursor: null,
    loading: false,
    loadingMore: false,
    errorCode: null,
    errorMode: null,
  };
}

function ticketStatusLabel(status: ApiAdminEventTicketStatus, t: (key: TranslationKey) => string): string {
  switch (status) {
    case "ALL": return t("admin.ticketList.statusAll");
    case "ISSUED": return t("admin.ticketList.statusIssued");
    case "CHECKED_IN": return t("admin.ticketList.statusCheckedIn");
    case "REVOKED": return t("admin.ticketList.statusRevoked");
  }
}

function StatCard({
  label,
  value,
  detail,
  onViewTickets,
  viewTicketsLabel,
  viewTicketsAriaLabel,
  formatNumber,
}: {
  label: string;
  value: number;
  detail: string;
  onViewTickets?: () => void;
  viewTicketsLabel?: string;
  viewTicketsAriaLabel?: string;
  formatNumber: (value: number) => string;
}) {
  return (
    <div className="min-w-0 rounded-xl border border-border bg-card p-4 sm:p-5">
      <dt className="text-[11px] font-semibold uppercase tracking-wider text-muted">{label}</dt>
      <dd className="mt-2 font-mono text-[26px] font-semibold leading-none tabular-nums text-foreground sm:text-[30px]">
        {formatNumber(value)}
      </dd>
      <p className="mt-2 break-words text-[11px] leading-relaxed text-muted">{detail}</p>
      {onViewTickets && viewTicketsLabel ? (
        <a
          href="#admin-event-ticket-records"
          onClick={onViewTickets}
          aria-label={viewTicketsAriaLabel}
          className="mt-3 min-h-11 rounded-md text-left text-xs font-semibold text-primary underline underline-offset-2 hover:text-foreground focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
        >
          {viewTicketsLabel}
        </a>
      ) : null}
    </div>
  );
}

function LoadingState({ label }: { label: string }) {
  return (
    <div role="status" aria-live="polite" className="flex min-h-40 items-center justify-center gap-2 text-[13px] text-muted">
      <RefreshCw size={15} className="animate-spin" aria-hidden="true" />
      {label}
    </div>
  );
}

function ErrorState({
  code,
  onRetry,
  t,
}: {
  code: AdminEventOperationsErrorCode;
  onRetry: () => void;
  t: (key: TranslationKey) => string;
}) {
  return (
    <section aria-label={t("admin.eventOperations.errorTitle")} className="rounded-xl border border-danger/30 bg-danger-bg p-5 sm:p-6">
      <div className="flex items-start gap-3">
        <AlertCircle size={18} className="mt-0.5 shrink-0 text-danger-ink" aria-hidden="true" />
        <div className="min-w-0 flex-1">
          <h2 className="text-sm font-semibold">{t("admin.eventOperations.errorTitle")}</h2>
          <p role="alert" className="mt-1 break-words text-[13px] leading-relaxed text-danger-ink">
            {t(OPERATIONS_ERROR_KEYS[code])}
          </p>
        </div>
      </div>
      <button
        type="button"
        onClick={onRetry}
        className="mt-4 inline-flex min-h-11 items-center gap-2 rounded-full border border-border px-4 text-[12px] font-semibold text-foreground transition-colors hover:bg-tag-bg focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
      >
        <RefreshCw size={13} aria-hidden="true" />
        {t("admin.eventOperations.retryRead")}
      </button>
    </section>
  );
}

export default function AdminEventOperationsPage() {
  const params = useParams<{ id: string }>();
  const rawEventId = params?.id;
  const eventId = isAdminEventOperationsId(rawEventId) ? rawEventId : null;
  const { user, isLoading: authLoading } = useAuth();
  const { t, number, date } = useI18n();
  const [reloadKey, setReloadKey] = useState(0);
  const sessionVersion = apiClient.getSessionVersion();
  const ownerKey = `${eventId ?? String(rawEventId ?? "invalid")}|${user?.id ?? "signed-out"}|${user?.role ?? "none"}|${sessionVersion}|${reloadKey}`;
  const canRead = Boolean(!authLoading && user?.id && user.role === "admin" && eventId);
  const [state, setState] = useState<OperationsState>({ key: "", status: "loading" });
  const [ticketStatus, setTicketStatus] = useState<ApiAdminEventTicketStatus>("ALL");
  const [ticketRefreshKey, setTicketRefreshKey] = useState(0);
  const [ticketRetryKey, setTicketRetryKey] = useState(0);
  const [copiedDeliveryBooking, setCopiedDeliveryBooking] = useState("");
  const [deliveryCopyFailed, setDeliveryCopyFailed] = useState(false);
  const ticketOwnerKey = [
    eventId ?? String(rawEventId ?? "invalid"),
    user?.id ?? "signed-out",
    user?.role ?? "none",
    String(sessionVersion),
    ticketStatus,
    String(ticketRefreshKey),
  ].join("|");
  const [ticketCursorState, setTicketCursorState] = useState<TicketCursorState>({ ownerKey: "", cursor: null });
  const [ticketState, setTicketState] = useState<TicketListState>(() => emptyTicketListState(""));
  const ticketCursor = ticketCursorState.ownerKey === ticketOwnerKey && ticketState.ownerKey === ticketOwnerKey
    ? ticketCursorState.cursor
    : null;
  const ticketRequestKey = JSON.stringify([ticketOwnerKey, ticketCursor, ticketRetryKey]);
  const lastTicketRequest = useRef<TicketRequestIdentity | null>(null);

  async function copyDeliveryBookingId(bookingId: string) {
    try {
      await navigator.clipboard.writeText(bookingId);
      setCopiedDeliveryBooking(`${eventId ?? ""}:${bookingId}`);
      setDeliveryCopyFailed(false);
    } catch {
      setCopiedDeliveryBooking("");
      setDeliveryCopyFailed(true);
    }
  }

  useEffect(() => {
    if (!canRead || !eventId) return;

    const controller = new AbortController();
    let current = true;

    const request = apiClient.getAdminEventTicketOperations(eventId, controller.signal);
    void withRequestDeadline(request, controller, 10_000)
      .then((data) => {
        if (!current || apiClient.getSessionVersion() !== sessionVersion) return;
        setState({ key: ownerKey, status: "ready", data });
      })
      .catch((error: unknown) => {
        // Cleanup marks the request inactive before aborting. The same signal
        // also aborts on deadline, which must become a retryable unavailable
        // state rather than an endless spinner.
        if (!current) return;
        const code = error instanceof AdminEventOperationsError
          ? error.code
          : adminEventOperationsErrorCode(error);
        setState({ key: ownerKey, status: "error", code });
      });

    return () => {
      current = false;
      controller.abort();
    };
  }, [canRead, eventId, ownerKey, sessionVersion]);

  useEffect(() => {
    if (!canRead || !eventId) return;
    if (lastTicketRequest.current?.ownerKey === ticketOwnerKey && lastTicketRequest.current.requestKey === ticketRequestKey) return;

    const controller = new AbortController();
    let current = true;
    const cursor = ticketCursor;

    const query = {
      status: ticketStatus,
      page_size: 20,
      ...(cursor ? { cursor } : {}),
    };
    void withRequestDeadline(
      apiClient.listAdminEventTickets(eventId, query, controller.signal),
      controller,
      10_000,
    )
      .then((response) => {
        if (!current || apiClient.getSessionVersion() !== sessionVersion) return;
        if (!cursor) setTicketCursorState({ ownerKey: ticketOwnerKey, cursor: null });
        lastTicketRequest.current = { ownerKey: ticketOwnerKey, requestKey: ticketRequestKey };
        setTicketState((previous) => {
          const ownsCurrentRows = Boolean(cursor && previous.ownerKey === ticketOwnerKey);
          const base = ownsCurrentRows ? previous : emptyTicketListState(ticketOwnerKey);
          const usedCursors = cursor ? [...base.usedCursors, cursor] : base.usedCursors;
          const existingTicketIds = new Set(base.tickets.map((ticket) => ticket.ticket_id));
          const pageTicketIds = new Set<string>();
          const hasDuplicateTicket = response.tickets.some((ticket) => {
            if (existingTicketIds.has(ticket.ticket_id) || pageTicketIds.has(ticket.ticket_id)) return true;
            pageTicketIds.add(ticket.ticket_id);
            return false;
          });
          const repeatsCursor = response.has_more && (
            !response.next_cursor ||
            response.next_cursor === cursor ||
            usedCursors.includes(response.next_cursor)
          );
          if (hasDuplicateTicket || repeatsCursor) {
            return {
              ...base,
              ownerKey: ticketOwnerKey,
              requestKey: ticketRequestKey,
              usedCursors,
              hasMore: false,
              nextCursor: null,
              loading: false,
              loadingMore: false,
              errorCode: "invalid_response",
              errorMode: cursor ? "load-more" : "first-page",
            };
          }
          return {
            ...base,
            ownerKey: ticketOwnerKey,
            requestKey: ticketRequestKey,
            usedCursors,
            tickets: [...base.tickets, ...response.tickets],
            observedAt: response.observed_at,
            hasMore: response.has_more,
            nextCursor: response.next_cursor,
            loading: false,
            loadingMore: false,
            errorCode: null,
            errorMode: null,
          };
        });
      })
      .catch((error: unknown) => {
        if (!current) return;
        if (!cursor) setTicketCursorState({ ownerKey: ticketOwnerKey, cursor: null });
        lastTicketRequest.current = { ownerKey: ticketOwnerKey, requestKey: ticketRequestKey };
        const code = error instanceof AdminEventOperationsError
          ? error.code
          : adminEventOperationsErrorCode(error);
        setTicketState((previous) => {
          const ownsCurrentRows = Boolean(cursor && previous.ownerKey === ticketOwnerKey);
          const keepRows = ownsCurrentRows && code !== "unauthorized" && code !== "forbidden" && code !== "not_found";
          const base = keepRows ? previous : emptyTicketListState(ticketOwnerKey);
          return {
            ...base,
            ownerKey: ticketOwnerKey,
            requestKey: ticketRequestKey,
            loading: false,
            loadingMore: false,
            errorCode: code,
            errorMode: cursor ? "load-more" : "first-page",
          };
        });
      });

    return () => {
      current = false;
      controller.abort();
    };
  }, [canRead, eventId, ticketOwnerKey, ticketCursor, ticketRetryKey, sessionVersion, ticketStatus, ticketRequestKey]);

  const authFailure: AdminEventOperationsErrorCode | null = authLoading
    ? null
    : !user
      ? "unauthorized"
      : user.role !== "admin"
        ? "forbidden"
        : !eventId
          ? "invalid_request"
          : null;
  const visibleState: OperationsState = authLoading
    ? { key: ownerKey, status: "loading" }
    : authFailure
    ? { key: ownerKey, status: "error", code: authFailure }
    : state.key === ownerKey
      ? state
      : { key: ownerKey, status: "loading" };
  const data = visibleState.status === "ready" ? visibleState.data : null;
  const ownedTicketState = ticketState.ownerKey === ticketOwnerKey
    ? ticketState
    : emptyTicketListState(ticketOwnerKey);
  const ticketRequestPending = Boolean(canRead && ownedTicketState.requestKey !== ticketRequestKey);
  const keepPrefixWhileLoadingMore = Boolean(ticketRequestPending && ticketCursor && ownedTicketState.tickets.length > 0);
  const ticketBaseState = ticketRequestPending && !keepPrefixWhileLoadingMore
    ? emptyTicketListState(ticketOwnerKey)
    : ownedTicketState;
  const visibleTicketState: TicketListState = {
    ...ticketBaseState,
    loading: ticketRequestPending && !keepPrefixWhileLoadingMore,
    loadingMore: keepPrefixWhileLoadingMore,
    errorCode: ticketRequestPending ? null : ticketBaseState.errorCode,
    errorMode: ticketRequestPending ? null : ticketBaseState.errorMode,
  };
  const ticketError = visibleTicketState.errorCode ?? (authLoading ? null : authFailure);

  function selectTicketStatus(status: ApiAdminEventTicketStatus) {
    setTicketCursorState({ ownerKey: "", cursor: null });
    setTicketStatus(status);
  }

  function refreshTickets() {
    setTicketRefreshKey((current) => current + 1);
  }

  function retryTickets() {
    if (
      ticketState.ownerKey === ticketOwnerKey &&
      (ticketState.errorCode === "unauthorized" || ticketState.errorCode === "forbidden" || ticketState.errorCode === "not_found")
    ) {
      setTicketCursorState({ ownerKey: ticketOwnerKey, cursor: null });
    }
    setTicketRetryKey((current) => current + 1);
  }

  return (
    <div className="flex min-h-[calc(100dvh_-_3.5rem)] flex-col lg:min-h-screen">
      <header className="sticky top-14 z-10 flex h-14 items-center justify-between border-b border-border bg-card px-3 sm:px-6 lg:top-0">
        <div className="flex min-w-0 items-center gap-1.5 text-sm text-muted">
          <span>{t("admin.nav.operations")}</span>
          <span aria-hidden="true">/</span>
          <Link href="/admin/events" className="rounded-sm hover:text-foreground">{t("admin.events.title")}</Link>
          <span aria-hidden="true">/</span>
          <strong className="truncate font-semibold text-foreground">{t("admin.eventOperations.title")}</strong>
        </div>
        <span className="hidden shrink-0 rounded-md border border-border bg-tag-bg px-2 py-0.5 font-mono text-[11px] text-muted sm:inline-flex">
          {t("admin.common.adminOnly")}
        </span>
      </header>

      <main className="flex min-w-0 w-full max-w-[1280px] flex-col gap-5 p-3 sm:p-6">
        {canRead && eventId && <AdminEventEngagement key={ownerKey} eventId={eventId} />}
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="min-w-0">
            <h1 className="break-words text-[22px] font-bold tracking-tight">
              {data?.event_title ?? t("admin.eventOperations.title")}
            </h1>
            <p className="mt-1 max-w-[700px] break-words text-[13px] leading-relaxed text-muted">
              {t("admin.eventOperations.pageDescription")}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Link
              href="/admin/events"
              className="inline-flex min-h-11 items-center gap-2 rounded-full border border-border px-4 text-[12px] font-semibold text-muted transition-colors hover:bg-tag-bg hover:text-foreground focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
            >
              <ArrowLeft size={14} aria-hidden="true" />
              {t("admin.eventOperations.backToEvents")}
            </Link>
            {eventId ? (
              <div className="flex flex-wrap gap-2">
                <Link
                  href={`/admin/events?eventId=${encodeURIComponent(eventId)}`}
                  className="inline-flex min-h-11 items-center gap-2 rounded-full bg-primary px-4 text-[12px] font-semibold text-graphite transition-colors hover:bg-primary-hover focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
                >
                  <ShieldCheck size={14} aria-hidden="true" />
                  {t("admin.eventOperations.manageScannerAssignments")}
                </Link>
                <Link href={`/admin/events/${eventId}/refunds`} className="inline-flex min-h-11 items-center gap-2 rounded-full border border-border bg-card px-4 text-[12px] font-semibold transition-colors hover:bg-tag-bg focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary">
                  {t("admin.refunds.openQueue")}
                </Link>
              </div>
            ) : null}
          </div>
        </div>

        {visibleState.status === "loading" ? <LoadingState label={t("admin.eventOperations.loading")} /> : null}
        {visibleState.status === "error" ? (
          <ErrorState code={visibleState.code} onRetry={() => setReloadKey((current) => current + 1)} t={t} />
        ) : null}
        {data ? (
          <>
            <section aria-label={t("admin.eventOperations.eventRecord")} className="grid min-w-0 grid-cols-1 gap-3 border-y border-border py-4 sm:grid-cols-3 sm:gap-5">
              <div className="min-w-0">
                <span className="block text-[10px] font-semibold uppercase tracking-wider text-muted">{t("admin.eventOperations.venue")}</span>
                <span className="mt-1 block break-words text-[13px] font-medium">{data.event_venue || t("admin.eventOperations.venueNotListed")}</span>
              </div>
              <div className="min-w-0">
                <span className="block text-[10px] font-semibold uppercase tracking-wider text-muted">{t("admin.eventOperations.eventDate")}</span>
                <span className="mt-1 block break-words text-[13px] font-medium">{formatTimestamp(data.event_date, date)}</span>
              </div>
              <div className="min-w-0">
                <span className="block text-[10px] font-semibold uppercase tracking-wider text-muted">{t("admin.eventOperations.bookingSource")}</span>
                <span className="mt-1 inline-flex items-center gap-1.5 text-[13px] font-medium">
                  <span aria-hidden="true" className="h-1.5 w-1.5 rounded-full bg-success" />
                  {t("admin.eventOperations.sourceAvailable")}
                </span>
              </div>
            </section>
            <p className="-mt-3 break-words text-[11px] leading-relaxed text-muted">
              {t("admin.eventOperations.sourcesObservedSeparately")}
            </p>

            {data.delivery_exceptions && data.delivery_exceptions.length > 0 ? (
              <div role="status" className="flex flex-wrap items-start justify-between gap-3 rounded-xl border border-border bg-tag-bg px-4 py-3 text-[13px]">
                <span className="flex min-w-0 items-start gap-2">
                  <AlertCircle size={15} className="mt-0.5 shrink-0" aria-hidden="true" />
                  <span className="min-w-0 break-words">
                    <span className="block font-semibold">{t("admin.eventOperations.deliveryAttention", { count: number(data.delivery_exceptions.length) })}</span>
                    <span className="mt-0.5 block text-[11px] leading-relaxed text-muted">{t("admin.eventOperations.deliveryAttentionScope")}</span>
                  </span>
                </span>
                <a href="#admin-event-delivery-exceptions" className="inline-flex min-h-10 shrink-0 items-center rounded-full border border-border bg-card px-3 text-[11px] font-semibold focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary">
                  {t("admin.eventOperations.deliveryAttentionDetails")}
                </a>
              </div>
            ) : null}

            <section aria-label={t("admin.eventOperations.recordedTicketCounts")} className="grid min-w-0 grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
              <StatCard
                label={t("admin.eventOperations.totalTickets")}
                value={data.total_recorded_count}
                detail={t("admin.eventOperations.bookingTicketsSnapshot")}
                onViewTickets={() => selectTicketStatus("ALL")}
                viewTicketsLabel={t("admin.ticketList.viewTickets")}
                viewTicketsAriaLabel={t("admin.ticketList.statusCard", { status: ticketStatusLabel("ALL", t) })}
                formatNumber={number}
              />
              <StatCard
                label={t("admin.eventOperations.issued")}
                value={data.issued_count}
                detail={t("admin.eventOperations.issuedDetail")}
                onViewTickets={() => selectTicketStatus("ISSUED")}
                viewTicketsLabel={t("admin.ticketList.viewTickets")}
                viewTicketsAriaLabel={t("admin.ticketList.statusCard", { status: ticketStatusLabel("ISSUED", t) })}
                formatNumber={number}
              />
              <StatCard
                label={t("admin.eventOperations.checkedIn")}
                value={data.checked_in_count}
                detail={t("admin.eventOperations.checkedInDetail")}
                onViewTickets={() => selectTicketStatus("CHECKED_IN")}
                viewTicketsLabel={t("admin.ticketList.viewTickets")}
                viewTicketsAriaLabel={t("admin.ticketList.statusCard", { status: ticketStatusLabel("CHECKED_IN", t) })}
                formatNumber={number}
              />
              <StatCard
                label={t("admin.eventOperations.revoked")}
                value={data.revoked_count}
                detail={t("admin.eventOperations.revokedDetail")}
                onViewTickets={() => selectTicketStatus("REVOKED")}
                viewTicketsLabel={t("admin.ticketList.viewTickets")}
                viewTicketsAriaLabel={t("admin.ticketList.statusCard", { status: ticketStatusLabel("REVOKED", t) })}
                formatNumber={number}
              />
            </section>

            <section aria-labelledby="admin-event-delivery-exceptions" className="min-w-0 rounded-xl border border-border bg-card p-4 sm:p-5">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="min-w-0">
                  <h2 id="admin-event-delivery-exceptions" className="text-sm font-semibold">{t("admin.eventOperations.deliveryExceptionsTitle")}</h2>
                  <p className="mt-1 break-words text-xs leading-relaxed text-muted">{t("admin.eventOperations.deliveryExceptionsScope")}</p>
                </div>
                {data.delivery_exceptions !== undefined ? <span className="shrink-0 rounded-full bg-tag-bg px-2.5 py-1 text-[11px] font-semibold">{t("admin.eventOperations.deliveryExceptionsCount", { count: number(data.delivery_exceptions.length) })}</span> : null}
              </div>
              {data.delivery_exceptions === undefined ? (
                <p role="status" className="mt-3 rounded-lg border border-border bg-background px-3 py-3 text-xs leading-relaxed text-muted">{t("admin.eventOperations.deliveryExceptionsUnavailable")}</p>
              ) : data.delivery_exceptions.length === 0 ? (
                <p role="status" className="mt-3 rounded-lg border border-border bg-background px-3 py-3 text-xs leading-relaxed text-muted">{t("admin.eventOperations.deliveryExceptionsEmpty")}</p>
              ) : (
                <ul className="mt-3 space-y-3">
                  {data.delivery_exceptions.map((exception) => {
                    const copyKey = `${eventId ?? ""}:${exception.booking_id}`;
                    const copied = copiedDeliveryBooking === copyKey;
                    const receiptLabel = exception.issuance_receipt_outcome === "NOT_RECORDED"
                      ? t("admin.eventOperations.deliveryReceiptNotRecorded")
                      : exception.issuance_receipt_outcome === "issued"
                        ? t("admin.eventOperations.deliveryReceiptIssued")
                        : exception.issuance_receipt_outcome === "denied"
                          ? t("admin.eventOperations.deliveryReceiptDenied")
                          : t("admin.eventOperations.deliveryReceiptConflict");
                    return (
                      <li key={exception.booking_id} className="min-w-0 rounded-lg border border-border bg-background p-3 sm:p-4">
                        <div className="flex flex-wrap items-start justify-between gap-2">
                          <div className="min-w-0">
                            <p className="text-[11px] font-semibold text-muted">{t("admin.eventOperations.deliveryExceptionState")}</p>
                            <p className="mt-1 break-words text-xs font-semibold">{t("admin.eventOperations.deliveryPending")}</p>
                          </div>
                          <div className="flex flex-wrap items-center gap-2">
                            <Link href={`/admin/orders/${exception.booking_id}`} className="inline-flex min-h-10 shrink-0 items-center rounded-lg border border-border px-3 text-[11px] font-semibold focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary">
                              {t("admin.orderLookup.openOrder")}
                            </Link>
                            <button type="button" onClick={() => void copyDeliveryBookingId(exception.booking_id)} className="inline-flex min-h-10 shrink-0 items-center gap-1.5 rounded-lg border border-border px-3 text-[11px] font-semibold focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary" aria-label={t("admin.eventOperations.copyBookingId", { id: exception.booking_id })}>
                              {copied ? <Check size={13} aria-hidden="true" /> : <Copy size={13} aria-hidden="true" />}
                              {copied ? t("admin.eventOperations.bookingIdCopied") : t("admin.eventOperations.copyBookingIdButton")}
                            </button>
                          </div>
                        </div>
                        <p className="mt-2 break-all font-mono text-[11px] text-muted">{t("admin.eventOperations.deliveryBookingId")}: <code>{exception.booking_id}</code></p>
                        <dl className="mt-3 grid min-w-0 grid-cols-1 gap-2 text-xs sm:grid-cols-2">
                          <div className="min-w-0"><dt className="inline text-muted">{t("admin.eventOperations.deliveryBookingStatus")}: </dt><dd className="inline font-semibold">{exception.booking_status}</dd></div>
                          <div className="min-w-0 break-words"><dt className="inline text-muted">{t("admin.eventOperations.deliveryTicketCounts")}: </dt><dd className="inline">{t("admin.eventOperations.deliveryTicketCountsValue", { expected: number(exception.expected_ticket_count), recorded: number(exception.recorded_ticket_count) })}</dd></div>
                          <div className="min-w-0 break-words"><dt className="inline text-muted">{t("admin.eventOperations.deliveryReceipt")}: </dt><dd className="inline">{receiptLabel}</dd></div>
                          <div className="min-w-0 break-words"><dt className="inline text-muted">{t("admin.eventOperations.deliveryReasonCode")}: </dt><dd className="inline">{t(deliveryReasonKey(exception.issuance_reason_code))}</dd></div>
                          <div className="min-w-0 break-words sm:col-span-2"><dt className="inline text-muted">{t("admin.eventOperations.deliveryBookingCreated")}: </dt><dd className="inline"><time dateTime={exception.booking_created_at}>{formatTimestamp(exception.booking_created_at, date)}</time></dd></div>
                        </dl>
                      </li>
                    );
                  })}
                </ul>
              )}
              {deliveryCopyFailed ? <p role="status" className="mt-2 break-words text-[11px] text-muted">{t("admin.eventOperations.deliveryCopyFailed")}</p> : null}
              {copiedDeliveryBooking.startsWith(`${eventId ?? ""}:`) && !deliveryCopyFailed ? <p role="status" className="mt-2 text-[11px] text-success-ink">{t("admin.eventOperations.bookingIdCopiedNotice")}</p> : null}
              <p className="mt-3 break-words border-t border-border pt-3 text-[11px] leading-relaxed text-muted">
                {t("admin.eventOperations.deliveryExceptionsObservation", { date: formatTimestamp(data.observed_at, date) })}
              </p>
            </section>

            <section aria-labelledby="admin-event-operations-checkins" className="grid min-w-0 grid-cols-1 gap-4 lg:grid-cols-[minmax(220px,0.7fr)_minmax(0,1.3fr)]">
              <div className="rounded-xl border border-border bg-card p-4 sm:p-5">
                <div className="flex items-center gap-2 text-muted">
                  <Activity size={14} aria-hidden="true" />
                  <h2 id="admin-event-operations-checkins" className="text-[11px] font-semibold uppercase tracking-wider">{t("admin.eventOperations.successfulCheckins")}</h2>
                </div>
                <p className="mt-3 font-mono text-[28px] font-semibold leading-none tabular-nums text-foreground">
                  {number(data.successful_checkin_count)}
                </p>
                <p className="mt-2 break-words text-[11px] leading-relaxed text-muted">{t("admin.eventOperations.auditRecordCount")}</p>
              </div>

              <div className="min-w-0 rounded-xl border border-border bg-card p-4 sm:p-5">
                <h3 className="text-[11px] font-semibold uppercase tracking-wider text-muted">{t("admin.eventOperations.latestCheckinTimes")}</h3>
                {data.recent_checkins.length === 0 ? (
                  <p className="mt-3 rounded-lg border border-border px-3 py-3 text-[12px] text-muted">{t("admin.eventOperations.noAcceptedCheckins")}</p>
                ) : (
                  <ol className="mt-2 divide-y divide-border">
                    {data.recent_checkins.map((entry, index) => (
                      <li key={`${entry.checked_in_at}-${index}`} className="flex min-w-0 items-center gap-2 py-2.5 text-[12px]">
                        <Clock3 size={13} className="shrink-0 text-muted" aria-hidden="true" />
                        <time className="min-w-0 break-words font-mono tabular-nums text-foreground" dateTime={entry.checked_in_at}>
                          {formatTimestamp(entry.checked_in_at, date)}
                        </time>
                      </li>
                    ))}
                  </ol>
                )}
              </div>
            </section>

            <section aria-label={t("admin.eventOperations.coverage")} className="rounded-xl border border-border bg-background p-4 sm:p-5">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="flex min-w-0 items-start gap-2.5">
                  <Ticket size={15} className="mt-0.5 shrink-0 text-primary" aria-hidden="true" />
                  <div className="min-w-0">
                    <h2 className="text-[12px] font-semibold">{t("admin.eventOperations.snapshotTitle")}</h2>
                    <p className="mt-1 break-words text-[11px] leading-relaxed text-muted">
                      {t("admin.eventOperations.snapshotCoverage")}
                    </p>
                  </div>
                </div>
                <div className="min-w-0 text-left sm:text-right">
                  <span className="block text-[10px] font-semibold uppercase tracking-wider text-muted">{t("admin.eventOperations.observedAt")}</span>
                  <time className="mt-1 block break-words font-mono text-[11px] tabular-nums text-foreground" dateTime={data.observed_at}>
                    {formatTimestamp(data.observed_at, date)}
                  </time>
                </div>
              </div>
              <div className="mt-4 flex flex-wrap items-center justify-between gap-3 border-t border-border pt-3 text-[11px]">
                <span className="text-muted">{t("admin.eventOperations.liveEligibility")} <strong className="font-semibold text-foreground">{t("admin.eventOperations.notEvaluated")}</strong></span>
                <span className="font-mono text-muted">{t("admin.eventOperations.sourceReadLimit", { count: number(ADMIN_EVENT_OPERATIONS_RECORD_LIMIT) })}</span>
              </div>
            </section>

            {data.total_recorded_count === 0 ? (
              <p role="status" className="rounded-lg border border-border px-4 py-3 text-[12px] text-muted">{t("admin.eventOperations.noTickets")}</p>
            ) : null}

            <p className="break-words text-[11px] leading-relaxed text-muted">
              {t("admin.eventOperations.recordedCountCaveat")}
            </p>
            <div className="flex justify-end border-t border-border pt-3">
              <button
                type="button"
                onClick={() => setReloadKey((current) => current + 1)}
                className="inline-flex min-h-11 items-center gap-2 rounded-full border border-border px-4 text-[12px] font-semibold text-muted transition-colors hover:bg-tag-bg hover:text-foreground focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
              >
                <RefreshCw size={13} aria-hidden="true" />
                {t("admin.common.refresh")}
              </button>
            </div>
          </>
        ) : null}

        <section
          id="admin-event-ticket-records"
          aria-label={t("admin.ticketList.title")}
          aria-busy={visibleTicketState.loading || visibleTicketState.loadingMore}
          className="min-w-0 rounded-xl border border-border bg-card p-4 sm:p-5"
        >
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="min-w-0">
              <h2 className="break-words text-base font-semibold">{t("admin.ticketList.title")}</h2>
              <p className="mt-1 max-w-3xl break-words text-xs leading-relaxed text-muted">
                {t("admin.ticketList.readOnlyScope")}
              </p>
            </div>
            <button
              type="button"
              onClick={refreshTickets}
              disabled={!canRead || visibleTicketState.loading}
              className="inline-flex min-h-11 shrink-0 items-center gap-2 rounded-full border border-border px-4 text-xs font-semibold text-muted hover:bg-tag-bg hover:text-foreground disabled:cursor-wait disabled:opacity-50"
            >
              <RefreshCw size={13} aria-hidden="true" className={visibleTicketState.loading ? "animate-spin" : ""} />
              {visibleTicketState.loading ? t("admin.ticketList.refreshing") : t("admin.ticketList.refresh")}
            </button>
          </div>

          <div className="mt-4 flex flex-wrap items-end justify-between gap-3">
            <label htmlFor="admin-ticket-status" className="grid gap-1.5 text-xs font-semibold text-muted">
              <span>{t("admin.ticketList.filterStatus")}</span>
              <select
                id="admin-ticket-status"
                aria-label={t("admin.ticketList.filterStatus")}
                value={ticketStatus}
                onChange={(event) => selectTicketStatus(event.target.value as ApiAdminEventTicketStatus)}
                disabled={!canRead}
                className="h-11 min-w-48 rounded-lg border border-control-border bg-background px-3 text-sm text-foreground focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary disabled:opacity-50"
              >
                {(["ALL", "ISSUED", "CHECKED_IN", "REVOKED"] as const).map((status) => (
                  <option key={status} value={status}>{ticketStatusLabel(status, t)}</option>
                ))}
              </select>
            </label>
            {visibleTicketState.observedAt ? (
              <p className="break-words text-xs text-muted">
                {t("admin.ticketList.observedAt", { time: formatTimestamp(visibleTicketState.observedAt, date) })}
              </p>
            ) : null}
          </div>

          <p className="mt-3 break-words text-[11px] leading-relaxed text-muted">
            {t("admin.ticketList.sourceNote", { count: number(5000) })}
          </p>

          {authLoading ? (
            <p role="status" aria-live="polite" className="mt-4 rounded-lg border border-border px-3 py-3 text-sm text-muted">
              {t("admin.ticketList.loading")}
            </p>
          ) : null}
          {!authLoading && authFailure ? (
            <div role="alert" className="mt-4 rounded-lg border border-danger/30 bg-danger-bg px-3 py-3 text-sm text-danger-ink">
              {t(TICKET_ERROR_KEYS[authFailure])}
            </div>
          ) : null}
          {canRead && visibleTicketState.loading ? (
            <p role="status" aria-live="polite" className="mt-4 rounded-lg border border-border px-3 py-3 text-sm text-muted">
              {t("admin.ticketList.loading")}
            </p>
          ) : null}
          {ticketError && canRead ? (
            <div role="alert" className="mt-4 flex flex-wrap items-start justify-between gap-3 rounded-lg border border-danger/30 bg-danger-bg px-3 py-3 text-sm text-danger-ink">
              <span className="min-w-0 flex-1 break-words">
                {ticketError === "capacity_exceeded"
                  ? t(TICKET_ERROR_KEYS[ticketError])
                  : visibleTicketState.errorMode === "load-more" && visibleTicketState.tickets.length > 0
                    ? t("admin.ticketList.loadMoreError")
                    : t(TICKET_ERROR_KEYS[ticketError])}
              </span>
              {ticketError !== "capacity_exceeded" ? (
                <button type="button" onClick={retryTickets} className="min-h-11 shrink-0 px-2 font-semibold underline underline-offset-2">
                  {t("admin.common.retry")}
                </button>
              ) : null}
            </div>
          ) : null}
          {canRead && !visibleTicketState.loading && !ticketError && visibleTicketState.tickets.length === 0 ? (
            <p role="status" className="mt-4 rounded-lg border border-border px-3 py-4 text-sm text-muted">
              {t("admin.ticketList.empty")}
            </p>
          ) : null}

          {visibleTicketState.tickets.length > 0 ? (
            <div className="mt-4 max-w-full overflow-x-auto rounded-lg border border-border">
              <table aria-label={t("admin.ticketList.title")} className="w-full min-w-[620px] border-collapse text-left text-xs">
                <thead>
                  <tr className="bg-background text-[10px] font-semibold uppercase tracking-wider text-muted">
                    <th scope="col" className="px-3 py-3">{t("admin.ticketList.ticketId")}</th>
                    <th scope="col" className="px-3 py-3">{t("admin.ticketList.status")}</th>
                    <th scope="col" className="px-3 py-3">{t("admin.ticketList.issuedAt")}</th>
                    <th scope="col" className="px-3 py-3">{t("admin.ticketList.checkedInAt")}</th>
                    <th scope="col" className="px-3 py-3">{t("admin.ticketList.revokedAt")}</th>
                  </tr>
                </thead>
                <tbody>
                  {visibleTicketState.tickets.map((ticket) => (
                    <tr key={ticket.ticket_id} className="border-t border-border align-top">
                      <td className="break-all px-3 py-3 font-mono text-[11px] text-muted">{ticket.ticket_id}</td>
                      <td className="whitespace-nowrap px-3 py-3 font-semibold text-foreground">
                        {ticketStatusLabel(ticket.status, t)}
                      </td>
                      <td className="whitespace-nowrap px-3 py-3 text-muted">{formatTimestamp(ticket.issued_at, date)}</td>
                      <td className="whitespace-nowrap px-3 py-3 text-muted">
                        {ticket.checked_in_at ? formatTimestamp(ticket.checked_in_at, date) : "—"}
                      </td>
                      <td className="whitespace-nowrap px-3 py-3 text-muted">
                        {ticket.revoked_at ? formatTimestamp(ticket.revoked_at, date) : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}

          {visibleTicketState.loadingMore ? (
            <p role="status" aria-live="polite" className="mt-3 text-center text-xs text-muted">
              {t("admin.ticketList.loadingMore")}
            </p>
          ) : null}
          {canRead && visibleTicketState.hasMore && !visibleTicketState.errorCode ? (
            <div className="mt-4 flex justify-center">
              <button
                type="button"
                onClick={() => setTicketCursorState({ ownerKey: ticketOwnerKey, cursor: visibleTicketState.nextCursor })}
                disabled={visibleTicketState.loadingMore || !visibleTicketState.nextCursor}
                className="inline-flex min-h-11 items-center justify-center rounded-full border border-border px-5 text-xs font-semibold text-foreground hover:bg-tag-bg disabled:cursor-wait disabled:opacity-50"
              >
                {visibleTicketState.loadingMore ? t("admin.ticketList.loadingMore") : t("admin.ticketList.loadMore")}
              </button>
            </div>
          ) : null}
          {canRead && !visibleTicketState.hasMore && visibleTicketState.tickets.length > 0 && !ticketError ? (
            <p className="mt-4 text-center text-[11px] text-muted">{t("admin.ticketList.endOfRecords")}</p>
          ) : null}
        </section>
      </main>
    </div>
  );
}
