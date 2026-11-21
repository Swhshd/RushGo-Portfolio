"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import { AlertCircle, ArrowLeft, ExternalLink, Loader2, RefreshCw } from "lucide-react";
import { useAuth } from "@/lib/auth-context";
import { apiClient } from "@/lib/api/client";
import type { ApiAdminBookingDetail, ApiEvent } from "@/lib/api/types";
import { isAdminEventOperationsId } from "@/lib/admin-event-operations";
import { adminBookingSignals, type AdminBookingSignal } from "@/lib/admin-booking-detail";
import { formatUsdCents } from "@/lib/wallet";
import { useI18n, type TranslationKey } from "@/lib/i18n";

const STATUS_KEYS: Record<string, TranslationKey> = {
  PENDING: "admin.orderLookup.statusPending",
  CONFIRMED: "admin.orderLookup.statusConfirmed",
  FAILED: "admin.orderLookup.statusFailed",
  CANCELLED: "admin.orderLookup.statusCancelled",
  EXPIRED: "admin.orderLookup.statusExpired",
};

const REFUND_STATUS_KEYS: Record<string, TranslationKey> = {
  REQUESTED: "admin.refunds.statusRequested",
  WAITING_CHECKIN_RESOLUTION: "admin.refunds.statusWaiting",
  APPROVED: "admin.refunds.statusApproved",
  REFUND_PROCESSING: "admin.refunds.statusProcessing",
  REFUNDED: "admin.refunds.statusRefunded",
  REJECTED: "admin.refunds.statusRejected",
  WITHDRAWN: "admin.refunds.statusWithdrawn",
  NEEDS_ATTENTION: "admin.refunds.statusAttention",
};

const PAYMENT_KEYS: Record<string, TranslationKey> = {
  pending: "admin.refunds.paymentPending",
  succeeded: "admin.refunds.paymentSucceeded",
  unknown: "admin.refunds.paymentUnknown",
  not_required: "admin.refunds.paymentNotRequired",
};

const TICKET_STATE_KEYS: Record<string, TranslationKey> = {
  NOT_STARTED: "admin.refunds.ticketNotStarted",
  PENDING: "admin.refunds.ticketPending",
  REVOKED: "admin.refunds.ticketRevoked",
  NEEDS_ATTENTION: "admin.refunds.ticketNeedsAttention",
};

const SIGNAL_KEYS: Record<AdminBookingSignal, TranslationKey> = {
  multiple_receipts: "admin.orderLookup.signalMultipleReceipts",
  receipt_without_ticket: "admin.orderLookup.signalReceiptWithoutTicket",
  issuance_denied: "admin.orderLookup.signalIssuanceDenied",
  tickets_not_recorded: "admin.orderLookup.signalTicketsNotRecorded",
  refund_payment_pending: "admin.orderLookup.signalRefundPaymentPending",
  refund_result_unknown: "admin.orderLookup.signalRefundUnknown",
  revocation_pending: "admin.orderLookup.signalRevocationPending",
  revocation_not_synced: "admin.orderLookup.signalRevocationNotSynced",
};

type LoadState = "loading" | "ready" | "not_found" | "forbidden" | "error";

export default function AdminOrderDetailPage() {
  const params = useParams<{ id: string }>();
  const rawId = Array.isArray(params?.id) ? params.id[0] : params?.id;
  const bookingId = isAdminEventOperationsId(rawId ?? "") ? rawId! : null;
  const { user, isLoading } = useAuth();
  const { t } = useI18n();

  if (isLoading) return <CenterStatus label={t("admin.orderLookup.loading")} />;
  if (!user || user.role !== "admin") {
    return <Problem title={t("admin.orderLookup.forbiddenTitle")} body={t("admin.orderLookup.forbiddenBody")} />;
  }
  if (!bookingId) return <Problem title={t("admin.orderLookup.notFoundTitle")} body={t("admin.orderLookup.notFoundBody")} />;
  return <AdminOrderDetail key={`${user.id}:${bookingId}`} bookingId={bookingId} />;
}

function CenterStatus({ label }: { label: string }) {
  return <main role="status" className="flex min-h-[50vh] items-center justify-center gap-2 text-sm text-muted"><Loader2 size={16} className="animate-spin" /> {label}</main>;
}

function Problem({ title, body, retry }: { title: string; body: string; retry?: () => void }) {
  const { t } = useI18n();
  return (
    <main className="mx-auto flex max-w-2xl flex-col items-center gap-3 px-5 py-16 text-center">
      <AlertCircle size={24} className="text-danger-ink" />
      <h1 className="text-xl font-semibold">{title}</h1>
      <p className="text-sm text-muted">{body}</p>
      <div className="mt-2 flex flex-wrap justify-center gap-2">
        <Link href="/admin/orders" className="inline-flex min-h-11 items-center gap-2 rounded-full border border-border px-4 text-sm font-semibold"><ArrowLeft size={14} /> {t("admin.orderLookup.backToLookup")}</Link>
        {retry ? <button type="button" onClick={retry} className="inline-flex min-h-11 items-center gap-2 rounded-full bg-primary px-4 text-sm font-semibold text-graphite"><RefreshCw size={14} /> {t("admin.orderLookup.retry")}</button> : null}
      </div>
    </main>
  );
}

function AdminOrderDetail({ bookingId }: { bookingId: string }) {
  const [refreshKey, setRefreshKey] = useState(0);
  return <AdminOrderDetailData key={`${bookingId}:${refreshKey}`} bookingId={bookingId} onRefresh={() => setRefreshKey((key) => key + 1)} />;
}

function AdminOrderDetailData({ bookingId, onRefresh }: { bookingId: string; onRefresh: () => void }) {
  const { t, date, number, locale } = useI18n();
  const [state, setState] = useState<LoadState>("loading");
  const [detail, setDetail] = useState<ApiAdminBookingDetail | null>(null);
  const [event, setEvent] = useState<ApiEvent | null | undefined>(undefined);

  useEffect(() => {
    let stopped = false;
    void (async () => {
      try {
        const loaded = await apiClient.getAdminBookingDetail(bookingId);
        if (stopped) return;
        setDetail(loaded);
        setState("ready");
        void apiClient.getEvent(loaded.event_id).then((value) => {
          if (!stopped) setEvent(value);
        }).catch(() => {
          if (!stopped) setEvent(null);
        });
      } catch (cause) {
        if (stopped) return;
        const status = typeof cause === "object" && cause !== null && "status" in cause ? (cause as { status?: number }).status : undefined;
        if (status === 404) setState("not_found");
        else if (status === 401 || status === 403) setState("forbidden");
        else setState("error");
      }
    })();
    return () => { stopped = true; };
  }, [bookingId]);

  if (state === "loading") return <CenterStatus label={t("admin.orderLookup.loading")} />;
  if (state === "not_found") return <Problem title={t("admin.orderLookup.notFoundTitle")} body={t("admin.orderLookup.notFoundBody")} />;
  if (state === "forbidden") return <Problem title={t("admin.orderLookup.forbiddenTitle")} body={t("admin.orderLookup.forbiddenBody")} />;
  if (state === "error" || !detail) return <Problem title={t("admin.orderLookup.errorTitle")} body={t("admin.orderLookup.errorBody")} retry={onRefresh} />;

  const signals = adminBookingSignals(detail);
  const statusKey = STATUS_KEYS[detail.booking_status.toUpperCase()] ?? "admin.orderLookup.statusUnknown";
  const eventTitle = event === undefined
    ? t("admin.orderLookup.eventLoading")
    : event === null
      ? t("admin.orderLookup.eventUnavailable")
      : event.title || t("admin.orderLookup.untitledEvent");
  const refund = detail.refund;
  const paymentKey = refund ? PAYMENT_KEYS[refund.payment_refund_state.toLowerCase()] ?? "admin.refunds.paymentUnavailable" : null;
  const ticketStateKey = refund ? TICKET_STATE_KEYS[refund.ticket_revocation_state.toUpperCase()] ?? "admin.refunds.ticketUnavailable" : null;

  return (
    <main className="mx-auto flex w-full min-w-0 max-w-6xl flex-col gap-5 px-5 py-8 md:px-8">
      <div className="flex w-full min-w-0 flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <Link href="/admin/orders" className="inline-flex items-center gap-1.5 text-xs font-medium text-muted hover:text-foreground"><ArrowLeft size={13} /> {t("admin.orderLookup.backToLookup")}</Link>
          <h1 className="mt-2 text-2xl font-semibold tracking-tight">{t("admin.orderLookup.detailTitle")}</h1>
          <p className="mt-1 break-all font-mono text-[11px] text-muted">{detail.booking_id}</p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Link href={`/admin/events/${detail.event_id}/operations`} className="inline-flex min-h-10 items-center gap-1.5 rounded-full border border-border px-3 text-xs font-semibold"><ExternalLink size={13} /> {t("admin.orderLookup.openOperations")}</Link>
          <Link href={`/admin/events/${detail.event_id}/refunds`} className="inline-flex min-h-10 items-center gap-1.5 rounded-full border border-border px-3 text-xs font-semibold"><ExternalLink size={13} /> {t("admin.orderLookup.openRefunds")}</Link>
          <button type="button" onClick={onRefresh} className="inline-flex min-h-10 items-center gap-1.5 rounded-full border border-border px-3 text-xs font-semibold"><RefreshCw size={13} /> {t("admin.orderLookup.retry")}</button>
        </div>
      </div>

      <section className="grid w-full min-w-0 gap-3 rounded-xl border border-border bg-card p-4 sm:grid-cols-2 lg:grid-cols-4">
        <Fact label={t("admin.orderLookup.bookingStatus")} value={t(statusKey)} />
        <Fact label={t("admin.orderLookup.amount")} value={`${formatUsdCents(detail.total_amount_cents, locale)} ${detail.currency}`} />
        <Fact label={t("admin.orderLookup.createdAt")} value={date(detail.created_at, { dateStyle: "medium", timeStyle: "short" }) || t("admin.orderLookup.timeUnavailable")} />
        <Fact label={t("admin.orderLookup.observedAt")} value={date(detail.observed_at, { dateStyle: "medium", timeStyle: "short" }) || t("admin.orderLookup.timeUnavailable")} />
      </section>

      {signals.length > 0 ? (
        <section aria-labelledby="admin-order-signals" className="rounded-xl border border-border bg-tag-bg p-4">
          <h2 id="admin-order-signals" className="text-sm font-semibold">{t("admin.orderLookup.signals")}</h2>
          <ul className="mt-2 list-disc space-y-1 pl-5 text-sm">
            {signals.map((signal) => <li key={signal}>{t(SIGNAL_KEYS[signal])}</li>)}
          </ul>
          <p className="mt-2 text-[11px] leading-relaxed text-muted">{t("admin.orderLookup.signalBoundary")}</p>
        </section>
      ) : null}

      <section className="w-full min-w-0 rounded-xl border border-border bg-card p-4">
        <h2 className="text-sm font-semibold">{t("admin.orderLookup.event")}</h2>
        <p className="mt-2 break-words text-base font-medium">{eventTitle}</p>
        {event ? <p className="mt-1 text-xs text-muted">{date(event.date, { dateStyle: "medium", timeStyle: "short" })} · {[event.venue, event.location].filter(Boolean).join(", ")}</p> : null}
        <p className="mt-2 break-all font-mono text-[11px] text-muted">{t("admin.orderLookup.eventId")}: {detail.event_id}</p>
        <p className="mt-1 break-all font-mono text-[11px] text-muted">{t("admin.orderLookup.buyerId")}: {detail.user_id}</p>
        <p className="mt-1 text-[11px] text-muted">{t("admin.orderLookup.modePhase", { mode: detail.booking_mode || t("admin.orderLookup.unavailable"), phase: detail.lifecycle_phase || t("admin.orderLookup.unavailable"), generation: String(detail.lifecycle_generation) })}</p>
      </section>

      <section aria-labelledby="admin-order-items" className="w-full min-w-0 max-w-full overflow-x-auto rounded-xl border border-border bg-card">
        <h2 id="admin-order-items" className="px-4 py-3 text-sm font-semibold">{t("admin.orderLookup.items")}</h2>
        <table className="w-full min-w-[560px] border-collapse text-left text-sm">
          <thead><tr className="border-y border-border text-[11px] uppercase tracking-wider text-muted">
            <th className="px-4 py-2 font-semibold">{t("admin.orderLookup.tier")}</th><th className="px-4 py-2 font-semibold">{t("admin.orderLookup.quantity")}</th><th className="px-4 py-2 font-semibold">{t("admin.orderLookup.unitPrice")}</th><th className="px-4 py-2 font-semibold">{t("admin.orderLookup.seats")}</th>
          </tr></thead>
          <tbody>{detail.items.map((item) => {
            const tierName = item.tier_name || event?.tiers.find((tier) => tier.id === item.ticket_tier_id)?.name || t("admin.orderLookup.tierUnavailable");
            return (
            <tr key={item.id} className="border-b border-border last:border-0">
              <td className="break-words px-4 py-3">{tierName}</td>
              <td className="px-4 py-3 tabular-nums">{number(item.quantity)}</td>
              <td className="px-4 py-3 font-mono tabular-nums">{formatUsdCents(item.unit_price_cents, locale)}</td>
              <td className="break-all px-4 py-3 font-mono text-[11px] text-muted">{item.seat_ids.length ? item.seat_ids.join(", ") : t("admin.orderLookup.noSeats")}</td>
            </tr>
            );
          })}</tbody>
        </table>
      </section>

      <section aria-labelledby="admin-order-tickets" className="w-full min-w-0 max-w-full rounded-xl border border-border bg-card p-4">
        <h2 id="admin-order-tickets" className="text-sm font-semibold">{t("admin.orderLookup.tickets", { count: detail.tickets.length })}</h2>
        {detail.tickets.length === 0 ? <p className="mt-2 text-sm text-muted">{t("admin.orderLookup.noTickets")}</p> : (
          <div className="mt-3 max-w-full overflow-x-auto">
            <table className="w-full min-w-[760px] border-collapse text-left text-sm">
              <thead><tr className="border-y border-border text-[11px] uppercase tracking-wider text-muted">
                <th className="px-3 py-2 font-semibold">{t("admin.orderLookup.ticketId")}</th><th className="px-3 py-2 font-semibold">{t("admin.orderLookup.ticketStatus")}</th><th className="px-3 py-2 font-semibold">{t("admin.orderLookup.seat")}</th><th className="px-3 py-2 font-semibold">{t("admin.orderLookup.tierId")}</th><th className="px-3 py-2 font-semibold">{t("admin.orderLookup.issuedAt")}</th><th className="px-3 py-2 font-semibold">{t("admin.orderLookup.checkedInAt")}</th><th className="px-3 py-2 font-semibold">{t("admin.orderLookup.revokedAt")}</th>
              </tr></thead>
              <tbody>{detail.tickets.map((ticket) => (
                <tr key={ticket.ticket_id} className="border-b border-border last:border-0">
                  <td className="break-all px-3 py-2 font-mono text-[11px]">{ticket.ticket_id}</td>
                  <td className="px-3 py-2">{ticket.status}</td>
                  <td className="break-all px-3 py-2 font-mono text-[11px]">{ticket.seat_id}</td>
                  <td className="break-all px-3 py-2 font-mono text-[11px]">{ticket.ticket_tier_id}</td>
                  <td className="px-3 py-2 text-xs">{date(ticket.issued_at, { dateStyle: "medium", timeStyle: "short" }) || t("admin.orderLookup.timeUnavailable")}</td>
                  <td className="px-3 py-2 text-xs">{ticket.checked_in_at ? date(ticket.checked_in_at, { dateStyle: "medium", timeStyle: "short" }) : t("admin.orderLookup.notApplicable")}</td>
                  <td className="px-3 py-2 text-xs">{ticket.revoked_at ? date(ticket.revoked_at, { dateStyle: "medium", timeStyle: "short" }) : t("admin.orderLookup.notApplicable")}</td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        )}
        <div className="mt-3 border-t border-border pt-3 text-xs text-muted">
          <p>{t("admin.orderLookup.receipt")}: {detail.receipt ? t(detail.receipt.outcome === "issued" ? "admin.orderLookup.receiptIssued" : "admin.orderLookup.receiptDenied") : t("admin.orderLookup.receiptNotRecorded")}</p>
          {detail.receipt ? <p className="mt-1 break-words">{t("admin.orderLookup.receiptDetail", { reason: detail.receipt.reason_code || t("admin.orderLookup.unavailable"), count: String(detail.receipt.ticket_count), receipts: String(detail.receipt.receipt_count) })}</p> : null}
        </div>
      </section>

      <section aria-labelledby="admin-order-refund" className="w-full min-w-0 rounded-xl border border-border bg-card p-4">
        <h2 id="admin-order-refund" className="text-sm font-semibold">{t("admin.orderLookup.refund")}</h2>
        {refund ? (
          <div className="mt-3 grid gap-2 text-sm sm:grid-cols-2">
            <Fact label={t("admin.orderLookup.refundStatus")} value={t(REFUND_STATUS_KEYS[refund.status] ?? "admin.refunds.statusUnknown")} />
            <Fact label={t("admin.orderLookup.refundAmount")} value={`${formatUsdCents(refund.amount_cents, locale)} ${refund.currency}`} />
            <Fact label={t("admin.refunds.paymentState")} value={t(paymentKey ?? "admin.refunds.paymentNotRecorded")} />
            <Fact label={t("admin.refunds.ticketState")} value={t(ticketStateKey ?? "admin.refunds.ticketUnavailable")} />
            <Fact label={t("admin.orderLookup.requestedAt")} value={refund.requested_at ? date(refund.requested_at, { dateStyle: "medium", timeStyle: "short" }) : t("admin.orderLookup.timeUnavailable")} />
            <Fact label={t("admin.orderLookup.decidedAt")} value={refund.decided_at ? date(refund.decided_at, { dateStyle: "medium", timeStyle: "short" }) : t("admin.orderLookup.notApplicable")} />
            <div className="min-w-0 sm:col-span-2"><p className="text-[10px] font-semibold uppercase tracking-wider text-muted">{t("admin.orderLookup.refundReason")}</p><p className="mt-1 break-words text-sm">{refund.reason || t("admin.orderLookup.unavailable")}</p></div>
            {refund.decision_reason ? <div className="min-w-0 sm:col-span-2"><p className="text-[10px] font-semibold uppercase tracking-wider text-muted">{t("admin.orderLookup.decisionReason")}</p><p className="mt-1 break-words text-sm">{refund.decision_reason}</p></div> : null}
            <div className="min-w-0 sm:col-span-2"><p className="break-all font-mono text-[11px] text-muted">{t("admin.orderLookup.requestId")}: {refund.request_id}</p></div>
          </div>
        ) : <p className="mt-2 text-sm text-muted">{t("admin.orderLookup.noRefund")}</p>}
        <p className="mt-3 border-t border-border pt-3 text-[11px] leading-relaxed text-muted">{t("admin.orderLookup.refundBoundary")}</p>
      </section>

      <section className="w-full min-w-0 rounded-xl border border-border bg-card p-4">
        <h2 className="text-sm font-semibold">{t("admin.orderLookup.sources")}</h2>
        <p className="mt-2 text-xs leading-relaxed text-muted">{t("admin.orderLookup.sourceNote")}</p>
        <div className="mt-3 flex flex-wrap gap-2">
          <Link href={`/admin/events/${detail.event_id}/operations`} className="inline-flex min-h-10 items-center rounded-full border border-border px-3 text-xs font-semibold">{t("admin.orderLookup.openOperations")}</Link>
          <Link href={`/admin/events/${detail.event_id}/refunds`} className="inline-flex min-h-10 items-center rounded-full border border-border px-3 text-xs font-semibold">{t("admin.orderLookup.openRefunds")}</Link>
          <Link href={`/admin/outbox?event_id=${encodeURIComponent(detail.event_id)}`} className="inline-flex min-h-10 items-center rounded-full border border-border px-3 text-xs font-semibold">{t("admin.orderLookup.openOutbox")}</Link>
        </div>
      </section>
    </main>
  );
}

function Fact({ label, value }: { label: string; value: string }) {
  return <div className="min-w-0"><p className="text-[10px] font-semibold uppercase tracking-wider text-muted">{label}</p><p className="mt-1 break-words text-sm font-medium">{value}</p></div>;
}
