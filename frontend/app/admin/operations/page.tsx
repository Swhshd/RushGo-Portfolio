"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { AlertCircle, ArrowDownUp, CalendarClock, ExternalLink, Loader2, RefreshCw } from "lucide-react";
import { apiClient } from "@/lib/api/client";
import type { ApiOperationsItem } from "@/lib/api/types";
import { isAdminEventOperationsId } from "@/lib/admin-event-operations";
import { notificationSourceSnapshot, type NotificationSourceSnapshot } from "@/lib/admin-operations-view";
import { useI18n } from "@/lib/i18n";

const PAGE_SIZE = 50;
const MAX_RANGE_MS = 31 * 24 * 60 * 60 * 1000;

type DateRange = { from: string; to: string };
type OperationsPage = { items: ApiOperationsItem[]; hasMore: boolean; observedAt: string | null };

function toLocalInput(value: Date) {
  const pad = (number: number) => String(number).padStart(2, "0");
  return `${value.getFullYear()}-${pad(value.getMonth() + 1)}-${pad(value.getDate())}T${pad(value.getHours())}:${pad(value.getMinutes())}`;
}

function fromLocalInput(value: string): Date | null {
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function paymentLabel(state: string, t: ReturnType<typeof useI18n>["t"]) {
  if (state === "available") return t("admin.operations.paymentAvailable");
  if (state === "unavailable") return t("admin.operations.paymentUnavailable");
  return t("admin.operations.paymentNotRecorded");
}

function OperationCards({ items, t, date, number }: {
  items: ApiOperationsItem[];
  t: ReturnType<typeof useI18n>["t"];
  date: ReturnType<typeof useI18n>["date"];
  number: ReturnType<typeof useI18n>["number"];
}) {
  if (!items.length) return null;
  return <div className="flex flex-col gap-3">
    {items.map((item) => {
      const bookingIsValid = isAdminEventOperationsId(item.booking_id);
      const eventIsValid = isAdminEventOperationsId(item.event_id);
      return <article key={item.booking_id} className="rounded-2xl border border-border bg-card p-4 sm:p-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0"><h3 className="break-all font-mono text-sm font-semibold">{item.booking_id}</h3><p className="mt-1 text-xs text-muted">{date(item.created_at, { dateStyle: "medium", timeStyle: "short" })}</p></div>
          <span className="rounded-full bg-tag-bg px-2.5 py-1 text-[11px] font-semibold">{paymentLabel(item.payment_source_state, t)}</span>
        </div>
        <div className="mt-3 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted">
          <span>{t("admin.operations.status", { status: item.booking_status || "—" })}</span>
          <span>{t("admin.operations.refundStatus", { status: item.refund_status || "—" })}</span>
          <span>{t("admin.operations.ticketCount", { count: number(item.ticket_count) })}</span>
        </div>
        {item.issues.length > 0 && <ul className="mt-3 flex flex-col gap-2 border-t border-border pt-3">
          {item.issues.map((issue, index) => <li key={`${issue.kind}:${issue.source}:${index}`} className="min-w-0 rounded-lg bg-tag-bg px-3 py-2 text-xs leading-5">
            <p className="break-words font-semibold">{t("admin.operations.issue", { kind: issue.kind, source: issue.source, state: issue.state })}</p>
            {issue.next_action && <p className="mt-0.5 break-words text-muted">{t("admin.operations.nextAction", { action: issue.next_action })}</p>}
            {issue.observed_at && <time className="mt-0.5 block text-[10px] text-muted" dateTime={issue.observed_at}>{date(issue.observed_at, { dateStyle: "medium", timeStyle: "short" })}</time>}
          </li>)}
        </ul>}
        <div className="mt-4 flex flex-wrap justify-end gap-2">
          {bookingIsValid && <Link href={`/admin/orders/${encodeURIComponent(item.booking_id)}`} className="inline-flex min-h-10 items-center gap-1.5 rounded-full border border-border px-3 text-xs font-semibold"><ExternalLink size={13} />{t("admin.operations.openOrder")}</Link>}
          {eventIsValid && <Link href={`/admin/events/${encodeURIComponent(item.event_id)}/refunds`} className="inline-flex min-h-10 items-center gap-1.5 rounded-full border border-border px-3 text-xs font-semibold"><ArrowDownUp size={13} />{t("admin.operations.openRefunds")}</Link>}
        </div>
      </article>;
    })}
  </div>;
}

export default function AdminOperationsPage() {
  const { t, date, number } = useI18n();
  const [fromInput, setFromInput] = useState("");
  const [toInput, setToInput] = useState("");
  const [range, setRange] = useState<DateRange | null>(null);
  const [rangeError, setRangeError] = useState(false);
  const [bookingOffset, setBookingOffset] = useState(0);
  const [notificationOffset, setNotificationOffset] = useState(0);
  const [bookingEntry, setBookingEntry] = useState<{ key: string; page: OperationsPage } | null>(null);
  const [bookingFailureKey, setBookingFailureKey] = useState<string | null>(null);
  const [notificationEntry, setNotificationEntry] = useState<{ key: string; page: NotificationSourceSnapshot } | null>(null);
  const [notificationFailureKey, setNotificationFailureKey] = useState<string | null>(null);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const bookingRequestKey = range ? `${range.from}:${range.to}:${bookingOffset}:${refreshNonce}` : null;
  const notificationRequestKey = range ? `${range.from}:${range.to}:${notificationOffset}:${refreshNonce}` : null;
  const booking = bookingRequestKey && bookingEntry?.key === bookingRequestKey ? bookingEntry.page : null;
  const notifications = notificationRequestKey && notificationEntry?.key === notificationRequestKey ? notificationEntry.page : null;
  const bookingError = Boolean(bookingRequestKey && bookingFailureKey === bookingRequestKey);
  const notificationError = Boolean(notificationRequestKey && notificationFailureKey === notificationRequestKey);
  const bookingLoading = Boolean(bookingRequestKey && !booking && !bookingError);
  const notificationLoading = Boolean(notificationRequestKey && !notifications && !notificationError);

  useEffect(() => {
    let active = true;
    void Promise.resolve().then(() => {
      if (!active) return;
      const to = new Date();
      const from = new Date(to.getTime() - 7 * 24 * 60 * 60 * 1000);
      setFromInput(toLocalInput(from));
      setToInput(toLocalInput(to));
      setRange({ from: from.toISOString(), to: to.toISOString() });
    });
    return () => { active = false; };
  }, []);

  useEffect(() => {
    if (!range || !bookingRequestKey) return;
    const controller = new AbortController();
    void apiClient.listAdminOperations({ from: range.from, to: range.to, limit: PAGE_SIZE, offset: bookingOffset }, controller.signal)
      .then((response) => { if (!controller.signal.aborted) { setBookingEntry({ key: bookingRequestKey, page: { items: response.items ?? [], hasMore: response.has_more, observedAt: response.observed_at } }); setBookingFailureKey(null); } })
      .catch(() => { if (!controller.signal.aborted) setBookingFailureKey(bookingRequestKey); });
    return () => controller.abort();
  }, [bookingOffset, bookingRequestKey, range, refreshNonce]);

  useEffect(() => {
    if (!range || !notificationRequestKey) return;
    const controller = new AbortController();
    void apiClient.listAdminOperations({ from: range.from, to: range.to, limit: PAGE_SIZE, offset: notificationOffset }, controller.signal)
      .then((response) => {
        if (controller.signal.aborted) return;
        setNotificationEntry({ key: notificationRequestKey, page: notificationSourceSnapshot(response) });
        setNotificationFailureKey(null);
      })
      .catch(() => { if (!controller.signal.aborted) setNotificationFailureKey(notificationRequestKey); });
    return () => controller.abort();
  }, [notificationOffset, notificationRequestKey, range, refreshNonce]);

  function applyRange() {
    const from = fromLocalInput(fromInput);
    const to = fromLocalInput(toInput);
    if (!from || !to || from >= to || to.getTime() - from.getTime() > MAX_RANGE_MS) {
      setRangeError(true);
      return;
    }
    setRangeError(false);
    setBookingOffset(0);
    setNotificationOffset(0);
    setRange({ from: from.toISOString(), to: to.toISOString() });
    setRefreshNonce((value) => value + 1);
  }

  const bookingPage = Math.floor(bookingOffset / PAGE_SIZE) + 1;
  const notificationPage = Math.floor(notificationOffset / PAGE_SIZE) + 1;
  const outboxPath = "/admin/outbox?source=notification";

  return (
    <div className="flex min-h-screen min-w-0 flex-col">
      <header className="sticky top-14 z-10 flex min-h-14 items-center justify-between gap-3 border-b border-border bg-card px-3 py-2 sm:px-6 lg:top-0">
        <div className="flex items-center gap-2 text-sm"><span className="text-muted">{t("admin.nav.operations")}</span><span className="text-muted">/</span><strong>{t("admin.operations.title")}</strong></div>
        <button type="button" onClick={() => setRefreshNonce((value) => value + 1)} disabled={bookingLoading || notificationLoading} className="inline-flex min-h-10 items-center gap-2 rounded-full border border-border px-3 text-xs font-semibold disabled:opacity-50"><RefreshCw size={14} />{t("admin.operations.refresh")}</button>
      </header>
      <main className="mx-auto flex w-full max-w-[1280px] flex-col gap-5 p-3 sm:p-6">
        <div><h1 className="text-[22px] font-bold tracking-tight">{t("admin.operations.title")}</h1><p className="mt-1 max-w-3xl text-[13px] leading-5 text-muted">{t("admin.operations.description")}</p></div>

        <section className="rounded-xl border border-border bg-card p-4">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-[1fr_1fr_auto] sm:items-end">
            <label className="flex flex-col gap-1.5 text-xs font-semibold text-muted">{t("admin.operations.from")}<input type="datetime-local" value={fromInput} onChange={(event) => setFromInput(event.target.value)} className="h-11 min-w-0 rounded-lg border border-control-border bg-background px-3 text-sm text-foreground" /></label>
            <label className="flex flex-col gap-1.5 text-xs font-semibold text-muted">{t("admin.operations.to")}<input type="datetime-local" value={toInput} onChange={(event) => setToInput(event.target.value)} className="h-11 min-w-0 rounded-lg border border-control-border bg-background px-3 text-sm text-foreground" /></label>
            <button type="button" onClick={applyRange} className="inline-flex min-h-11 items-center justify-center gap-2 rounded-full bg-primary px-4 text-xs font-semibold text-graphite"><CalendarClock size={14} />{t("admin.operations.applyRange")}</button>
          </div>
          {rangeError && <p role="alert" className="mt-3 text-xs text-danger-ink">{t("admin.operations.rangeError")}</p>}
        </section>

        <div className="grid grid-cols-1 gap-5 xl:grid-cols-2">
          <section className="flex min-w-0 flex-col gap-3">
            <div className="flex flex-wrap items-center justify-between gap-2"><h2 className="text-base font-semibold">{t("admin.operations.bookingHeading")}</h2>{booking?.observedAt && <span className="text-[10px] text-muted">{t("admin.operations.observedAt", { date: date(booking.observedAt, { dateStyle: "short", timeStyle: "short" }) })}</span>}</div>
            {bookingError && <p role="alert" className="flex items-center gap-2 rounded-xl border border-danger/30 bg-danger-bg p-3 text-xs text-danger-ink"><AlertCircle size={15} />{t("admin.operations.bookingError")}</p>}
            {bookingLoading && !booking && <p role="status" className="flex min-h-24 items-center justify-center gap-2 text-xs text-muted"><Loader2 className="animate-spin" size={17} />{t("common.loading")}</p>}
            {booking && !bookingLoading && booking.items.length === 0 && <p className="rounded-xl border border-dashed border-border bg-card p-5 text-center text-xs text-muted">{t("admin.operations.emptyBookings")}</p>}
            {booking && <OperationCards items={booking.items} t={t} date={date} number={number} />}
            <div className="flex items-center justify-between gap-2"><button type="button" onClick={() => setBookingOffset((value) => Math.max(0, value - PAGE_SIZE))} disabled={bookingOffset === 0 || bookingLoading} className="min-h-10 rounded-full border border-border px-3 text-xs disabled:opacity-40">{t("admin.operations.previous")}</button><span className="text-[10px] text-muted">{t("admin.operations.page", { page: number(bookingPage) })}</span><button type="button" onClick={() => setBookingOffset((value) => value + PAGE_SIZE)} disabled={!booking?.hasMore || bookingLoading} className="min-h-10 rounded-full border border-border px-3 text-xs disabled:opacity-40">{t("admin.operations.next")}</button></div>
          </section>

          <section className="flex min-w-0 flex-col gap-3">
            <div className="flex flex-wrap items-center justify-between gap-2"><h2 className="text-base font-semibold">{t("admin.operations.notificationHeading")}</h2><Link href={outboxPath} className="inline-flex min-h-10 items-center gap-1 rounded-full border border-border px-3 text-[11px] font-semibold">{t("admin.operations.openNotificationOutbox")}</Link></div>
            {notificationError && <p role="alert" className="flex items-center gap-2 rounded-xl border border-danger/30 bg-danger-bg p-3 text-xs text-danger-ink"><AlertCircle size={15} />{t("admin.operations.notificationError")}</p>}
            {notificationLoading && !notifications && <p role="status" className="flex min-h-24 items-center justify-center gap-2 text-xs text-muted"><Loader2 className="animate-spin" size={17} />{t("common.loading")}</p>}
            {notifications?.state === "unavailable" && <p role="status" className="rounded-xl border border-border bg-tag-bg p-4 text-xs leading-5 text-muted">{t("admin.operations.notificationUnavailable")}</p>}
            {notifications?.state === "available" && !notificationLoading && notifications.items.length === 0 && <p className="rounded-xl border border-dashed border-border bg-card p-5 text-center text-xs text-muted">{t("admin.operations.emptyNotifications")}</p>}
            {notifications?.state === "available" && <OperationCards items={notifications.items} t={t} date={date} number={number} />}
            <div className="flex items-center justify-between gap-2"><button type="button" onClick={() => setNotificationOffset((value) => Math.max(0, value - PAGE_SIZE))} disabled={notificationOffset === 0 || notificationLoading} className="min-h-10 rounded-full border border-border px-3 text-xs disabled:opacity-40">{t("admin.operations.previous")}</button><span className="text-[10px] text-muted">{t("admin.operations.page", { page: number(notificationPage) })}</span><button type="button" onClick={() => setNotificationOffset((value) => value + PAGE_SIZE)} disabled={notifications?.state !== "available" || !notifications.hasMore || notificationLoading} className="min-h-10 rounded-full border border-border px-3 text-xs disabled:opacity-40">{t("admin.operations.next")}</button></div>
            {notifications?.observedAt && <p className="text-right text-[10px] text-muted">{t("admin.operations.observedAt", { date: date(notifications.observedAt, { dateStyle: "short", timeStyle: "short" }) })}</p>}
          </section>
        </div>
      </main>
    </div>
  );
}
