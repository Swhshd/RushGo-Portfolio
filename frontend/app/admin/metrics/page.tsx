"use client";

import { useEffect, useState } from "react";
import { CalendarClock, Download, RefreshCw } from "lucide-react";
import { apiClient } from "@/lib/api/client";
import { ApiError } from "@/lib/api/errors";
import type { ApiAdminMetricsMoneyGroup, ApiAdminMetricsResponse } from "@/lib/api/types";
import { useI18n } from "@/lib/i18n";

const MAX_RANGE_MS = 31 * 24 * 60 * 60 * 1000;

type DateRange = { from: string; to: string };

function toLocalInput(value: Date) {
  const pad = (number: number) => String(number).padStart(2, "0");
  return `${value.getFullYear()}-${pad(value.getMonth() + 1)}-${pad(value.getDate())}T${pad(value.getHours())}:${pad(value.getMinutes())}`;
}

function fromLocalInput(value: string): Date | null {
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function formatMinor(locale: string, currency: string, minor: number): string {
  try {
    const formatter = new Intl.NumberFormat(locale, { style: "currency", currency });
    const exponent = formatter.resolvedOptions().maximumFractionDigits ?? 2;
    return formatter.format(minor / (10 ** exponent));
  } catch {
    return `${currency} ${new Intl.NumberFormat(locale).format(minor)}`;
  }
}

function MetricCount({ label, value, number }: { label: string; value: number; number: (value: number) => string }) {
  return <div className="min-w-0 rounded-lg bg-background p-3"><dt className="break-words text-xs text-muted">{label}</dt><dd className="mt-1 break-all text-xl font-semibold tabular-nums">{number(value)}</dd></div>;
}

function MoneyGroup({ group, locale, t, number }: {
  group: ApiAdminMetricsMoneyGroup;
  locale: string;
  t: ReturnType<typeof useI18n>["t"];
  number: (value: number) => string;
}) {
  const stateLabel = group.state === "recorded_complete" ? t("admin.metrics.groupStateComplete") : t("admin.metrics.groupStatePartial");
  return <article className="min-w-0 rounded-xl border border-border bg-card p-4">
    <div className="flex flex-wrap items-start justify-between gap-2">
      <h3 className="break-words font-semibold">{t("admin.metrics.currencyMode", { currency: group.currency, mode: group.mode.toUpperCase() })}</h3>
      <span className="rounded-full bg-tag-bg px-2.5 py-1 text-[10px] font-medium text-muted">{stateLabel}</span>
    </div>
    <dl className="mt-4 grid min-w-0 grid-cols-1 gap-2 sm:grid-cols-2">
      <MetricCount label={t("admin.metrics.payments")} value={group.payments} number={number} />
      <MetricCount label={t("admin.metrics.unknownPayments")} value={group.unknown_payments} number={number} />
      <MetricCount label={t("admin.metrics.confirmedRefunds")} value={group.confirmed_refunds} number={number} />
      <div className="min-w-0 rounded-lg bg-background p-3"><dt className="break-words text-xs text-muted">{t("admin.metrics.collectedKnown")}</dt><dd className="mt-1 break-all text-sm font-semibold tabular-nums">{formatMinor(locale, group.currency, group.collected_known_minor)}</dd></div>
      <div className="min-w-0 rounded-lg bg-background p-3"><dt className="break-words text-xs text-muted">{t("admin.metrics.refundedKnown")}</dt><dd className="mt-1 break-all text-sm font-semibold tabular-nums">{formatMinor(locale, group.currency, group.refunded_known_minor)}</dd></div>
      <div className="min-w-0 rounded-lg bg-background p-3"><dt className="break-words text-xs text-muted">{t("admin.metrics.netKnown")}</dt><dd className="mt-1 break-words text-sm font-semibold tabular-nums">{group.net_minor === null ? t("admin.metrics.netUnknown") : formatMinor(locale, group.currency, group.net_minor)}</dd></div>
    </dl>
  </article>;
}

export default function AdminMetricsPage() {
  const { t, date, number, locale } = useI18n();
  const [fromInput, setFromInput] = useState("");
  const [toInput, setToInput] = useState("");
  const [range, setRange] = useState<DateRange | null>(null);
  const [rangeError, setRangeError] = useState(false);
  const [dataEntry, setDataEntry] = useState<{ key: string; data: ApiAdminMetricsResponse } | null>(null);
  const [loadFailure, setLoadFailure] = useState<{ key: string; narrowRange: boolean } | null>(null);
  const [downloading, setDownloading] = useState(false);
  const [downloadFailure, setDownloadFailure] = useState<{ narrowRange: boolean } | null>(null);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const requestKey = range ? `${range.from}:${range.to}:${refreshNonce}` : null;
  const data = requestKey && dataEntry?.key === requestKey ? dataEntry.data : null;
  const currentFailure = requestKey && loadFailure?.key === requestKey ? loadFailure : null;
  const loading = Boolean(requestKey) && !data && !currentFailure;
  const loadError = Boolean(currentFailure || downloadFailure);
  const narrowRange = Boolean(currentFailure?.narrowRange || downloadFailure?.narrowRange);

  useEffect(() => {
    let active = true;
    void Promise.resolve().then(() => {
      if (!active) return;
      const to = new Date();
      to.setSeconds(0, 0);
      const from = new Date(to.getTime() - 7 * 24 * 60 * 60 * 1000);
      setFromInput(toLocalInput(from));
      setToInput(toLocalInput(to));
      setRange({ from: from.toISOString(), to: to.toISOString() });
    });
    return () => { active = false; };
  }, []);

  useEffect(() => {
    if (!range || !requestKey) return;
    const controller = new AbortController();
    void apiClient.getAdminMetrics(range.from, range.to, controller.signal)
      .then((response) => { if (!controller.signal.aborted) { setDataEntry({ key: requestKey, data: response }); setLoadFailure(null); } })
      .catch((error: unknown) => {
        if (controller.signal.aborted) return;
        setLoadFailure({ key: requestKey, narrowRange: error instanceof ApiError && error.status === 422 });
      });
    return () => controller.abort();
  }, [range, refreshNonce, requestKey]);

  const inputFrom = fromLocalInput(fromInput);
  const inputTo = fromLocalInput(toInput);
  const rangeIsApplied = Boolean(range && inputFrom && inputTo && inputFrom.toISOString() === range.from && inputTo.toISOString() === range.to);

  function applyRange() {
    const from = fromLocalInput(fromInput);
    const to = fromLocalInput(toInput);
    if (!from || !to || from >= to || to.getTime() - from.getTime() > MAX_RANGE_MS) {
      setRangeError(true);
      return;
    }
    setRangeError(false);
    setDownloadFailure(null);
    setRange({ from: from.toISOString(), to: to.toISOString() });
    setRefreshNonce((value) => value + 1);
  }

  async function downloadCsv() {
    if (!range || downloading) return;
    setDownloading(true);
    setDownloadFailure(null);
    try {
      const { blob, filename } = await apiClient.downloadAdminMetricsCsv(range.from, range.to);
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = filename;
      anchor.style.display = "none";
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      window.setTimeout(() => URL.revokeObjectURL(url), 0);
    } catch (error) {
      setDownloadFailure({ narrowRange: error instanceof ApiError && error.status === 422 });
    } finally {
      setDownloading(false);
    }
  }

  return (
    <div className="flex min-h-screen min-w-0 flex-col">
      <header className="sticky top-14 z-10 flex min-h-14 items-center justify-between gap-3 border-b border-border bg-card px-3 py-2 sm:px-6 lg:top-0">
        <div className="flex min-w-0 items-center gap-2 text-sm"><span className="text-muted">{t("admin.nav.operations")}</span><span className="text-muted">/</span><strong className="truncate">{t("admin.metrics.title")}</strong></div>
        <button type="button" onClick={() => { setDownloadFailure(null); setRefreshNonce((value) => value + 1); }} disabled={!range || !rangeIsApplied || loading} className="inline-flex min-h-10 shrink-0 items-center gap-2 rounded-full border border-border px-3 text-xs font-semibold disabled:opacity-50"><RefreshCw size={14} />{t("admin.metrics.refresh")}</button>
      </header>
      <main className="mx-auto flex w-full max-w-[1280px] min-w-0 flex-col gap-5 p-3 sm:p-6">
        <div><h1 className="text-[22px] font-bold tracking-tight">{t("admin.metrics.title")}</h1><p className="mt-1 max-w-3xl text-[13px] leading-5 text-muted">{t("admin.metrics.description")}</p></div>

        <section className="rounded-xl border border-border bg-card p-4">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-[1fr_1fr_auto] sm:items-end">
            <label className="flex min-w-0 flex-col gap-1.5 text-xs font-semibold text-muted">{t("admin.metrics.from")}<input type="datetime-local" value={fromInput} onChange={(event) => setFromInput(event.target.value)} className="h-11 min-w-0 rounded-lg border border-control-border bg-background px-3 text-sm text-foreground" /></label>
            <label className="flex min-w-0 flex-col gap-1.5 text-xs font-semibold text-muted">{t("admin.metrics.to")}<input type="datetime-local" value={toInput} onChange={(event) => setToInput(event.target.value)} className="h-11 min-w-0 rounded-lg border border-control-border bg-background px-3 text-sm text-foreground" /></label>
            <button type="button" onClick={applyRange} className="inline-flex min-h-11 items-center justify-center gap-2 rounded-full bg-primary px-4 text-xs font-semibold text-graphite"><CalendarClock size={14} />{t("admin.metrics.applyRange")}</button>
          </div>
          {rangeError && <p role="alert" className="mt-3 text-xs text-danger-ink">{t("admin.metrics.rangeError")}</p>}
          <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
            {data && <p className="min-w-0 break-words text-xs text-muted">{t("admin.metrics.scope", { scope: data.scope })} · {t("admin.metrics.recordLimit", { count: number(data.record_limit) })}</p>}
            <button type="button" onClick={() => void downloadCsv()} disabled={!range || !rangeIsApplied || downloading} className="inline-flex min-h-10 items-center gap-2 rounded-full border border-border px-4 text-xs font-semibold disabled:opacity-50"><Download size={14} />{downloading ? t("admin.metrics.downloading") : t("admin.metrics.downloadCsv")}</button>
          </div>
        </section>

        {loading && !data && <p role="status" className="rounded-xl border border-border bg-card p-5 text-sm text-muted">{t("admin.metrics.loading")}</p>}
        {loadError && <p role="alert" className="rounded-xl border border-danger/30 bg-danger-bg p-4 text-sm text-danger-ink">{narrowRange ? t("admin.metrics.narrowRange") : t("admin.metrics.loadError")}</p>}

        {data && <>
          <section className="min-w-0 rounded-xl border border-border bg-card p-4 sm:p-5">
            <div className="flex flex-wrap items-center justify-between gap-2"><h2 className="text-base font-semibold">{t("admin.metrics.bookingHeading")}</h2><span className="break-all text-[10px] text-muted">{data.booking.source}</span></div>
            <p className="mt-1 text-[10px] text-muted">{t("admin.metrics.observedAt", { date: date(data.booking.observed_at, { dateStyle: "short", timeStyle: "short" }) })}</p>
            <dl className="mt-4 grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-5">
              <MetricCount label={t("admin.metrics.orders")} value={data.booking.orders} number={number} />
              <MetricCount label={t("admin.metrics.issuedTickets")} value={data.booking.issued_tickets} number={number} />
              <MetricCount label={t("admin.metrics.checkedInTickets")} value={data.booking.checked_in_tickets} number={number} />
              <MetricCount label={t("admin.metrics.refundRequests")} value={data.booking.refund_requests} number={number} />
              <MetricCount label={t("admin.metrics.confirmedRefunds")} value={data.booking.confirmed_refunds} number={number} />
            </dl>
          </section>

          <section className="flex min-w-0 flex-col gap-3">
            <div className="flex flex-wrap items-center justify-between gap-2"><h2 className="text-base font-semibold">{t("admin.metrics.paymentHeading")}</h2><span className="break-all text-[10px] text-muted">{data.payment.source}</span></div>
            {data.payment.source_state !== "available" ? <div role="status" className="rounded-xl border border-border bg-card p-5 text-sm text-muted"><p>{t("admin.metrics.paymentUnavailable")}</p>{data.payment.unavailable_reason && <p className="mt-1 break-words text-xs">{t("admin.metrics.paymentReason", { reason: data.payment.unavailable_reason })}</p>}</div> : <>
              {data.payment.observed_at && <p className="text-[10px] text-muted">{t("admin.metrics.observedAt", { date: date(data.payment.observed_at, { dateStyle: "short", timeStyle: "short" }) })}</p>}
              <p className="text-xs text-muted">{t("admin.metrics.scopeHint")}</p>
              {data.payment.groups.length === 0 ? <p className="rounded-xl border border-dashed border-border bg-card p-5 text-center text-sm text-muted">{t("admin.metrics.noMoneyGroups")}</p> : <div className="grid min-w-0 grid-cols-1 gap-3 lg:grid-cols-2">{data.payment.groups.map((group) => <MoneyGroup key={`${group.currency}:${group.mode}`} group={group} locale={locale} t={t} number={number} />)}</div>}
            </>}
          </section>
        </>}
      </main>
    </div>
  );
}
