"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { AlertCircle, CalendarDays, Loader2, RefreshCw } from "lucide-react";
import { useAuth } from "@/lib/auth-context";
import { apiClient } from "@/lib/api/client";
import type { ApiWorkItem, ApiWorkResponse } from "@/lib/api/types";
import { isAdminEventOperationsId } from "@/lib/admin-event-operations";
import { loginHref } from "@/lib/auth-navigation";
import { useI18n } from "@/lib/i18n";

const PAGE_SIZE = 50;

function accessLabel(access: string, t: ReturnType<typeof useI18n>["t"]) {
  if (access === "scanner") return t("customer.work.scanner");
  if (access === "approver") return t("customer.work.approver");
  return t("customer.work.admin");
}

function actionFor(item: ApiWorkItem) {
  if (!isAdminEventOperationsId(item.event_id)) return null;
  if (item.access === "scanner") return { href: `/events/${encodeURIComponent(item.event_id)}/check-in`, key: "customer.work.openScanner" as const };
  if (item.access === "approver") return { href: `/admin/events/${encodeURIComponent(item.event_id)}/refunds`, key: "customer.work.reviewRefunds" as const };
  return { href: `/admin/events/${encodeURIComponent(item.event_id)}/operations`, key: "customer.work.openOperations" as const };
}

export default function WorkbenchPage() {
  const { t, date, number } = useI18n();
  const { user, isLoading: authLoading } = useAuth();
  const router = useRouter();
  const [offset, setOffset] = useState(0);
  const [loadState, setLoadState] = useState<{ key: string; snapshot: ApiWorkResponse } | { key: string; error: true } | null>(null);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const requestKey = `${user?.id ?? "guest"}:${offset}:${refreshNonce}`;
  const currentLoadState = loadState?.key === requestKey ? loadState : null;
  const snapshot = currentLoadState && "snapshot" in currentLoadState ? currentLoadState.snapshot : null;
  const loading = Boolean(user) && currentLoadState === null;
  const error = Boolean(currentLoadState && "error" in currentLoadState);

  useEffect(() => {
    if (!authLoading && !user) router.replace(loginHref("/work", "AUTH_REQUIRED"));
  }, [authLoading, router, user]);

  useEffect(() => {
    if (!user) return;
    const controller = new AbortController();
    void apiClient.listMyWork(PAGE_SIZE, offset, controller.signal)
      .then((result) => { if (!controller.signal.aborted) setLoadState({ key: requestKey, snapshot: result }); })
      .catch(() => { if (!controller.signal.aborted) setLoadState({ key: requestKey, error: true }); });
    return () => controller.abort();
  }, [offset, requestKey, refreshNonce, user]);

  if (authLoading || !user) return <main role="status" className="mx-auto flex min-h-72 max-w-3xl items-center justify-center gap-3 px-4 text-sm text-muted"><Loader2 size={20} className="animate-spin text-primary" />{t("common.loading")}</main>;

  return (
    <main className="mx-auto flex w-full max-w-5xl flex-col gap-5 px-4 py-6 sm:py-9">
      <header className="flex flex-wrap items-start justify-between gap-3">
        <div><h1 className="text-2xl font-bold tracking-tight">{t("customer.work.title")}</h1><p className="mt-1 text-sm text-muted">{t("customer.work.description")}</p></div>
        <button type="button" onClick={() => { setLoadState(null); setRefreshNonce((value) => value + 1); }} disabled={loading} className="inline-flex min-h-11 items-center gap-2 rounded-full border border-border px-4 text-sm font-semibold disabled:opacity-50"><RefreshCw size={15} className={loading ? "animate-spin" : ""} />{t("customer.work.refresh")}</button>
      </header>

      {error && <p role="alert" className="flex items-center gap-2 rounded-xl border border-danger/30 bg-danger-bg px-4 py-3 text-sm text-danger-ink"><AlertCircle size={17} />{t("customer.work.loadError")}</p>}
      {loading && !snapshot && <div role="status" className="flex min-h-40 items-center justify-center gap-3 text-sm text-muted"><Loader2 size={20} className="animate-spin text-primary" />{t("common.loading")}</div>}
      {snapshot && !loading && snapshot.items.length === 0 && <div className="rounded-2xl border border-dashed border-border bg-card px-5 py-12 text-center"><CalendarDays className="mx-auto mb-3 text-muted" size={26} /><p className="text-sm text-muted">{t("customer.work.empty")}</p></div>}

      {snapshot && snapshot.items.length > 0 && <section aria-label={t("customer.work.title")} className="grid grid-cols-1 gap-3 md:grid-cols-2">
        {snapshot.items.map((item) => {
          const action = actionFor(item);
          return <article key={`${item.event_id}:${item.access}`} className="flex min-w-0 flex-col rounded-2xl border border-border bg-card p-4 sm:p-5">
            <div className="flex items-start justify-between gap-3"><div className="min-w-0"><h2 className="break-words text-base font-semibold">{item.title || item.event_id}</h2>{item.starts_at && <p className="mt-1 text-xs text-muted">{date(item.starts_at, { dateStyle: "medium", timeStyle: "short" })}</p>}</div><span className="shrink-0 rounded-full bg-tag-bg px-2.5 py-1 text-[11px] font-semibold">{accessLabel(item.access, t)}</span></div>
            {item.access === "approver" && <p className="mt-3 rounded-lg bg-tag-bg px-3 py-2 text-sm">{t("customer.work.pendingRefunds", { count: number(item.pending_refunds) })}</p>}
            <p className="mt-3 text-xs text-muted">{t("customer.work.eventState", { state: item.event_state || "—" })}</p>
            {item.event_source_state !== "available" && <p className="mt-2 text-xs text-warning-ink">{t("customer.work.sourceUnavailable")}</p>}
            <div className="mt-auto flex justify-end pt-4">{action && <Link href={action.href} className="inline-flex min-h-11 items-center rounded-full bg-primary px-4 text-xs font-semibold text-graphite">{t(action.key)}</Link>}</div>
          </article>;
        })}
      </section>}

      {snapshot && (offset > 0 || snapshot.has_more) && <nav className="flex items-center justify-between gap-3" aria-label={t("customer.work.title")}>
        <button type="button" onClick={() => { setLoadState(null); setOffset((value) => Math.max(0, value - PAGE_SIZE)); }} disabled={offset === 0 || loading} className="min-h-11 rounded-full border border-border px-4 text-sm disabled:opacity-40">{t("customer.work.previous")}</button>
        <span className="text-xs text-muted">{t("customer.work.page", { page: number(Math.floor(offset / PAGE_SIZE) + 1) })}</span>
        <button type="button" onClick={() => { setLoadState(null); setOffset((value) => value + PAGE_SIZE); }} disabled={!snapshot.has_more || loading} className="min-h-11 rounded-full border border-border px-4 text-sm disabled:opacity-40">{t("customer.work.next")}</button>
      </nav>}
      {snapshot && <p className="text-right text-[11px] text-muted">{t("admin.operations.observedAt", { date: date(snapshot.observed_at, { dateStyle: "medium", timeStyle: "short" }) })}</p>}
    </main>
  );
}
