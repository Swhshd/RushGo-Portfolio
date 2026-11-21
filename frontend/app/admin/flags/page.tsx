"use client";

import { useEffect, useState } from "react";
import { RefreshCw } from "lucide-react";
import { apiClient } from "@/lib/api/client";
import type { ApiAdminFeatureFlagsResponse, ApiFeatureFlag } from "@/lib/api/types";
import { isAllowedAdminFeatureFlag, updateFeatureFlagWithConflictReload } from "@/lib/admin-feature-flags";
import { useI18n } from "@/lib/i18n";

export default function AdminFeatureFlagsPage() {
  const { t, date } = useI18n();
  const [data, setData] = useState<ApiAdminFeatureFlagsResponse | null>(null);
  const [dataKey, setDataKey] = useState<string | null>(null);
  const [loadErrorKey, setLoadErrorKey] = useState<string | null>(null);
  const [changeError, setChangeError] = useState(false);
  const [conflict, setConflict] = useState(false);
  const [saving, setSaving] = useState(false);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const requestKey = String(refreshNonce);
  const currentData = dataKey === requestKey ? data : null;
  const loading = !currentData && loadErrorKey !== requestKey;
  const loadError = loadErrorKey === requestKey;

  useEffect(() => {
    const controller = new AbortController();
    void apiClient.getAdminFeatureFlags(controller.signal)
      .then((response) => { if (!controller.signal.aborted) { setData(response); setDataKey(requestKey); setLoadErrorKey(null); } })
      .catch(() => { if (!controller.signal.aborted) setLoadErrorKey(requestKey); });
    return () => controller.abort();
  }, [requestKey]);

  const flag = currentData?.flags.find((item) => isAllowedAdminFeatureFlag(item.key));
  const audit = (currentData?.audit ?? []).filter((item) => isAllowedAdminFeatureFlag(item.key));

  async function toggleFlag(current: ApiFeatureFlag) {
    if (saving || !isAllowedAdminFeatureFlag(current.key)) return;
    setSaving(true);
    setChangeError(false);
    setConflict(false);
    try {
      const result = await updateFeatureFlagWithConflictReload(
        current,
        (key, enabled, expectedVersion) => apiClient.updateAdminFeatureFlag(key, enabled, expectedVersion),
        () => apiClient.getAdminFeatureFlags(),
      );
      if (result.state === "conflict") {
        setConflict(true);
        if (result.latest) { setData(result.latest); setDataKey(requestKey); }
        else setLoadErrorKey(requestKey);
      } else setRefreshNonce((value) => value + 1);
    } catch {
      setChangeError(true);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="flex min-h-screen min-w-0 flex-col">
      <header className="sticky top-14 z-10 flex min-h-14 items-center justify-between gap-3 border-b border-border bg-card px-3 py-2 sm:px-6 lg:top-0">
        <div className="flex min-w-0 items-center gap-2 text-sm"><span className="text-muted">{t("admin.nav.operations")}</span><span className="text-muted">/</span><strong className="truncate">{t("admin.flags.title")}</strong></div>
        <button type="button" onClick={() => setRefreshNonce((value) => value + 1)} disabled={loading || saving} className="inline-flex min-h-10 shrink-0 items-center gap-2 rounded-full border border-border px-3 text-xs font-semibold disabled:opacity-50"><RefreshCw size={14} />{t("admin.flags.refresh")}</button>
      </header>
      <main className="mx-auto flex w-full max-w-[1100px] min-w-0 flex-col gap-5 p-3 sm:p-6">
        <div><h1 className="text-[22px] font-bold tracking-tight">{t("admin.flags.title")}</h1><p className="mt-1 max-w-3xl text-[13px] leading-5 text-muted">{t("admin.flags.description")}</p></div>

        {loading && !currentData && <p role="status" className="rounded-xl border border-border bg-card p-5 text-sm text-muted">{t("admin.flags.loading")}</p>}
        {loadError && <div role="alert" className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-danger/30 bg-danger-bg p-4 text-sm text-danger-ink"><span>{t("admin.flags.loadError")}</span><button type="button" onClick={() => setRefreshNonce((value) => value + 1)} className="min-h-10 rounded-full border border-danger/30 px-4 font-semibold">{t("admin.flags.retry")}</button></div>}
        {conflict && <p role="alert" className="rounded-xl border border-primary/30 bg-tag-bg p-4 text-sm text-foreground">{t("admin.flags.conflict")}</p>}
        {changeError && <p role="alert" className="rounded-xl border border-danger/30 bg-danger-bg p-4 text-sm text-danger-ink">{t("admin.flags.changeError")}</p>}

        {currentData && <>
          <section className="rounded-xl border border-border bg-card p-4 sm:p-5">
            <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted">
              <span className="break-all">{t("admin.flags.source", { source: currentData.source })}</span>
              <span>{t("admin.flags.observedAt", { date: date(currentData.observed_at, { dateStyle: "medium", timeStyle: "short" }) })}</span>
            </div>
            <div className="mt-4 flex flex-col gap-4">
              {flag ? <article className="flex min-w-0 flex-col gap-4 rounded-xl border border-border bg-background p-4 sm:flex-row sm:items-center sm:justify-between sm:p-5">
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2"><h2 className="break-all font-mono text-sm font-semibold">{flag.key}</h2><span className="rounded-full bg-tag-bg px-2 py-1 text-[10px] font-medium text-muted">{t("admin.flags.allowlisted")}</span></div>
                  <p className="mt-2 text-sm leading-5 text-muted">{t("admin.flags.discoveryDescription")}</p>
                  <div className="mt-3 flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-muted">
                    <span>{t("admin.flags.defaultValue", { value: flag.default_enabled ? t("admin.flags.enabled") : t("admin.flags.disabled") })}</span>
                    <span>{t("admin.flags.version", { version: flag.version })}</span>
                    {flag.updated_at && <span>{t("admin.flags.updatedAt", { date: date(flag.updated_at, { dateStyle: "short", timeStyle: "short" }) })}</span>}
                  </div>
                </div>
                <button type="button" role="switch" aria-checked={flag.enabled} aria-label={t("admin.flags.discoveryDescription")} disabled={saving || loading} onClick={() => void toggleFlag(flag)} className={`inline-flex min-h-11 shrink-0 items-center justify-center rounded-full px-5 text-sm font-semibold disabled:opacity-50 ${flag.enabled ? "bg-primary text-graphite" : "border border-border bg-card text-foreground"}`}>
                  {saving ? t("common.loading") : flag.enabled ? t("admin.flags.enabled") : t("admin.flags.disabled")}
                </button>
              </article> : <p role="status" className="rounded-lg border border-dashed border-border p-4 text-sm text-muted">{t("admin.flags.loadError")}</p>}
            </div>
          </section>

          <section className="flex min-w-0 flex-col gap-3">
            <div className="flex flex-wrap items-end justify-between gap-2"><div><h2 className="text-base font-semibold">{t("admin.flags.auditHeading")}</h2><p className="mt-1 text-xs text-muted">{t("admin.flags.auditLimit", { count: currentData.audit_limit })}</p></div></div>
            {audit.length === 0 ? <p className="rounded-xl border border-dashed border-border bg-card p-5 text-center text-sm text-muted">{t("admin.flags.auditEmpty")}</p> : audit.map((entry, index) => <article key={`${entry.key}:${entry.version}:${entry.changed_at}:${index}`} className="grid min-w-0 grid-cols-1 gap-2 rounded-xl border border-border bg-card p-4 text-xs sm:grid-cols-2">
              <p className="break-all font-mono font-semibold sm:col-span-2">{entry.key} · {t("admin.flags.version", { version: entry.version })}</p>
              <p><span className="text-muted">{t("admin.flags.before")}: </span>{entry.before ? t("admin.flags.enabled") : t("admin.flags.disabled")}</p>
              <p><span className="text-muted">{t("admin.flags.after")}: </span>{entry.after ? t("admin.flags.enabled") : t("admin.flags.disabled")}</p>
              <p className="break-all"><span className="text-muted">{t("admin.flags.actor", { actor: entry.actor_id || "—" })}</span></p>
              <time className="text-muted" dateTime={entry.changed_at}>{t("admin.flags.changedAt")}: {date(entry.changed_at, { dateStyle: "medium", timeStyle: "short" })}</time>
            </article>)}
          </section>
        </>}
      </main>
    </div>
  );
}
