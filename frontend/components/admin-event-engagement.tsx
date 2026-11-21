"use client";

import { useEffect, useState } from "react";
import { useAuth } from "@/lib/auth-context";
import { useI18n } from "@/lib/i18n";
import { eventInterestsApi, type EngagementCounts } from "@/lib/api/event-interests";

export function AdminEventEngagement({ eventId }: { eventId: string }) {
  const { user } = useAuth();
  const { t, number } = useI18n();
  const key = `${user?.id}:${eventId}`;
  const [nonce, setNonce] = useState(0);
  const [state, setState] = useState<{ key: string; counts?: EngagementCounts; failed?: boolean } | null>(null);
  const snapshot = state?.key === key ? state : null;
  useEffect(() => {
    if (user?.role !== "admin") return;
    const controller = new AbortController();
    void eventInterestsApi.engagement(eventId, controller.signal).then(counts => {
      if (!controller.signal.aborted) setState({ key, counts });
    }).catch(() => { if (!controller.signal.aborted) setState({ key, failed: true }); });
    return () => controller.abort();
  }, [eventId, key, nonce, user]);
  if (user?.role !== "admin") return null;
  return <section className="min-w-0 rounded-xl border border-border bg-card p-5">
    <div className="flex flex-wrap items-center justify-between gap-3"><h2 className="text-sm font-semibold">{t("customer.interests.adminSummary")}</h2><button type="button" onClick={() => setNonce(n => n + 1)} className="min-h-11 rounded-full border border-border px-4 text-xs">{t("customer.interests.retry")}</button></div>
    {snapshot?.failed ? <p role="status" className="mt-2 text-sm text-muted">{t("customer.interests.summaryUnavailable")}</p> : !snapshot?.counts ? <p role="status" className="mt-2 text-sm text-muted">{t("customer.interests.loading")}</p> : <dl className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-3">{(["favorites", "onsale", "restock"] as const).map(field => <div key={field}><dt className="text-xs text-muted">{t(field === "favorites" ? "customer.interests.myFavorites" : field === "onsale" ? "customer.interests.onsale" : "customer.interests.restock")}</dt><dd className="mt-1 text-xl font-bold tabular-nums">{number(snapshot.counts![field])}</dd></div>)}</dl>}
  </section>;
}
