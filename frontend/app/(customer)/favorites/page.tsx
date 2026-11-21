"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { Bell, Heart, Loader2 } from "lucide-react";
import { useAuth } from "@/lib/auth-context";
import { loginHref } from "@/lib/auth-navigation";
import { apiClient } from "@/lib/api/client";
import type { ApiEvent } from "@/lib/api/types";
import { eventInterestsApi, type EventSubscription, type FavoriteEvent, type InterestPage } from "@/lib/api/event-interests";
import { interestErrorKey } from "@/components/event-interests";
import { useI18n, type TranslationKey } from "@/lib/i18n";

type Row = { item: FavoriteEvent | EventSubscription; event: ApiEvent | null };
type View = "favorites" | "subscriptions";
export default function FavoritesPage() {
  const { t, date } = useI18n();
  const { user, isLoading } = useAuth();
  const router = useRouter();
  const [view, setView] = useState<View>("favorites");
  const [offset, setOffset] = useState(0);
  const [nonce, setNonce] = useState(0);
  const key = `${user?.id}:${view}:${offset}:${nonce}`;
  const current = useRef(key);
  useEffect(() => { current.current = key; }, [key]);
  const [load, setLoad] = useState<{ key: string; rows?: Row[]; hasMore?: boolean; error?: TranslationKey } | null>(null);
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<{ key: string; message: TranslationKey } | null>(null);
  const snapshot = load?.key === key ? load : null;
  useEffect(() => { if (!isLoading && !user) router.replace(loginHref("/favorites", "AUTH_REQUIRED")); }, [isLoading, router, user]);
  useEffect(() => {
    if (!user) return;
    const controller = new AbortController();
    const request: Promise<InterestPage<FavoriteEvent | EventSubscription>> = view === "favorites" ? eventInterestsApi.favorites(offset, controller.signal) : eventInterestsApi.subscriptions(offset, controller.signal);
    void request.then(async page => {
      const ids = [...new Set(page.items.map(item => item.eventId))];
      const events = new Map(await Promise.all(ids.map(async id => {
        try { return [id, await apiClient.getEvent(id)] as const; } catch { return [id, null] as const; }
      })));
      if (!controller.signal.aborted) setLoad({ key, rows: page.items.map(item => ({ item, event: events.get(item.eventId) ?? null })), hasMore: page.hasMore });
    }).catch(error => { if (!controller.signal.aborted) setLoad({ key, error: interestErrorKey(error) }); });
    return () => controller.abort();
  }, [key, offset, user, view]);

  async function change(action: () => Promise<unknown>) {
    if (busy) return;
    const requestKey = key;
    setBusy(true);
    setActionError(null);
    try { await action(); if (current.current === requestKey) setNonce(n => n + 1); }
    catch (error) { if (current.current === requestKey) setActionError({ key: requestKey, message: interestErrorKey(error) }); }
    finally { setBusy(false); }
  }
  if (isLoading || !user) return <div role="status" className="flex min-h-64 items-center justify-center"><Loader2 className="animate-spin text-primary" aria-label={t("customer.interests.loading")} /></div>;
  return <main className="mx-auto w-full max-w-5xl px-5 py-8 md:px-10">
    <h1 className="text-2xl font-bold">{t("customer.interests.manage")}</h1>
    <p className="mt-2 text-sm leading-6 text-muted">{t("customer.interests.noReservation")}</p>
    <div className="mt-5 flex flex-wrap gap-2" role="group" aria-label={t("customer.interests.manage")}>
      {(["favorites", "subscriptions"] as View[]).map(v => <button key={v} type="button" aria-pressed={view === v} disabled={busy} onClick={() => { setView(v); setOffset(0); }} className={`inline-flex min-h-11 items-center gap-2 rounded-full border px-4 text-sm ${view === v ? "border-primary text-primary" : "border-border"}`}>{v === "favorites" ? <Heart size={16} /> : <Bell size={16} />}{t(v === "favorites" ? "customer.interests.myFavorites" : "customer.interests.mySubscriptions")}</button>)}
      <button type="button" disabled={busy} onClick={() => setNonce(n => n + 1)} className="min-h-11 rounded-full border border-border px-4 text-sm">{t("customer.interests.retry")}</button>
    </div>
    {view === "subscriptions" && <div className="mt-4 flex flex-wrap gap-2">{(["onsale", "restock"] as const).map(kind => <button key={kind} type="button" disabled={busy || !snapshot?.rows} onClick={() => void change(() => eventInterestsApi.disableCategoryEmails(kind))} className="min-h-11 rounded-full border border-border px-4 text-xs disabled:opacity-50">{t(kind === "onsale" ? "customer.interests.disableOnsaleEmails" : "customer.interests.disableRestockEmails")}</button>)}</div>}
    {actionError?.key === key && <p role="alert" className="mt-4 text-sm text-danger-ink">{t(actionError.message)}</p>}
    {!snapshot ? <p role="status" className="mt-6 text-muted">{t("customer.interests.loading")}</p> : snapshot.error ? <p role="alert" className="mt-6 text-danger-ink">{t(snapshot.error)}</p> : <>
      {snapshot.rows?.length === 0 && <p className="mt-6 rounded-xl border border-border p-6 text-muted">{t("customer.interests.empty")}</p>}
      <div className="mt-5 space-y-4">{snapshot.rows?.map(({ item, event }) => {
        const subscription = "kind" in item ? item : null;
        return <article key={`${item.eventId}:${subscription?.kind ?? "favorite"}`} className="min-w-0 rounded-xl border border-border bg-card p-5">
          <h2 className="break-words font-semibold">{event ? event.title : t("customer.interests.eventUnavailable")}</h2>
          {event && <p className="mt-1 break-words text-xs text-muted">{date(event.date, { dateStyle: "medium" })} · {event.venue}</p>}
          {subscription && <><p className="mt-2 text-sm">{t(subscription.kind === "onsale" ? "customer.interests.onsale" : "customer.interests.restock")} · {t(subscription.emailOptIn ? "customer.interests.inAppAndEmail" : "customer.interests.inAppOnly")}</p>{subscription.status === "triggered" && <p className="mt-1 text-xs text-muted">{t("customer.interests.triggered")}</p>}{subscription.status === "collection_pending" && <p className="mt-1 text-xs text-muted">{t("customer.interests.collectionPending")}</p>}</>}
          <div className="mt-3 flex flex-wrap items-center gap-2">
            {event && <Link href={`/events/${encodeURIComponent(event.id)}`} className="inline-flex min-h-11 items-center rounded-full border border-border px-4 text-sm">{t("customer.interests.openEvent")}</Link>}
            <button type="button" disabled={busy} onClick={() => void change(() => subscription ? eventInterestsApi.unsubscribe(item.eventId, subscription.kind) : eventInterestsApi.favorite(item.eventId, false))} className="min-h-11 rounded-full border border-border px-4 text-sm disabled:opacity-50">{t(subscription ? "customer.interests.cancel" : "customer.interests.unfavorite")}</button>
            {subscription && <button type="button" disabled={busy} onClick={() => void change(() => eventInterestsApi.subscribe(item.eventId, subscription.kind, !subscription.emailOptIn))} className="min-h-11 px-2 text-sm text-primary underline disabled:opacity-50">{t(subscription.emailOptIn ? "customer.interests.disableEmail" : "customer.interests.enableEmail")}</button>}
          </div>
        </article>;
      })}</div>
      <div className="mt-5 flex gap-3"><button type="button" disabled={busy || offset === 0} onClick={() => setOffset(n => Math.max(0, n - 20))} className="min-h-11 rounded-full border border-border px-4 text-sm disabled:opacity-40">{t("customer.interests.previous")}</button><button type="button" disabled={busy || !snapshot.hasMore} onClick={() => setOffset(n => n + 20)} className="min-h-11 rounded-full border border-border px-4 text-sm disabled:opacity-40">{t("customer.interests.next")}</button></div>
    </>}
    <p className="mt-5 text-xs leading-5 text-muted">{t("customer.interests.withdrawalNote")}</p>
  </main>;
}
