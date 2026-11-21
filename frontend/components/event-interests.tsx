"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { Bell, Heart, Loader2 } from "lucide-react";
import { useAuth } from "@/lib/auth-context";
import { loginHref } from "@/lib/auth-navigation";
import { useI18n, type TranslationKey } from "@/lib/i18n";
import { eventInterestsApi, type AlertKind, type EventInterests } from "@/lib/api/event-interests";

export function interestErrorKey(error: unknown): TranslationKey {
  const status = typeof error === "object" && error !== null && "status" in error ? (error as { status?: number }).status : undefined;
  if (status === 401) return "customer.interests.session";
  if (status === 409) return "customer.interests.conflict";
  return "customer.interests.error";
}

export function EventInterestsPanel({ eventId }: { eventId: string }) {
  const { t } = useI18n();
  const { user, isLoading } = useAuth();
  const identity = `${user?.id ?? "guest"}:${eventId}`;
  const current = useRef(identity);
  useEffect(() => { current.current = identity; }, [identity]);
  const [nonce, setNonce] = useState(0);
  const [snapshot, setSnapshot] = useState<{ key: string; data?: EventInterests; error?: TranslationKey } | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [actionError, setActionError] = useState<{ key: string; message: TranslationKey } | null>(null);
  const [emailChoice, setEmailChoice] = useState<{ key: string; value: boolean } | null>(null);
  const email = emailChoice?.key === identity ? emailChoice.value : false;
  const state = snapshot?.key === identity ? snapshot : null;

  useEffect(() => {
    if (!user) return;
    const controller = new AbortController();
    void eventInterestsApi.state(eventId, controller.signal).then(data => {
      if (!controller.signal.aborted) setSnapshot({ key: identity, data });
    }).catch(error => {
      if (!controller.signal.aborted) setSnapshot({ key: identity, error: interestErrorKey(error) });
    });
    return () => controller.abort();
  }, [eventId, identity, nonce, user]);

  async function change(name: string, action: () => Promise<unknown>) {
    if (busy || !state?.data) return;
    const key = identity;
    setBusy(name);
    setActionError(null);
    try {
      await action();
      const data = await eventInterestsApi.state(eventId);
      if (current.current === key) setSnapshot({ key, data });
    } catch (error) {
      if (current.current === key) setActionError({ key, message: interestErrorKey(error) });
    } finally { setBusy(null); }
  }

  if (isLoading) return null;
  return <section aria-label={t("customer.interests.title")} className="min-w-0 rounded-xl border border-border bg-card p-5">
    <div className="flex flex-wrap items-center justify-between gap-3"><h2 className="font-semibold">{t("customer.interests.title")}</h2><Link href="/favorites" className="text-sm text-primary underline underline-offset-4">{t("customer.interests.manage")}</Link></div>
    {!user ? <Link href={loginHref(`/events/${eventId}`, "AUTH_REQUIRED")} className="mt-3 inline-flex min-h-11 items-center text-sm text-primary">{t("customer.interests.login")}</Link> : !state ? <p role="status" className="mt-3 flex items-center gap-2 text-sm text-muted"><Loader2 size={16} className="animate-spin" />{t("customer.interests.loading")}</p> : state.error ? <div role="alert" className="mt-3 text-sm"><p>{t(state.error)}</p><button type="button" onClick={() => setNonce(n => n + 1)} className="min-h-11 text-primary">{t("customer.interests.retry")}</button></div> : state.data ? <div className="mt-4 space-y-4">
      <button type="button" disabled={busy !== null} aria-pressed={state.data.favorite} onClick={() => void change("favorite", () => eventInterestsApi.favorite(eventId, !state.data!.favorite))} className="inline-flex min-h-11 items-center gap-2 rounded-full border border-border px-4 text-sm disabled:opacity-50"><Heart size={16} fill={state.data.favorite ? "currentColor" : "none"} />{t(state.data.favorite ? "customer.interests.unfavorite" : "customer.interests.favorite")}</button>
      <label className="flex items-start gap-3 text-sm leading-6"><input type="checkbox" checked={email} onChange={e => setEmailChoice({ key: identity, value: e.target.checked })} className="mt-1 h-4 w-4 shrink-0" />{t("customer.interests.emailConsent")}</label>
      {(["onsale", "restock"] as AlertKind[]).map(kind => {
        const subscription = state.data!.subscriptions.find(s => s.kind === kind && s.status !== "withdrawn" && s.status !== "cancelled");
        const available = kind !== "onsale" || state.data!.onsaleAvailable || Boolean(subscription);
        return <div key={kind} className="border-t border-border pt-3">
          <h3 className="flex items-center gap-2 text-sm font-semibold"><Bell size={15} />{t(kind === "onsale" ? "customer.interests.onsale" : "customer.interests.restock")}</h3>
          <p className="mt-1 text-xs leading-5 text-muted">{t(kind === "onsale" ? "customer.interests.onsaleNote" : "customer.interests.restockNote")}</p>
          {!available && <p className="mt-1 text-xs text-muted">{t("customer.interests.alreadyOnSale")}</p>}
          {(kind === "restock" && (state.data!.restockStatus === "collection_pending" || subscription?.status === "collection_pending")) && <p className="mt-1 text-xs text-muted">{t("customer.interests.collectionPending")}</p>}
          {subscription?.status === "triggered" && <p className="mt-1 text-xs text-muted">{t("customer.interests.triggered")}</p>}
          <div className="mt-2 flex flex-wrap items-center gap-2">
            <button type="button" disabled={busy !== null || !available} onClick={() => void change(kind, () => subscription ? eventInterestsApi.unsubscribe(eventId, kind) : eventInterestsApi.subscribe(eventId, kind, email))} className="min-h-11 rounded-full border border-border px-4 text-sm disabled:opacity-50">{t(subscription ? "customer.interests.cancel" : "customer.interests.subscribe")}</button>
            {subscription && <><span className="text-xs text-muted">{t(subscription.emailOptIn ? "customer.interests.inAppAndEmail" : "customer.interests.inAppOnly")}</span><button type="button" disabled={busy !== null} onClick={() => void change(`${kind}:email`, () => eventInterestsApi.subscribe(eventId, kind, !subscription.emailOptIn))} className="min-h-11 px-2 text-xs text-primary underline disabled:opacity-50">{t(subscription.emailOptIn ? "customer.interests.disableEmail" : "customer.interests.enableEmail")}</button></>}
          </div>
        </div>;
      })}
      {busy && <p role="status" className="text-xs text-muted">{t("customer.interests.saving")}</p>}
      {actionError?.key === identity && <p role="alert" className="text-sm text-danger-ink">{t(actionError.message)}</p>}
    </div> : null}
    <p className="mt-4 text-xs leading-5 text-muted">{t("customer.interests.noReservation")}</p>
  </section>;
}
