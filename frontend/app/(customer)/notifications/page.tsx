"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { AlertCircle, Bell, Check, Loader2, RefreshCw } from "lucide-react";
import { useAuth } from "@/lib/auth-context";
import { apiClient } from "@/lib/api/client";
import type { ApiNotification, ApiNotificationPage } from "@/lib/api/types";
import { loginHref, safeAuthRedirect } from "@/lib/auth-navigation";
import { useI18n } from "@/lib/i18n";

const PAGE_SIZE = 20;

function errorText(error: unknown) {
  const status = typeof error === "object" && error !== null && "status" in error ? (error as { status?: number }).status : undefined;
  if (status === 401) return "Your session has expired. Please sign in again.";
  if (status === 403) return "You do not have access to these notifications.";
  return "Notifications could not be loaded. Please try again.";
}

export default function NotificationsPage() {
  const { t, date, number } = useI18n();
  const { user, isLoading: authLoading } = useAuth();
  const router = useRouter();
  const [page, setPage] = useState(1);
  const [unreadOnly, setUnreadOnly] = useState(false);
  const [loadState, setLoadState] = useState<{ key: string; page: ApiNotificationPage } | { key: string; error: string } | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [markingId, setMarkingId] = useState<string | null>(null);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const requestKey = `${user?.id ?? "guest"}:${page}:${unreadOnly ? "unread" : "all"}:${refreshNonce}`;
  const currentLoadState = loadState?.key === requestKey ? loadState : null;
  const snapshot = currentLoadState && "page" in currentLoadState ? currentLoadState.page : null;
  const loading = Boolean(user) && currentLoadState === null;
  const error = currentLoadState && "error" in currentLoadState ? currentLoadState.error : actionError;

  useEffect(() => {
    if (!authLoading && !user) router.replace(loginHref("/notifications", "AUTH_REQUIRED"));
  }, [authLoading, router, user]);

  useEffect(() => {
    if (!user) return;
    const controller = new AbortController();
    void apiClient.listMyNotifications(page, PAGE_SIZE, unreadOnly, controller.signal)
      .then((result) => {
        if (!controller.signal.aborted) setLoadState({ key: requestKey, page: result });
      })
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) setLoadState({ key: requestKey, error: errorText(reason) });
      });
    return () => controller.abort();
  }, [page, refreshNonce, requestKey, unreadOnly, user]);

  async function markRead(notification: ApiNotification) {
    if (notification.read_at || markingId) return;
    setMarkingId(notification.id);
    setActionError(null);
    try {
      const result = await apiClient.markNotificationRead(notification.id);
      setLoadState((current) => {
        if (!current || current.key !== requestKey || !("page" in current)) return current;
        const currentSnapshot = current.page;
        const notifications = unreadOnly
          ? currentSnapshot.notifications.filter((item) => item.id !== notification.id)
          : currentSnapshot.notifications.map((item) => item.id === result.id ? { ...item, read_at: result.read_at } : item);
        return { key: current.key, page: {
          ...currentSnapshot,
          notifications,
          total: currentSnapshot.total > 0 && unreadOnly ? currentSnapshot.total - 1 : currentSnapshot.total,
          unread_count: Math.max(0, currentSnapshot.unread_count - 1),
        } };
      });
      if (unreadOnly && snapshot?.notifications.length === 1 && page > 1) {
        setLoadState(null);
        setPage((current) => current - 1);
      }
    } catch (reason) {
      setActionError(errorText(reason));
    } finally {
      setMarkingId(null);
    }
  }

  const totalPages = snapshot ? Math.max(1, Math.ceil(snapshot.total / PAGE_SIZE)) : 1;
  const lastPage = Math.max(1, totalPages);

  if (authLoading || !user) {
    return <main role="status" className="mx-auto flex min-h-72 max-w-3xl items-center justify-center gap-3 px-4 text-sm text-muted"><Loader2 size={20} className="animate-spin text-primary" />{t("common.loading")}</main>;
  }

  return (
    <main className="mx-auto flex w-full max-w-4xl flex-col gap-5 px-4 py-6 sm:py-9">
      <header className="flex flex-wrap items-start justify-between gap-3">
        <div><h1 className="text-2xl font-bold tracking-tight">{t("customer.notifications.title")}</h1><p className="mt-1 text-sm text-muted">{t("customer.notifications.description")}</p></div>
        <button type="button" onClick={() => { setActionError(null); setLoadState(null); setRefreshNonce((value) => value + 1); }} disabled={loading} className="inline-flex min-h-11 items-center gap-2 rounded-full border border-border px-4 text-sm font-semibold disabled:opacity-50"><RefreshCw size={15} className={loading ? "animate-spin" : ""} />{t("customer.notifications.refresh")}</button>
      </header>

      {snapshot && <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-border bg-card px-4 py-3">
        <label className="inline-flex min-h-11 items-center gap-2 text-sm"><input type="checkbox" checked={unreadOnly} onChange={(event) => { setActionError(null); setLoadState(null); setUnreadOnly(event.target.checked); setPage(1); }} />{t("customer.notifications.unreadOnly")}</label>
        <span className="text-xs text-muted">{t("customer.notifications.unreadCount", { count: number(snapshot.unread_count) })}</span>
      </div>}

      {error && <p role="alert" className="flex items-center gap-2 rounded-xl border border-danger/30 bg-danger-bg px-4 py-3 text-sm text-danger-ink"><AlertCircle size={17} />{error}</p>}
      {loading && !snapshot && <div role="status" className="flex min-h-40 items-center justify-center gap-3 text-sm text-muted"><Loader2 size={20} className="animate-spin text-primary" />{t("common.loading")}</div>}
      {snapshot && !loading && !error && snapshot.notifications.length === 0 && <div className="rounded-2xl border border-dashed border-border bg-card px-5 py-12 text-center"><Bell className="mx-auto mb-3 text-muted" size={25} /><p className="text-sm text-muted">{t("customer.notifications.empty")}</p></div>}

      {snapshot && snapshot.notifications.length > 0 && <section aria-label={t("customer.notifications.title")} className="flex flex-col gap-3">
        {snapshot.notifications.map((notification) => {
          const destination = safeAuthRedirect(notification.detail_path, "/my-tickets");
          return <article key={notification.id} className={`rounded-2xl border bg-card p-4 sm:p-5 ${notification.read_at ? "border-border" : "border-primary/40"}`}>
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0"><h2 className="break-words text-sm font-semibold">{notification.title}</h2><p className="mt-1 whitespace-pre-wrap break-words text-sm leading-6 text-muted">{notification.body}</p></div>
              {!notification.read_at && <span className="mt-1 h-2.5 w-2.5 shrink-0 rounded-full bg-primary" aria-label={t("customer.notifications.unreadOnly")} />}
            </div>
            <div className="mt-4 flex flex-wrap items-center justify-between gap-3 border-t border-border pt-3">
              <time className="text-xs text-muted" dateTime={notification.created_at}>{date(notification.created_at, { dateStyle: "medium", timeStyle: "short" })}</time>
              <div className="flex flex-wrap items-center gap-2">
                {destination.startsWith("/") && <Link href={destination} className="inline-flex min-h-10 items-center rounded-full border border-border px-3 text-xs font-semibold">{t(destination.startsWith("/events/") ? "customer.interests.openEvent" : "customer.notifications.openOrder")}</Link>}
                {notification.read_at ? <span className="inline-flex min-h-10 items-center gap-1 text-xs text-muted"><Check size={14} />{t("customer.notifications.read")}</span> : <button type="button" onClick={() => void markRead(notification)} disabled={markingId !== null} className="inline-flex min-h-10 items-center rounded-full px-3 text-xs font-semibold text-primary disabled:opacity-50">{markingId === notification.id ? <Loader2 size={14} className="animate-spin" /> : t("customer.notifications.markRead")}</button>}
              </div>
            </div>
          </article>;
        })}
      </section>}

      {snapshot && snapshot.total > 0 && <nav aria-label={t("customer.notifications.title")} className="flex items-center justify-between gap-3">
        <button type="button" onClick={() => { setActionError(null); setLoadState(null); setPage((value) => Math.max(1, value - 1)); }} disabled={page <= 1 || loading} className="min-h-11 rounded-full border border-border px-4 text-sm disabled:opacity-40">{t("customer.notifications.previous")}</button>
        <span className="text-xs text-muted">{t("customer.notifications.page", { page: number(page), pages: number(totalPages) })}</span>
        <button type="button" onClick={() => { setActionError(null); setLoadState(null); setPage((value) => Math.min(lastPage, value + 1)); }} disabled={page >= lastPage || loading} className="min-h-11 rounded-full border border-border px-4 text-sm disabled:opacity-40">{t("customer.notifications.next")}</button>
      </nav>}
    </main>
  );
}
