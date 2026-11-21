"use client";

import Link from "next/link";
import { useState, useSyncExternalStore } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@/lib/auth-context";
import { useI18n } from "@/lib/i18n";
import { approvalEventId } from "@/lib/aftersales-navigation";

const action = "inline-flex min-h-11 items-center justify-center rounded-lg border border-border px-4 py-2 text-sm font-semibold hover:bg-tag-bg focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary";
const panel = "min-w-0 rounded-xl border border-border bg-card p-5 sm:p-6";
const subscribeToHydration = () => () => {};
const clientSnapshot = () => true;
const serverSnapshot = () => false;

export default function AftersalesPage() {
  const { t } = useI18n();
  const { user, isLoading } = useAuth();
  const hydrated = useSyncExternalStore(subscribeToHydration, clientSnapshot, serverSnapshot);
  const router = useRouter();
  const [eventId, setEventId] = useState("");
  const [invalid, setInvalid] = useState(false);
  return <div className="mx-auto w-full max-w-5xl space-y-6 px-5 py-8 sm:py-12">
    <header className="space-y-2">
      <h1 className="text-2xl font-bold sm:text-3xl">{t("aftersales.title")}</h1>
      <p className="max-w-2xl text-sm leading-relaxed text-muted">{t("aftersales.intro")}</p>
    </header>
    {!hydrated || isLoading ? <p role="status">{t("customer.wallet.sessionChecking")}</p> : !user ? <section className={panel}>
      <p className="mb-4 text-sm text-muted">{t("aftersales.signIn")}</p>
      <Link className={action} href="/login?redirect=%2Faftersales">{t("customer.navigation.signIn")}</Link>
    </section> : <div className="grid gap-4 md:grid-cols-2">
      <section className={panel}>
        <h2 className="text-lg font-semibold">{t("aftersales.orders")}</h2>
        <p className="mb-5 mt-2 text-sm leading-relaxed text-muted">{t("aftersales.ordersHelp")}</p>
        <Link href="/my-tickets#orders" className={action}>{t("aftersales.openOrders")}</Link>
      </section>
      <section className={panel}>
        <h2 className="text-lg font-semibold">{t("aftersales.approver")}</h2>
        <p className="mb-5 mt-2 text-sm leading-relaxed text-muted">{t("aftersales.approverHelp")}</p>
        <form className="space-y-3" onSubmit={event => {
          event.preventDefault();
          const id = approvalEventId(eventId, window.location.origin);
          if (!id) { setInvalid(true); return; }
          setInvalid(false);
          router.push(`/admin/events/${encodeURIComponent(id)}/refunds`);
        }}>
          <label className="block text-sm font-medium" htmlFor="aftersales-event-id">{t("aftersales.eventId")}</label>
          <input id="aftersales-event-id" value={eventId} onChange={event => { setEventId(event.target.value); setInvalid(false); }} autoComplete="off" spellCheck={false} maxLength={2048} aria-invalid={invalid} aria-describedby={invalid ? "aftersales-event-error" : undefined} className="min-h-11 w-full min-w-0 rounded-lg border border-border bg-background px-3 text-sm focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary" />
          {invalid ? <p role="alert" id="aftersales-event-error" className="text-sm text-danger-ink">{t("aftersales.invalidEvent")}</p> : null}
          <button type="submit" className={action}>{t("aftersales.openQueue")}</button>
        </form>
      </section>
      {user.role === "admin" ? <section className={`${panel} md:col-span-2`}>
        <h2 className="text-lg font-semibold">{t("aftersales.admin")}</h2>
        <p className="mb-5 mt-2 text-sm leading-relaxed text-muted">{t("aftersales.adminHelp")}</p>
        <Link href="/admin/events" className={action}>{t("aftersales.openEvents")}</Link>
      </section> : null}
    </div>}
  </div>;
}
