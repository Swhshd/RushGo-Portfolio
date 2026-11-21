"use client";

import { useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { ArrowLeft, Search } from "lucide-react";
import { useAuth } from "@/lib/auth-context";
import { isAdminEventOperationsId } from "@/lib/admin-event-operations";
import { useI18n } from "@/lib/i18n";

export default function AdminOrderLookupPage() {
  const { t } = useI18n();
  const router = useRouter();
  const { user, isLoading } = useAuth();
  const [bookingId, setBookingId] = useState("");
  const [error, setError] = useState(false);

  function submit(event: React.FormEvent) {
    event.preventDefault();
    const value = bookingId.trim();
    if (!isAdminEventOperationsId(value)) {
      setError(true);
      return;
    }
    setError(false);
    router.push(`/admin/orders/${value}`);
  }

  return (
    <main className="mx-auto flex max-w-3xl flex-col gap-5 px-5 py-8 md:px-8">
      <div>
        <Link href="/admin/events" className="inline-flex items-center gap-1.5 text-xs font-medium text-muted hover:text-foreground">
          <ArrowLeft size={13} /> {t("admin.orderLookup.backToEvents")}
        </Link>
        <h1 className="mt-2 text-2xl font-semibold tracking-tight">{t("admin.orderLookup.title")}</h1>
        <p className="mt-1 max-w-[640px] text-sm leading-relaxed text-muted">{t("admin.orderLookup.subtitle")}</p>
      </div>

      <form onSubmit={submit} className="rounded-xl border border-border bg-card p-4 sm:p-5">
        <label htmlFor="admin-booking-id" className="text-sm font-semibold">{t("admin.orderLookup.label")}</label>
        <p className="mt-1 text-xs text-muted">{t("admin.orderLookup.help")}</p>
        <div className="mt-3 flex flex-col gap-2 sm:flex-row">
          <input
            id="admin-booking-id"
            value={bookingId}
            onChange={(event) => { setBookingId(event.target.value); setError(false); }}
            autoComplete="off"
            spellCheck={false}
            placeholder={t("admin.orderLookup.placeholder")}
            className="min-h-11 min-w-0 flex-1 rounded-lg border border-control-border bg-background px-3 font-mono text-sm outline-none focus-visible:ring-2 focus-visible:ring-primary"
          />
          <button type="submit" disabled={isLoading || user?.role !== "admin"} className="inline-flex min-h-11 items-center justify-center gap-2 rounded-lg bg-primary px-5 text-sm font-semibold text-graphite disabled:opacity-50">
            <Search size={15} /> {t("admin.orderLookup.search")}
          </button>
        </div>
        {error ? <p role="alert" className="mt-2 text-xs text-danger-ink">{t("admin.orderLookup.invalidId")}</p> : null}
        <p className="mt-3 border-t border-border pt-3 text-[11px] leading-relaxed text-muted">{t("admin.orderLookup.scopeNote")}</p>
      </form>
    </main>
  );
}
