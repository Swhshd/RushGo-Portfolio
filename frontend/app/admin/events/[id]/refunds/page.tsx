"use client";

import Link from "next/link";
import { useState } from "react";
import { ArrowLeft } from "lucide-react";
import { useParams } from "next/navigation";
import { AdminEventRefunds } from "@/components/admin-event-refunds";
import { LanguageSwitcher } from "@/components/language-switcher";
import { useAuth } from "@/lib/auth-context";
import { useI18n } from "@/lib/i18n/index";
import { isAdminEventOperationsId } from "@/lib/admin-event-operations";

export default function EventRefundApprovalsPage() {
  const params = useParams<{ id: string }>();
  const eventId = isAdminEventOperationsId(params?.id) ? params.id : null;
  const { user } = useAuth();
  const { t } = useI18n();
  const [copyStatus, setCopyStatus] = useState<"" | "copied" | "failed">("");

  async function copyApprovalLink() {
    if (!eventId) return;
    try {
      await navigator.clipboard.writeText(`${window.location.origin}/admin/events/${eventId}/refunds`);
      setCopyStatus("copied");
    } catch { setCopyStatus("failed"); }
  }

  return (
    <div className="mx-auto flex min-h-screen w-full max-w-5xl flex-col gap-4 p-3 sm:p-6">
      <header className="flex flex-wrap items-center gap-3 border-b border-border pb-4">
        <Link href={user?.role === "admin" ? `/admin/events/${eventId ?? ""}/operations` : "/aftersales"} className="inline-flex min-h-11 items-center gap-2 rounded-lg border border-border px-3 text-sm font-semibold">
          <ArrowLeft size={15} />{t(user?.role === "admin" ? "admin.refunds.back" : "aftersales.title")}
        </Link>
        <div className="min-w-0">
          <h1 className="break-words text-lg font-bold">{t("admin.refunds.title")}</h1>
          <p className="break-all font-mono text-xs text-muted">{eventId ?? t("admin.refunds.invalidEvent")}</p>
        </div>
        {user?.role !== "admin" ? <div className="ml-auto"><LanguageSwitcher /></div> : null}
      </header>
      {eventId && user?.role === "admin" ? <div>
        <button type="button" onClick={() => void copyApprovalLink()} className="min-h-11 rounded-lg border border-border px-4 text-sm font-semibold hover:bg-tag-bg">{t("aftersales.copyLink")}</button>
        {copyStatus ? <p role="status" className="mt-2 text-sm text-muted">{t(copyStatus === "copied" ? "aftersales.linkCopied" : "aftersales.copyFailed")}</p> : null}
      </div> : null}
      {eventId && user ? <AdminEventRefunds key={`${eventId}:${user?.id ?? "anonymous"}`} eventId={eventId} userId={user.id} isAdmin={user?.role === "admin"} /> : !eventId ? <p role="alert" className="rounded-lg border border-danger/30 bg-danger-bg p-4 text-sm text-danger-ink">{t("admin.refunds.invalidEvent")}</p> : null}
    </div>
  );
}
