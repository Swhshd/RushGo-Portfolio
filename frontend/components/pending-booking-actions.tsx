"use client";

import { useState } from "react";
import Link from "next/link";
import { Loader2, RefreshCw } from "lucide-react";
import { apiClient } from "@/lib/api/client";
import type { ApiBooking } from "@/lib/api/types";
import { parseBookingResume, type ParsedBookingResume } from "@/lib/booking-resume";
import { useI18n } from "@/lib/i18n/index";

export function PendingBookingActions({
  bookingId,
  bookingStatus,
  onBookingChanged,
}: {
  bookingId: string;
  bookingStatus: string;
  onBookingChanged?: (booking: ApiBooking) => void;
}) {
  const { t, date } = useI18n();
  const [resume, setResume] = useState<ParsedBookingResume | null>(null);
  const [loading, setLoading] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [failed, setFailed] = useState(false);
  const [cancelled, setCancelled] = useState(false);

  if (bookingStatus.toUpperCase() !== "PENDING" || cancelled) {
    return cancelled ? <p role="status" className="text-sm text-muted">{t("customer.checkout.resumeCancelled")}</p> : null;
  }

  async function checkPaymentOptions() {
    if (loading || cancelling) return;
    setFailed(false);
    setLoading(true);
    try {
      const response = await apiClient.resumeBooking(bookingId);
      const checked = parseBookingResume(response, bookingId);
      setResume(checked);
      if (checked.booking) onBookingChanged?.({ ...checked.booking, paymentIntentClientSecret: undefined });
    } catch {
      setFailed(true);
      setResume(null);
    } finally {
      setLoading(false);
    }
  }

  async function cancelBooking() {
    if (!resume?.can_cancel || cancelling || loading) return;
    setCancelling(true);
    setFailed(false);
    try {
      const cancelledBooking = await apiClient.cancelBooking(bookingId);
      const safeBooking = { ...cancelledBooking, paymentIntentClientSecret: undefined };
      setResume({
        booking: safeBooking,
        checkout_state: "completed",
        reservation_deadline: resume.reservation_deadline,
        can_cancel: false,
      });
      setCancelled(true);
      onBookingChanged?.(safeBooking);
    } catch {
      setFailed(true);

    } finally {
      setCancelling(false);
    }
  }

  const paymentStateKey = resume
    ? resume.checkout_state === "ready"
      ? "customer.checkout.resumeReady"
      : resume.checkout_state === "processing"
        ? "customer.checkout.resumeProcessing"
        : resume.checkout_state === "expired"
          ? "customer.checkout.resumeExpired"
          : resume.checkout_state === "completed"
            ? resume.booking?.status.toUpperCase() === "CANCELLED"
              ? "customer.checkout.resumeCancelled"
              : "customer.checkout.resumeCompleted"
            : "customer.checkout.resumeUnavailable"
    : null;
  const checkoutHref = "/checkout?booking_id=" + encodeURIComponent(bookingId);

  return (
    <div className="mt-3 flex flex-col gap-2 rounded-lg border border-border bg-background/70 p-3">
      {loading && (
        <p role="status" className="flex items-center gap-2 text-sm text-muted">
          <Loader2 size={14} className="animate-spin" /> {t("customer.checkout.resumeChecking")}
        </p>
      )}
      {paymentStateKey && <p role="status" className="text-sm text-muted">{t(paymentStateKey)}</p>}
      {failed && <p role="alert" className="text-sm text-danger-ink">{t("customer.checkout.resumeUnavailable")}</p>}
      {resume?.reservation_deadline && (
        <p className="text-xs text-muted">
          {t("customer.checkout.resumeReservationDeadline", {
            date: date(resume.reservation_deadline, { dateStyle: "medium", timeStyle: "short" }),
          })}
        </p>
      )}
      <div className="flex flex-wrap gap-2">
        {!resume ? (
          <button type="button" onClick={checkPaymentOptions} disabled={loading} className="inline-flex min-h-10 items-center gap-2 rounded-full border border-border px-3 text-xs font-semibold disabled:opacity-60">
            {t("customer.checkout.resumeCheckOptions")}
          </button>
        ) : (
          <>
            {resume.checkout_state === "ready" && resume.booking?.paymentIntentClientSecret ? (
              <Link href={checkoutHref} className="inline-flex min-h-10 items-center rounded-full bg-primary px-3 text-xs font-semibold text-graphite">
                {t("customer.checkout.resumeContinue")}
              </Link>
            ) : null}
            {resume.checkout_state !== "expired" && resume.checkout_state !== "completed" ? (
              <button type="button" onClick={checkPaymentOptions} disabled={loading || cancelling} className="inline-flex min-h-10 items-center gap-2 rounded-full border border-border px-3 text-xs font-semibold disabled:opacity-60">
                <RefreshCw size={13} /> {t("customer.checkout.resumeRetry")}
              </button>
            ) : null}
            {resume.can_cancel ? (
              <button type="button" onClick={cancelBooking} disabled={loading || cancelling} className="inline-flex min-h-10 items-center gap-2 rounded-full border border-danger/40 px-3 text-xs font-semibold text-danger-ink disabled:opacity-60">
                {cancelling ? <Loader2 size={13} className="animate-spin" /> : null}
                {t("customer.checkout.resumeCancel")}
              </button>
            ) : null}
          </>
        )}
      </div>
    </div>
  );
}