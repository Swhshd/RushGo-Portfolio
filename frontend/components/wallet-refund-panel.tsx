"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { Loader2, RefreshCw, RotateCcw } from "lucide-react";
import { apiClient } from "@/lib/api/client";
import type { ApiBookingRefund } from "@/lib/api/types";
import { ApiError } from "@/lib/api/errors";
import { useI18n, type TranslationKey } from "@/lib/i18n/index";
import { formatUsdCents } from "@/lib/wallet";
import { buyerRefundEntryAccess, canSubmitBuyerRefundRequest, type BuyerRefundEntryAccess } from "@/lib/booking-refunds";
import { refundDecisionReason, unapprovedRefundNotice, type RefundOrderSummaryUpdate } from "@/lib/refund-presentation";

type SavedRequest = { key: string; reason: string };

function savedRequestKey(userId: string, bookingId: string) {
  return `rushgo.refund-request.${userId}.${bookingId}`;
}

function legacySavedRequestKey(bookingId: string) {
  return `rushgo.refund-request.${bookingId}`;
}

function withdrawRequestKey(userId: string, bookingId: string, requestId: string) {
  return `rushgo.refund-withdraw.${userId}.${bookingId}.${requestId}`;
}

function legacyWithdrawRequestKey(bookingId: string, requestId: string) {
  return `rushgo.refund-withdraw.${bookingId}.${requestId}`;
}

function readStored(key: string): string | null {
  try { const value = localStorage.getItem(key); if (value) return value; } catch { /* Try tab storage. */ }
  try { return sessionStorage.getItem(key); } catch { return null; }
}

function writeStored(key: string, value: string): boolean {
  try { localStorage.setItem(key, value); return true; } catch { /* Try tab storage. */ }
  try { sessionStorage.setItem(key, value); return true; } catch { return false; }
}

function removeStored(key: string) {
  try { localStorage.removeItem(key); } catch { /* Continue to tab storage. */ }
  try { sessionStorage.removeItem(key); } catch { /* Optional retry cache. */ }
}

function readSavedRequest(userId: string, bookingId: string): SavedRequest | null {
  try {
    const raw = readStored(savedRequestKey(userId, bookingId)) ?? readStored(legacySavedRequestKey(bookingId));
    if (!raw) return null;
    const value = JSON.parse(raw) as Partial<SavedRequest>;
    return typeof value.key === "string" && typeof value.reason === "string"
      ? { key: value.key, reason: value.reason }
      : null;
  } catch {
    return null;
  }
}

function removeSavedRequest(userId: string, bookingId: string) {
  removeStored(savedRequestKey(userId, bookingId));
  removeStored(legacySavedRequestKey(bookingId));
}

function statusKey(status: string): TranslationKey {
  const known: Record<string, TranslationKey> = {
    REQUESTED: "customer.refund.statusRequested",
    WAITING_CHECKIN_RESOLUTION: "customer.refund.statusWaiting",
    APPROVED: "customer.refund.statusApproved",
    REFUND_PROCESSING: "customer.refund.statusProcessing",
    REFUNDED: "customer.refund.statusRefunded",
    REJECTED: "customer.refund.statusRejected",
    WITHDRAWN: "customer.refund.statusWithdrawn",
    NEEDS_ATTENTION: "customer.refund.statusAttention",
  };
  return known[status] ?? "customer.refund.statusUnknown";
}

function paymentStateKey(state: string): TranslationKey {
  const known: Record<string, TranslationKey> = {
    pending: "customer.refund.paymentStatePending",
    succeeded: "customer.refund.paymentStateSucceeded",
    unknown: "customer.refund.paymentStateUnknown",
    not_required: "customer.refund.paymentStateNotRequired",
  };
  return known[state.toLowerCase()] ?? "customer.refund.paymentStateUnavailable";
}

function ticketStateKey(state: string): TranslationKey {
  const known: Record<string, TranslationKey> = {
    NOT_STARTED: "customer.refund.ticketStateNotStarted",
    PENDING: "customer.refund.ticketStatePending",
    REVOKED: "customer.refund.ticketStateRevoked",
    NEEDS_ATTENTION: "customer.refund.ticketStateAttention",
  };
  return known[state.toUpperCase()] ?? "customer.refund.ticketStateUnavailable";
}

export function WalletRefundPanel({
  userId,
  bookingId,
  bookingStatus,
  amountCents,
  hasIssuedTicket,
  hasCheckedInTicket,
  onRefundAccessChange,
  onRefundSummaryChange,
  onTicketsChanged,
}: {
  userId: string;
  bookingId: string;
  bookingStatus: string;
  amountCents: number;
  hasIssuedTicket: boolean;
  hasCheckedInTicket: boolean;
  onRefundAccessChange: (access: BuyerRefundEntryAccess) => void;
  onRefundSummaryChange?: (summary: RefundOrderSummaryUpdate) => void;
  onTicketsChanged?: () => void;
}) {
  const { t, locale } = useI18n();
  const [refund, setRefund] = useState<ApiBookingRefund | null>(null);
  const [reason, setReason] = useState("");
  const [statusKnown, setStatusKnown] = useState(false);
  const [savedRequest, setSavedRequest] = useState(false);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [busy, setBusy] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState("");
  const [observedAt, setObservedAt] = useState<string | null>(null);
  const readSequence = useRef(0);
  const observedRevocation = useRef("");
  const observeRefund = useCallback((data: ApiBookingRefund) => {
    setRefund(data);
    onRefundSummaryChange?.(data);
    onRefundAccessChange(buyerRefundEntryAccess(true, data));
    if (data.ticket_revocation_state.toUpperCase() === "REVOKED" && observedRevocation.current !== data.request_id) {
      observedRevocation.current = data.request_id;
      onTicketsChanged?.();
    }
  }, [onRefundAccessChange, onRefundSummaryChange, onTicketsChanged]);

  const refreshStatus = useCallback(async (initial = false) => {
    const sequence = ++readSequence.current;
    setStatusKnown(false);
    if (initial) setLoading(true);
    else setRefreshing(true);
    setError("");
    onRefundAccessChange("UNKNOWN");
    onRefundSummaryChange?.("loading");
    try {
      const data = await apiClient.getBookingRefund(bookingId);
      if (sequence !== readSequence.current) return;
      observeRefund(data);
      setStatusKnown(true);
      setObservedAt(new Date().toISOString());
      setConfirming(false);
      setSavedRequest(false);
      removeSavedRequest(userId, bookingId);
    } catch (cause) {
      if (sequence !== readSequence.current) return;
      if (cause instanceof ApiError && cause.status === 404) {
        setRefund(null);
        onRefundSummaryChange?.(null);
        onRefundAccessChange(buyerRefundEntryAccess(true, null));
        setStatusKnown(true);
        setObservedAt(new Date().toISOString());
        const saved = readSavedRequest(userId, bookingId);
        setSavedRequest(Boolean(saved));
        setConfirming(false);
        if (saved) setReason(saved.reason);
      } else {
        onRefundSummaryChange?.("error");
        onRefundAccessChange(buyerRefundEntryAccess(false, null));
        setError(cause instanceof Error ? cause.message : t("customer.refund.loadFailed"));
      }
    } finally {
      if (sequence === readSequence.current) {
        setLoading(false);
        setRefreshing(false);
      }
    }
  }, [bookingId, observeRefund, onRefundAccessChange, onRefundSummaryChange, t, userId]);

  useEffect(() => {
    const task = window.setTimeout(() => { void refreshStatus(true); }, 0);
    return () => { window.clearTimeout(task); readSequence.current += 1; };
  }, [refreshStatus]);

  async function submit() {
    if (busy || refreshing || !reason.trim() || !statusKnown || refund || amountCents <= 0) return;
    setBusy(true);
    setError("");
    onRefundAccessChange("UNKNOWN");
    onRefundSummaryChange?.("loading");
    const storageKey = savedRequestKey(userId, bookingId);
    const oldStorageKey = legacySavedRequestKey(bookingId);
    const saved = readSavedRequest(userId, bookingId);
    const identity = saved ?? { key: crypto.randomUUID(), reason: reason.trim() };
    setSavedRequest(true);
    if (!writeStored(storageKey, JSON.stringify(identity))) {
      setSavedRequest(false);
      setError(t("customer.refund.retryStorageUnavailable"));
      onRefundSummaryChange?.("error");
      setBusy(false);
      return;
    }
    removeStored(oldStorageKey);
    const sequence = ++readSequence.current;
    setStatusKnown(false);
    try {
      const result = await apiClient.requestBookingRefund(bookingId, identity.reason, identity.key);
      if (sequence !== readSequence.current) return;
      removeSavedRequest(userId, bookingId);
      observeRefund(result);
      setStatusKnown(true);
      setObservedAt(new Date().toISOString());
      setSavedRequest(false);
      setConfirming(false);
    } catch (cause) {
      if (sequence !== readSequence.current) return;
      setError(cause instanceof Error ? cause.message : t("customer.refund.submitFailed"));
      try {
        const committed = await apiClient.getBookingRefund(bookingId);
        if (sequence !== readSequence.current) return;
        observeRefund(committed);
        setStatusKnown(true);
        setObservedAt(new Date().toISOString());
        setSavedRequest(false);
        setConfirming(false);
        setError("");
        removeSavedRequest(userId, bookingId);
      } catch {
        if (sequence !== readSequence.current) return;
        // Retain the same user-scoped key and reason after an ambiguous response.
        onRefundSummaryChange?.("error");
        onRefundAccessChange(buyerRefundEntryAccess(false, null));
      }
    } finally {
      setBusy(false);
    }
  }

  async function withdraw() {
    if (!refund || busy || refreshing || !statusKnown || !refund.can_withdraw) return;
    const sequence = ++readSequence.current;
    onRefundAccessChange("UNKNOWN");
    onRefundSummaryChange?.("loading");
    setBusy(true);
    setError("");
    const keyName = withdrawRequestKey(userId, bookingId, refund.request_id);
    const oldKeyName = legacyWithdrawRequestKey(bookingId, refund.request_id);
    const key = readStored(keyName) ?? readStored(oldKeyName) ?? crypto.randomUUID();
    if (!writeStored(keyName, key)) {
      setBusy(false);
      setError(t("customer.refund.retryStorageUnavailable"));
      onRefundSummaryChange?.("error");
      return;
    }
    removeStored(oldKeyName);
    try {
      const result = await apiClient.withdrawBookingRefund(refund.request_id, key);
      if (sequence !== readSequence.current) return;
      observeRefund(result);
      setObservedAt(new Date().toISOString());
      removeStored(keyName);
      removeStored(oldKeyName);
    } catch (cause) {
      if (sequence !== readSequence.current) return;
      // Approval may have won while withdrawal was in flight. Re-read authority
      // before allowing a credential; a transport failure is not a withdrawal.
      await refreshStatus();
      setError(cause instanceof Error ? cause.message : t("customer.refund.withdrawFailed"));
    } finally {
      setBusy(false);
    }
  }

  const canRequest = amountCents > 0 && canSubmitBuyerRefundRequest(statusKnown, refund, bookingStatus, hasIssuedTicket, hasCheckedInTicket);
  const unapprovedNotice = refund ? unapprovedRefundNotice(refund) : null;
  if (loading) return <p role="status" className="mt-4 text-xs text-muted">{t("customer.refund.loading")}</p>;
  if (!refund && !canRequest && !error && !busy && !refreshing) return null;

  return (
    <section aria-label={t("customer.refund.title")} aria-busy={busy || refreshing} className="mt-4 rounded-lg border border-border bg-card p-3 sm:p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold">{t("customer.refund.title")}</h3>
        <div className="flex flex-wrap items-center gap-2">
          {refund ? <span className="rounded-full bg-tag-bg px-2.5 py-1 text-xs font-semibold">{t(statusKey(refund.status))}</span> : null}
          <button type="button" onClick={() => void refreshStatus()} disabled={refreshing || busy} aria-label={t("customer.refund.refresh")} className="inline-flex min-h-10 items-center gap-1.5 rounded-lg border border-border px-3 text-xs font-semibold disabled:opacity-50">
            {refreshing ? <Loader2 size={13} className="animate-spin" aria-hidden="true" /> : <RefreshCw size={13} aria-hidden="true" />}
            {refreshing ? t("customer.refund.refreshing") : t("customer.refund.refresh")}
          </button>
        </div>
      </div>
      {observedAt ? <p className="mt-2 text-[11px] text-muted">{t("customer.refund.lastChecked", { date: new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeStyle: "short" }).format(new Date(observedAt)) })}</p> : null}
      {refund ? (
        <div className="mt-2 space-y-2 text-xs text-muted">
          <p>{t("customer.refund.amount", { amount: formatUsdCents(refund.amount_cents, locale), currency: refund.currency })}</p>
          {refund.requested_at ? <p>{t("customer.refund.requestedAt", { date: new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeStyle: "short" }).format(new Date(refund.requested_at)) })}</p> : null}
          {refund.decided_at ? <p>{t("customer.refund.decidedAt", { date: new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeStyle: "short" }).format(new Date(refund.decided_at)) })}</p> : null}
          {refund.status === "REQUESTED" ? <p role="status">{t("customer.refund.requestSaved")}</p> : null}
          {refund.status === "WAITING_CHECKIN_RESOLUTION" ? <p role="status">{t("customer.refund.waitingExplanation")}</p> : null}
          {refund.status === "REFUNDED" && refund.ticket_revocation_state !== "PENDING" && refund.ticket_revocation_state !== "REVOKED" ? <p role="status" className="rounded-md border border-danger/30 bg-danger-bg px-3 py-2 text-danger-ink">{t("customer.refund.revocationNeedsAttention")}</p> : null}
          {(refund.status === "APPROVED" || refund.status === "REFUND_PROCESSING") ? <p role="status">{t("customer.refund.keepPassBlocked")}</p> : null}
          {refund.status === "REFUNDED" && refund.ticket_revocation_state === "PENDING" ? <p role="status">{t("customer.refund.revocationPending")}</p> : null}
          {refund.status === "REFUNDED" && refund.ticket_revocation_state === "REVOKED" ? <p role="status">{t("customer.refund.revoked")}</p> : null}
          {refund.status === "NEEDS_ATTENTION" ? <p role="status" className="rounded-md border border-danger/30 bg-danger-bg px-3 py-2 text-danger-ink">{t("customer.refund.statusAttentionExplanation")}</p> : null}
          {unapprovedNotice ? <p role="status">{t(unapprovedNotice)}</p> : null}
          {refund.payment_refund_state.toLowerCase() === "unknown" && !unapprovedNotice ? <p role="status" className="rounded-md border border-danger/30 bg-danger-bg px-3 py-2 text-danger-ink">{t("customer.refund.paymentUnknownExplanation")}</p> : null}
          {(refund.status === "APPROVED" || refund.status === "REFUND_PROCESSING" || refund.status === "REFUNDED" || refund.status === "NEEDS_ATTENTION") ? (
            <dl className="grid min-w-0 grid-cols-1 gap-1.5 rounded-md bg-background px-3 py-2 sm:grid-cols-2">
              <div className="min-w-0"><dt className="inline font-medium text-foreground">{t("customer.refund.paymentStateLabel")}: </dt><dd className="inline break-words">{refund.payment_refund_state ? t(paymentStateKey(refund.payment_refund_state)) : t("customer.refund.paymentStateNotRecorded")}</dd></div>
              <div className="min-w-0"><dt className="inline font-medium text-foreground">{t("customer.refund.ticketStateLabel")}: </dt><dd className="inline break-words">{t(ticketStateKey(refund.ticket_revocation_state))}</dd></div>
            </dl>
          ) : null}
          {refund.decision_reason ? <p className="break-words">{t("customer.refund.decisionReason")}: {refundDecisionReason(refund.decision_reason, t)}</p> : null}
          {refund.can_withdraw ? <button type="button" onClick={withdraw} disabled={busy} className="inline-flex min-h-11 items-center gap-2 rounded-lg border border-border px-3 font-semibold text-foreground disabled:opacity-50">{busy ? <Loader2 size={14} className="animate-spin" /> : <RotateCcw size={14} />}{t("customer.refund.withdraw")}</button> : null}
        </div>
      ) : canRequest ? (
        <div className="mt-2 space-y-2">
          <p className="text-xs leading-relaxed text-muted">{t("customer.refund.scopeNotice")}</p>
          <p className="rounded-md bg-background px-3 py-2 text-xs font-semibold">{t("customer.refund.amount", { amount: formatUsdCents(amountCents, locale), currency: "USD" })}</p>
          <label htmlFor={`refund-reason-${bookingId}`} className="block text-xs font-medium">{t("customer.refund.reason")}</label>
          <textarea id={`refund-reason-${bookingId}`} value={reason} onChange={(event) => setReason(event.target.value)} disabled={savedRequest || busy} maxLength={500} rows={3} className="w-full resize-y rounded-lg border border-border bg-background px-3 py-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-primary disabled:opacity-70" />
          {!confirming ? (
            <button type="button" onClick={() => setConfirming(true)} disabled={busy || refreshing || !reason.trim()} className="inline-flex min-h-11 items-center gap-2 rounded-lg bg-primary px-4 font-semibold text-graphite disabled:opacity-50">{t("customer.refund.request")}</button>
          ) : (
            <div role="group" aria-label={t("customer.refund.confirmationTitle")} className="space-y-2 rounded-lg border border-border bg-background p-3">
              <h4 className="text-xs font-semibold">{t("customer.refund.confirmationTitle")}</h4>
              <p className="text-xs leading-relaxed text-muted">{t("customer.refund.confirmationMessage", { amount: formatUsdCents(amountCents, locale) })}</p>
              <div className="flex flex-col gap-2 sm:flex-row">
                <button type="button" onClick={() => void submit()} disabled={busy || refreshing || !reason.trim()} className="inline-flex min-h-11 items-center justify-center gap-2 rounded-lg bg-primary px-4 font-semibold text-graphite disabled:opacity-50">
                  {busy ? <Loader2 size={15} className="animate-spin" aria-hidden="true" /> : null}{busy ? t("customer.refund.submitting") : t("customer.refund.confirmRequest")}
                </button>
                <button type="button" onClick={() => setConfirming(false)} disabled={busy} className="min-h-11 rounded-lg border border-border px-4 text-xs font-semibold disabled:opacity-50">{t("customer.refund.cancelConfirmation")}</button>
              </div>
            </div>
          )}
        </div>
      ) : null}
      {error ? <p role="alert" className="mt-2 break-words text-xs text-danger-ink">{error}</p> : null}
    </section>
  );
}
