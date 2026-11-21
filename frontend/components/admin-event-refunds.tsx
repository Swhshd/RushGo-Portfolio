"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import Link from "next/link";
import { Loader2, RefreshCw, ShieldCheck } from "lucide-react";
import { apiClient } from "@/lib/api/client";
import type { ApiBookingRefund, ApiEventRefundApprover } from "@/lib/api/types";
import { useI18n, type TranslationKey } from "@/lib/i18n/index";
import { formatUsdCents } from "@/lib/wallet";
import { canApproveBookingRefund, canRejectBookingRefund } from "@/lib/booking-refunds";
import { readRefundDecision, reserveRefundDecision, clearRefundDecision, type PendingRefundDecision } from "@/lib/refund-decision-storage";
import { awaitingRefundApproval, refundDecisionReason, unapprovedRefundNotice } from "@/lib/refund-presentation";

type RefundList = { refunds: ApiBookingRefund[]; approval_enabled: boolean; approval_block_reason: string; next_cursor: string };


const KNOWN_REFUND_STATUSES = new Set([
  "REQUESTED", "WAITING_CHECKIN_RESOLUTION", "APPROVED", "REFUND_PROCESSING",
  "REFUNDED", "REJECTED", "WITHDRAWN", "NEEDS_ATTENTION",
]);
const KNOWN_PAYMENT_STATES = new Set(["pending", "succeeded", "unknown", "not_required"]);
const KNOWN_TICKET_STATES = new Set(["NOT_STARTED", "PENDING", "REVOKED", "NEEDS_ATTENTION"]);

function paymentStateKey(state: string): TranslationKey {
  const known: Record<string, TranslationKey> = {
    pending: "admin.refunds.paymentPending",
    succeeded: "admin.refunds.paymentSucceeded",
    unknown: "admin.refunds.paymentUnknown",
    not_required: "admin.refunds.paymentNotRequired",
  };
  return known[state.toLowerCase()] ?? (state ? "admin.refunds.paymentUnavailable" : "admin.refunds.paymentNotRecorded");
}

function ticketStateKey(state: string): TranslationKey {
  const known: Record<string, TranslationKey> = {
    NOT_STARTED: "admin.refunds.ticketNotStarted",
    PENDING: "admin.refunds.ticketPending",
    REVOKED: "admin.refunds.ticketRevoked",
    NEEDS_ATTENTION: "admin.refunds.ticketNeedsAttention",
  };
  return known[state.toUpperCase()] ?? "admin.refunds.ticketUnavailable";
}

function needsRefundFollowUp(refund: ApiBookingRefund): boolean {
  const payment = refund.payment_refund_state.toLowerCase();
  const ticket = refund.ticket_revocation_state.toUpperCase();
  const refundInProgress = refund.status === "APPROVED" || refund.status === "REFUND_PROCESSING" || refund.status === "REFUNDED";
  return !KNOWN_REFUND_STATUSES.has(refund.status) ||
    refund.status === "NEEDS_ATTENTION" ||
    (payment === "unknown" && !awaitingRefundApproval(refund)) ||
    (refund.status === "REQUESTED" && (Boolean(refund.refund_decision_id) || Boolean(refund.decided_at) || payment === "pending" || ticket === "PENDING" || ticket === "REVOKED")) ||
    Boolean(payment && !KNOWN_PAYMENT_STATES.has(payment)) ||
    Boolean(ticket && !KNOWN_TICKET_STATES.has(ticket)) ||
    (refundInProgress && (!payment || !ticket)) ||
    (refund.status === "REFUNDED" && payment !== "succeeded") ||
    (payment === "succeeded" && refund.status !== "REFUNDED" && refund.status !== "NEEDS_ATTENTION") ||
    ticket === "NEEDS_ATTENTION" ||
    (refund.status === "REFUNDED" && ticket !== "REVOKED");
}

function refundStatusKey(status: string) {
  const keys = {
    REQUESTED: "admin.refunds.statusRequested",
    WAITING_CHECKIN_RESOLUTION: "admin.refunds.statusWaiting",
    APPROVED: "admin.refunds.statusApproved",
    REFUND_PROCESSING: "admin.refunds.statusProcessing",
    REFUNDED: "admin.refunds.statusRefunded",
    REJECTED: "admin.refunds.statusRejected",
    WITHDRAWN: "admin.refunds.statusWithdrawn",
    NEEDS_ATTENTION: "admin.refunds.statusAttention",
  } as const;
  return keys[status as keyof typeof keys] ?? "admin.refunds.statusUnknown";
}

export function AdminEventRefunds({ eventId, userId, isAdmin = false }: { eventId: string; userId: string; isAdmin?: boolean }) {
  const { t, locale, number } = useI18n();
  const [data, setData] = useState<RefundList | null>(null);
  const [assignments, setAssignments] = useState<ApiEventRefundApprover[]>([]);
  const [staffUserId, setStaffUserId] = useState("");
  const [reasonByRequest, setReasonByRequest] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [assignmentError, setAssignmentError] = useState("");
  const [notice, setNotice] = useState("");
  const [observedAt, setObservedAt] = useState<string | null>(null);

  const readSequence = useRef(0);
  const [pendingDecisions, setPendingDecisions] = useState<Record<string, PendingRefundDecision | "INVALID">>({});

  const reconcileDecisions = useCallback((refunds: ApiBookingRefund[]) => {
    const pending: Record<string, PendingRefundDecision | "INVALID"> = {};
    const reasons: Record<string, string> = {};
    for (const refund of refunds) {
      try {
        if (["APPROVED", "REFUND_PROCESSING", "REFUNDED", "REJECTED", "WITHDRAWN", "NEEDS_ATTENTION"].includes(refund.status)) {
          clearRefundDecision(userId, eventId, refund.request_id);
        } else {
          const saved = readRefundDecision(userId, eventId, refund.request_id);
          if (saved) { pending[refund.request_id] = saved; reasons[refund.request_id] = saved.reason; }
        }
      } catch { pending[refund.request_id] = "INVALID"; }
    }
    setPendingDecisions((current) => {
      const next = { ...current };
      refunds.forEach((refund) => { delete next[refund.request_id]; });
      return { ...next, ...pending };
    });
    setReasonByRequest((current) => ({ ...current, ...reasons }));
  }, [eventId, userId]);

  const reload = useCallback(async () => {
    const sequence = ++readSequence.current;
    setLoading(true);
    setLoadingMore(false);
    setError("");
    setAssignmentError("");
    setData(null);
    setAssignments([]);
    setObservedAt(null);
    const [refundResult, approverResult] = await Promise.allSettled([
        apiClient.listEventBookingRefunds(eventId),
        isAdmin ? apiClient.listEventRefundApprovers(eventId) : Promise.resolve({ assignments: [] }),
    ]);
    if (sequence !== readSequence.current) return;
    if (refundResult.status === "fulfilled") {
      const refunds = refundResult.value;
      setData(refunds);
      setObservedAt(new Date().toISOString());
      reconcileDecisions(refunds.refunds);
    }
    else setError(refundResult.reason instanceof Error ? refundResult.reason.message : t("admin.refunds.loadFailed"));
    if (approverResult.status === "fulfilled") setAssignments(approverResult.value.assignments ?? []);
    else if (isAdmin) setAssignmentError(approverResult.reason instanceof Error ? approverResult.reason.message : t("admin.refunds.assignmentFailed"));
    setLoading(false);
  }, [eventId, isAdmin, reconcileDecisions, t]);

  useEffect(() => {
    const task = window.setTimeout(() => { void reload(); }, 0);
    return () => { window.clearTimeout(task); readSequence.current += 1; };
  }, [reload]);

  async function loadMore() {
    if (!data?.next_cursor || loadingMore || loading || busy) return;
    const sequence = readSequence.current;
    setLoadingMore(true);
    setError("");
    try {
      const page = await apiClient.listEventBookingRefunds(eventId, data.next_cursor);
      if (sequence !== readSequence.current) return;
      reconcileDecisions(page.refunds);
      setData((current) => current ? {
        ...page,
        refunds: Array.from(new Map([...current.refunds, ...page.refunds].map((refund) => [refund.request_id, refund])).values()),
      } : page);
      setObservedAt(new Date().toISOString());
    } catch (cause) {
      if (sequence === readSequence.current) setError(cause instanceof Error ? cause.message : t("admin.refunds.loadFailed"));
    } finally {
      if (sequence === readSequence.current) setLoadingMore(false);
    }
  }

  async function decide(refund: ApiBookingRefund, decision: "APPROVE" | "REJECT") {
    const reason = (reasonByRequest[refund.request_id] ?? "").trim();
    if (busy || !reason || (decision === "APPROVE" && !canApproveBookingRefund(refund.status, Boolean(data?.approval_enabled && refund.approval_enabled), refund.currency)) || (decision === "REJECT" && !canRejectBookingRefund(refund.status))) return;
    let identity: PendingRefundDecision;
    try { identity = reserveRefundDecision(userId, eventId, refund.request_id, decision, reason, refund.status === "WAITING_CHECKIN_RESOLUTION"); } catch {
      setError(t("admin.refunds.retryStorageUnavailable"));
      return;
    }
    setPendingDecisions((current) => ({ ...current, [refund.request_id]: identity }));
    setReasonByRequest((current) => ({ ...current, [refund.request_id]: identity.reason }));
    setBusy(refund.request_id);
    readSequence.current += 1;
    setLoadingMore(false);
    setError("");
    setNotice("");
    try {
      const result = await apiClient.decideBookingRefund(eventId, refund.request_id, decision, identity.reason, identity.key);
      const waitingForCheckin = decision === "APPROVE" && result.status === "WAITING_CHECKIN_RESOLUTION";
      reconcileDecisions([result]);
      setNotice(waitingForCheckin ? t("admin.refunds.waitingCheckinNotice") : decision === "APPROVE" ? t("admin.refunds.approvedNotice") : t("admin.refunds.rejectedNotice"));
      await reload();
    } catch (cause) {
      await reload(); // A lost response can follow a committed decision.
      setError(cause instanceof Error ? cause.message : t("admin.refunds.decisionFailed"));
    } finally {
      setBusy("");
    }
  }

  async function setApprover(userId: string, active: boolean) {
    const normalized = userId.trim();
    if (!normalized || busy) return;
    setBusy(normalized);
    setError("");
    try {
      await apiClient.setEventRefundApprover(eventId, normalized, active);
      setStaffUserId("");
      setNotice(active ? t("admin.refunds.approverGranted") : t("admin.refunds.approverRevoked"));
      await reload();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : t("admin.refunds.assignmentFailed"));
    } finally {
      setBusy("");
    }
  }

  const date = (value: string) => value ? new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeStyle: "short" }).format(new Date(value)) : t("admin.refunds.notAvailable");
  const loadedRefunds = data?.refunds ?? [];
  const counts = {
    requested: loadedRefunds.filter((refund) => refund.status === "REQUESTED").length,
    waiting: loadedRefunds.filter((refund) => refund.status === "WAITING_CHECKIN_RESOLUTION").length,
    processing: loadedRefunds.filter((refund) => refund.status === "APPROVED" || refund.status === "REFUND_PROCESSING").length,
    refunded: loadedRefunds.filter((refund) => refund.status === "REFUNDED").length,
    followUp: loadedRefunds.filter(needsRefundFollowUp).length,
  };
  const lastChecked = observedAt ? date(observedAt) : t("admin.refunds.notAvailable");
  const approvalGateNotice = data?.approval_block_reason && !/^[A-Za-z0-9_]+$/.test(data.approval_block_reason)
    ? data.approval_block_reason
    : t("admin.refunds.approvalDisabled");

  return (
    <section aria-labelledby="admin-event-refunds-title" className="min-w-0 rounded-xl border border-border bg-card p-4 sm:p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h2 id="admin-event-refunds-title" className="text-sm font-semibold">{t("admin.refunds.title")}</h2>
          <p className="mt-1 text-xs leading-relaxed text-muted">{t("admin.refunds.description")}</p>
          <p className="mt-1 text-xs leading-relaxed text-muted">{t("admin.refunds.approvalNotRefunded")}</p>
          <p className="mt-1 text-xs leading-relaxed text-muted">{t("admin.refunds.permissionNotice")}</p>
        </div>
        <button type="button" onClick={() => void reload()} disabled={loading || busy !== ""} className="inline-flex min-h-11 shrink-0 items-center gap-2 rounded-lg border border-border px-3 text-xs font-semibold disabled:opacity-50">
          {loading ? <Loader2 size={14} className="animate-spin" /> : <RefreshCw size={14} />}{t("admin.common.refresh")}
        </button>
      </div>

      {isAdmin ? (
        <div className="mt-4 rounded-lg border border-border p-3 sm:p-4">
          <h3 className="flex items-center gap-2 text-xs font-semibold"><ShieldCheck size={14} />{t("admin.refunds.approversTitle")}</h3>
          <p className="mt-1 text-xs leading-relaxed text-muted">{t("admin.refunds.approversDescription")}</p>
          <form onSubmit={(event) => { event.preventDefault(); void setApprover(staffUserId, true); }} className="mt-3 flex flex-col gap-2 sm:flex-row">
            <label className="sr-only" htmlFor="refund-approver-id">{t("admin.refunds.userId")}</label>
            <input id="refund-approver-id" value={staffUserId} onChange={(event) => setStaffUserId(event.target.value)} autoComplete="off" spellCheck={false} className="min-h-11 min-w-0 flex-1 rounded-lg border border-border bg-background px-3 text-sm font-mono" placeholder={t("admin.refunds.userIdPlaceholder")} />
            <button type="submit" disabled={busy !== "" || !staffUserId.trim()} className="min-h-11 rounded-lg bg-primary px-4 text-sm font-semibold text-graphite disabled:opacity-50">{t("admin.refunds.grant")}</button>
          </form>
          {assignmentError ? <p role="alert" className="mt-3 break-words text-xs text-danger-ink">{assignmentError}</p> : null}
          {assignments.length ? <ul className="mt-3 divide-y divide-border" aria-label={t("admin.refunds.approverList")}>
            {assignments.map((assignment) => <li key={assignment.staff_user_id} className="flex min-w-0 flex-wrap items-center justify-between gap-2 py-2 text-xs">
              <code className="min-w-0 break-all font-mono">{assignment.staff_user_id}</code>
              <span className="text-muted">{assignment.active ? t("admin.refunds.active") : t("admin.refunds.revoked")}</span>
              {assignment.active ? <button type="button" disabled={busy !== ""} onClick={() => void setApprover(assignment.staff_user_id, false)} className="min-h-10 rounded-lg border border-border px-3 font-semibold disabled:opacity-50">{t("admin.refunds.revoke")}</button> : null}
            </li>)}
          </ul> : <p className="mt-3 text-xs text-muted">{t("admin.refunds.noApprovers")}</p>}
        </div>
      ) : null}

      {!loading && data && !data.approval_enabled ? <p role="status" className="mt-4 break-words rounded-lg border border-border bg-background px-3 py-3 text-xs leading-relaxed text-muted">{approvalGateNotice}</p> : null}
      {notice ? <p role="status" aria-live="polite" className="mt-3 text-xs text-success-ink">{notice}</p> : null}
      {error ? <p role="alert" className="mt-3 break-words text-xs text-danger-ink">{error}</p> : null}
      {loading ? <p role="status" className="mt-4 flex items-center gap-2 text-xs text-muted"><Loader2 size={14} className="animate-spin" />{t("admin.refunds.loading")}</p> : null}
      {!loading && data ? (
        <section aria-labelledby="admin-refund-exceptions-title" className="mt-4 rounded-lg border border-border bg-background p-3 sm:p-4">
          <div className="flex flex-wrap items-start justify-between gap-2">
            <div className="min-w-0">
              <h3 id="admin-refund-exceptions-title" className="text-xs font-semibold">{t("admin.refunds.summaryTitle")}</h3>
              <p className="mt-1 break-words text-[11px] leading-relaxed text-muted">{t(data.next_cursor ? "admin.refunds.summaryPartial" : "admin.refunds.summaryComplete", { count: number(loadedRefunds.length) })}</p>
              <p className="mt-1 break-words text-[11px] leading-relaxed text-muted">{t("admin.refunds.summaryCoverage")}</p>
            </div>
            {observedAt ? <p className="shrink-0 text-[11px] text-muted">{t("admin.refunds.lastChecked", { date: lastChecked })}</p> : null}
          </div>
          <dl className="mt-3 grid min-w-0 grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-5">
            <div className="min-w-0 rounded-md border border-border bg-card p-2.5"><dt className="break-words text-[10px] text-muted">{t("admin.refunds.summaryRequested")}</dt><dd className="mt-1 font-mono text-lg font-semibold tabular-nums">{number(counts.requested)}</dd></div>
            <div className="min-w-0 rounded-md border border-border bg-card p-2.5"><dt className="break-words text-[10px] text-muted">{t("admin.refunds.summaryWaiting")}</dt><dd className="mt-1 font-mono text-lg font-semibold tabular-nums">{number(counts.waiting)}</dd></div>
            <div className="min-w-0 rounded-md border border-border bg-card p-2.5"><dt className="break-words text-[10px] text-muted">{t("admin.refunds.summaryProcessing")}</dt><dd className="mt-1 font-mono text-lg font-semibold tabular-nums">{number(counts.processing)}</dd></div>
            <div className="min-w-0 rounded-md border border-border bg-card p-2.5"><dt className="break-words text-[10px] text-muted">{t("admin.refunds.summaryRefunded")}</dt><dd className="mt-1 font-mono text-lg font-semibold tabular-nums">{number(counts.refunded)}</dd></div>
            <div className={`min-w-0 rounded-md border p-2.5 ${counts.followUp > 0 ? "border-danger/30 bg-danger-bg text-danger-ink" : "border-border bg-card text-muted"}`}><dt className="break-words text-[10px]">{t("admin.refunds.summaryFollowUp")}</dt><dd className="mt-1 font-mono text-lg font-semibold tabular-nums">{number(counts.followUp)}</dd></div>
          </dl>
        </section>
      ) : null}
      {!loading && data && data.refunds.length === 0 ? <p className="mt-4 rounded-lg border border-border px-3 py-4 text-xs text-muted">{t("admin.refunds.empty")}</p> : null}
      {!loading && data && data.refunds.length > 0 ? <ul className="mt-4 space-y-3">
        {data.refunds.map((refund) => {
          const actionable = canRejectBookingRefund(refund.status);
          const busyThis = busy === refund.request_id;
          const pending = pendingDecisions[refund.request_id];
          const unapprovedNotice = unapprovedRefundNotice(refund);
          return <li key={refund.request_id} className="min-w-0 rounded-lg border border-border bg-background p-3 sm:p-4">
            <div className="flex flex-wrap items-start justify-between gap-2">
              <div className="min-w-0"><p className="break-words text-xs font-semibold">{t("admin.refunds.booking")}: <code className="font-mono">{refund.booking_id}</code></p><p className="mt-1 break-words text-[11px] text-muted">{t("admin.refunds.requestedAt", { date: date(refund.requested_at) })}</p><Link href={`/admin/orders/${refund.booking_id}`} className="mt-1 inline-flex text-[11px] font-semibold text-primary underline underline-offset-2">{t("admin.orderLookup.openOrder")}</Link></div>
              <span className="shrink-0 rounded-full bg-tag-bg px-2.5 py-1 text-[11px] font-semibold">{t(refundStatusKey(refund.status))}</span>
            </div>
            {refund.status === "WAITING_CHECKIN_RESOLUTION" ? <p role="status" className="mt-2 text-xs leading-relaxed text-muted">{t("admin.refunds.waitingCheckin")}</p> : null}
            {refund.status === "REFUNDED" && refund.ticket_revocation_state !== "REVOKED" ? <p role="status" className={`mt-2 break-words rounded-lg border px-3 py-2 text-xs leading-relaxed ${refund.ticket_revocation_state === "PENDING" ? "border-border bg-card text-muted" : "border-danger/30 bg-danger-bg text-danger-ink"}`}>{refund.ticket_revocation_state === "PENDING" ? t("admin.refunds.revocationPending") : t("admin.refunds.revocationNeedsAttention")}</p> : null}
            {refund.status === "NEEDS_ATTENTION" ? <p role="status" className="mt-2 break-words rounded-lg border border-danger/30 bg-danger-bg px-3 py-2 text-xs leading-relaxed text-danger-ink">{t("admin.refunds.statusAttentionExplanation")}</p> : null}
            {unapprovedNotice ? <p role="status" className="mt-2 text-xs text-muted">{t(unapprovedNotice)}</p> : null}
            {refund.payment_refund_state.toLowerCase() === "unknown" && !unapprovedNotice ? <p role="status" className="mt-2 break-words rounded-lg border border-danger/30 bg-danger-bg px-3 py-2 text-xs leading-relaxed text-danger-ink">{t("admin.refunds.paymentUnknownExplanation")}</p> : null}
            <div className="mt-3 grid min-w-0 grid-cols-1 gap-2 text-xs sm:grid-cols-2">
              <p>{t("admin.refunds.amount", { amount: formatUsdCents(refund.amount_cents, locale), currency: refund.currency || "USD" })}</p>
              <p className="break-words">{t("admin.refunds.reason")}: {refund.reason || t("admin.refunds.notAvailable")}</p>
              <p className="break-words">{t("admin.refunds.paymentState")}: {awaitingRefundApproval(refund) ? t("aftersales.paymentNotStarted") : refund.payment_refund_state ? t(paymentStateKey(refund.payment_refund_state)) : t("admin.refunds.paymentNotRecorded")}</p>
              <p className="break-words">{t("admin.refunds.ticketState")}: {t(ticketStateKey(refund.ticket_revocation_state))}</p>
            </div>
            {actionable ? <div className="mt-3 space-y-2">
              {pending ? <p role="status" className="text-xs text-muted">{t("admin.refunds.pendingDecisionNotice")}</p> : null}
              <label htmlFor={`refund-decision-${refund.request_id}`} className="block text-xs font-medium">{t("admin.refunds.decisionReason")}</label>
              <textarea disabled={(Boolean(pending) && !(pending !== "INVALID" && pending?.decision === "APPROVE" && refund.status === "WAITING_CHECKIN_RESOLUTION")) || busy !== ""} id={`refund-decision-${refund.request_id}`} value={reasonByRequest[refund.request_id] ?? ""} onChange={(event) => setReasonByRequest((current) => ({ ...current, [refund.request_id]: event.target.value }))} maxLength={500} rows={2} className="w-full resize-y rounded-lg border border-border bg-card px-3 py-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-primary" />
              <div className="flex flex-col gap-2 sm:flex-row sm:flex-wrap">
                <button type="button" disabled={pending === "INVALID" || (pending && pending.decision !== "APPROVE") || busyThis || busy !== "" || !canApproveBookingRefund(refund.status, data.approval_enabled && refund.approval_enabled, refund.currency) || !(reasonByRequest[refund.request_id] ?? "").trim()} onClick={() => void decide(refund, "APPROVE")} className="min-h-11 rounded-lg bg-primary px-4 text-sm font-semibold text-graphite disabled:opacity-50">{busyThis ? t("admin.refunds.saving") : refund.status === "WAITING_CHECKIN_RESOLUTION" ? t("admin.refunds.retryApproval") : t("admin.refunds.approve")}</button>
                <button type="button" disabled={pending === "INVALID" || (pending && pending.decision !== "REJECT" && refund.status !== "WAITING_CHECKIN_RESOLUTION") || busyThis || busy !== "" || !(reasonByRequest[refund.request_id] ?? "").trim()} onClick={() => void decide(refund, "REJECT")} className="min-h-11 rounded-lg border border-border px-4 text-sm font-semibold disabled:opacity-50">{busyThis ? t("admin.refunds.saving") : t("admin.refunds.reject")}</button>
              </div>
            </div> : (
              <div className="mt-3 space-y-1 text-xs text-muted">
                <p>{t("admin.refunds.decidedAt", { date: date(refund.decided_at) })}</p>
                {refund.decision_reason ? <p className="break-words">{t("admin.refunds.decisionReason")}: {refundDecisionReason(refund.decision_reason, t)}</p> : null}
              </div>
            )}
          </li>;
        })}
      </ul> : null}
      {!loading && data && data.next_cursor ? <button type="button" onClick={() => void loadMore()} disabled={loadingMore} className="mt-3 inline-flex min-h-11 items-center gap-2 rounded-lg border border-border px-3 text-xs font-semibold disabled:opacity-50">{loadingMore ? <Loader2 size={14} className="animate-spin" /> : null}{loadingMore ? t("admin.refunds.loadingMore") : t("admin.refunds.moreAvailable", { count: number(data.refunds.length) })}</button> : null}
    </section>
  );
}
