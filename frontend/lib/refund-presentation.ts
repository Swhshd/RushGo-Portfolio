import type { ApiBookingRefund } from "./api/types";
import type { TranslationKey } from "./i18n/core";

// Only a complete pre-approval snapshot can explain an unknown payment state
// as not started. A lagging status must never hide downstream work or a decision.
export function awaitingRefundApproval(refund: ApiBookingRefund): boolean {
  return refund.status === "REQUESTED" && refund.refund_decision_id === "" &&
    (refund.decided_at === "" || refund.decided_at === null) &&
    ["", "not_required", "unknown"].includes(refund.payment_refund_state.toLowerCase()) &&
    refund.ticket_revocation_state.toUpperCase() === "NOT_STARTED";
}

// Presentation only: never changes refund eligibility or ticket admission gates.
export function unapprovedRefundNotice(refund: ApiBookingRefund): TranslationKey | null {
  if (awaitingRefundApproval(refund)) return "aftersales.awaitingApproval";
  const payment = refund.payment_refund_state.toLowerCase();
  const ticket = refund.ticket_revocation_state.toUpperCase();
  if (!["", "not_required", "unknown"].includes(payment) || !["", "NOT_STARTED"].includes(ticket)) return null;
  if (refund.status === "REJECTED") return "aftersales.notApproved";
  if (refund.status === "WITHDRAWN") return "aftersales.withdrawn";
  return null;
}

export type RefundOrderSummaryUpdate = ApiBookingRefund | null | "loading" | "error";
export type RefundOrderSummaryTone = "success" | "pending" | "danger" | "muted";

export function refundOrderSummary(update: RefundOrderSummaryUpdate): {
  key: TranslationKey;
  tone: RefundOrderSummaryTone;
} | null {
  if (update === "loading") return { key: "aftersales.refundStatusLoading", tone: "muted" };
  if (update === "error") return { key: "aftersales.refundStatusUnavailable", tone: "danger" };
  if (!update) return null;

  const status = update.status.trim().toUpperCase();
  const payment = update.payment_refund_state.trim().toLowerCase();
  const ticket = update.ticket_revocation_state.trim().toUpperCase();
  const needsAttention = { key: "aftersales.refundNeedsAttention" as const, tone: "danger" as const };

  if (status === "REFUNDED") {
    if (payment === "succeeded" && ticket === "REVOKED") {
      return { key: "aftersales.refundComplete", tone: "success" };
    }
    if (payment === "succeeded" && ticket === "PENDING") {
      return { key: "aftersales.refundTicketRevocationPending", tone: "pending" };
    }
    return needsAttention;
  }

  // A successful payment refund without the terminal refund status is an
  // inconsistent snapshot. Approval and ticket revocation alone never count
  // as a completed refund.
  if (payment === "succeeded") return needsAttention;

  switch (status) {
    case "REQUESTED": {
      const notice = unapprovedRefundNotice(update);
      return notice ? { key: notice, tone: "pending" } : needsAttention;
    }
    case "WAITING_CHECKIN_RESOLUTION":
      return { key: "customer.refund.statusWaiting", tone: "pending" };
    case "APPROVED":
      return { key: "aftersales.refundApprovedPending", tone: "pending" };
    case "REFUND_PROCESSING":
      return { key: "customer.refund.statusProcessing", tone: "pending" };
    case "REJECTED":
    case "WITHDRAWN": {
      const notice = unapprovedRefundNotice(update);
      return notice ? { key: notice, tone: "muted" } : needsAttention;
    }
    case "NEEDS_ATTENTION":
    default:
      return needsAttention;
  }
}

export function refundDecisionReason(reason: string, t: (key: TranslationKey) => string): string {
  if (reason === "ticket_set_mismatch") return t("aftersales.ticketNotEligible");
  // Unknown machine codes are not useful customer instructions. Human review
  // reasons remain unchanged, and are rendered as text by React.
  if (/^[a-z][a-z0-9]*(?:_[a-z0-9]+)+$/.test(reason)) return t("aftersales.reasonUnavailable");
  return reason;
}
