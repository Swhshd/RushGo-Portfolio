export type PendingRefundDecision = { key: string; reason: string; decision: "APPROVE" | "REJECT" };

function storageKey(userId: string, eventId: string, requestId: string) {
  if (!userId) throw new Error("Refund decision requires a signed-in operator.");
  return `rushgo.refund-decision.v2.${userId}.${eventId}.${requestId}`;
}

function readStored(key: string) {
  try { const value = localStorage.getItem(key); if (value) return value; } catch { /* Try tab storage. */ }
  try { return sessionStorage.getItem(key); } catch { return null; }
}

export function readRefundDecision(userId: string, eventId: string, requestId: string): PendingRefundDecision | null {
  const raw = readStored(storageKey(userId, eventId, requestId));
  if (!raw) return null;
  const value = JSON.parse(raw) as Partial<PendingRefundDecision>;
  if (!value || typeof value.key !== "string" || !/^[0-9a-f-]{36}$/i.test(value.key) ||
      typeof value.reason !== "string" || !value.reason.trim() || value.reason.length > 500 ||
      (value.decision !== "APPROVE" && value.decision !== "REJECT")) {
    throw new Error("Pending refund decision is invalid.");
  }
  return value as PendingRefundDecision;
}

export function reserveRefundDecision(userId: string, eventId: string, requestId: string, decision: PendingRefundDecision["decision"], reason: string, waitingForCheckin = false): PendingRefundDecision {
  const existing = readRefundDecision(userId, eventId, requestId);
  if (existing) {
    if (existing.decision === decision) return existing; // Preserve retries after ambiguous responses.
    // Waiting approval has not committed a refund decision; rejection remains allowed.
    if (!(waitingForCheckin && existing.decision === "APPROVE" && decision === "REJECT")) {
      throw new Error("An unresolved decision must be reconciled first.");
    }
  }
  const value = { key: crypto.randomUUID(), reason, decision };
  const key = storageKey(userId, eventId, requestId);
  try { localStorage.setItem(key, JSON.stringify(value)); return value; } catch { /* Try tab storage. */ }
  try { sessionStorage.setItem(key, JSON.stringify(value)); return value; } catch {
    throw new Error("Pending refund decision could not be stored.");
  }
}

export function clearRefundDecision(userId: string, eventId: string, requestId: string) {
  const key = storageKey(userId, eventId, requestId);
  try { localStorage.removeItem(key); } catch { /* Continue to tab storage. */ }
  try { sessionStorage.removeItem(key); } catch { /* Optional retry cache. */ }
}
