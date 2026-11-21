import { ticketCredentialPendingStorageKey } from "./ticket-credential-version";

export type PendingRotationKeySnapshot = {
  loaded: boolean;
  key: string | null;
};

const serverSnapshot: PendingRotationKeySnapshot = { loaded: false, key: null };
const snapshots = new Map<string, PendingRotationKeySnapshot>();
const listeners = new Map<string, Set<() => void>>();

export function getPendingRotationKeySnapshot(ticketId: string): PendingRotationKeySnapshot {
  return snapshots.get(ticketId) ?? serverSnapshot;
}

export function getPendingRotationKeyServerSnapshot(): PendingRotationKeySnapshot {
  return serverSnapshot;
}

export function subscribePendingRotationKey(ticketId: string, listener: () => void): () => void {
  let ticketListeners = listeners.get(ticketId);
  if (!ticketListeners) {
    ticketListeners = new Set();
    listeners.set(ticketId, ticketListeners);
  }
  ticketListeners.add(listener);

  if (!getPendingRotationKeySnapshot(ticketId).loaded) {
    let key: string | null = null;
    try {
      if (typeof window !== "undefined") {
        key = window.sessionStorage.getItem(ticketCredentialPendingStorageKey(ticketId));
      }
    } catch {
      // A failed browser storage read keeps the request usable in this tab.
    }
    snapshots.set(ticketId, { loaded: true, key });
    ticketListeners.forEach((notify) => notify());
  }

  return () => {
    ticketListeners?.delete(listener);
    if (ticketListeners?.size === 0) listeners.delete(ticketId);
  };
}

export function setPendingRotationKey(ticketId: string, key: string | null): void {
  snapshots.set(ticketId, { loaded: true, key });
  try {
    if (typeof window !== "undefined") {
      const storageKey = ticketCredentialPendingStorageKey(ticketId);
      if (key) window.sessionStorage.setItem(storageKey, key);
      else window.sessionStorage.removeItem(storageKey);
    }
  } catch {
    // Keep the idempotency key in memory when sessionStorage is unavailable.
  }
  listeners.get(ticketId)?.forEach((notify) => notify());
}
