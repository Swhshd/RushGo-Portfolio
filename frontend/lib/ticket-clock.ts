const listeners = new Set<() => void>();
let snapshot = 0;
let timer: ReturnType<typeof setInterval> | null = null;

export function getTicketClockSnapshot(): number {
  return snapshot;
}

export function getTicketClockServerSnapshot(): number {
  return 0;
}

export function getTicketClockNow(): number {
  return Date.now();
}

export function refreshTicketClock(): void {
  snapshot = Date.now();
  listeners.forEach((listener) => listener());
}

export function subscribeTicketClock(listener: () => void): () => void {
  listeners.add(listener);
  if (listeners.size === 1) {
    refreshTicketClock();
    timer = setInterval(refreshTicketClock, 500);
  }
  return () => {
    listeners.delete(listener);
    if (listeners.size === 0 && timer !== null) {
      clearInterval(timer);
      timer = null;
    }
  };
}
