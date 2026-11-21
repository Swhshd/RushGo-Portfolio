export const ADMIN_READ_TIMEOUT_MS = 10_000;

export type DeadlineScheduler = {
  setTimeout(callback: () => void, delayMs: number): ReturnType<typeof setTimeout>;
  clearTimeout(handle: ReturnType<typeof setTimeout>): void;
};

const browserScheduler: DeadlineScheduler = {
  setTimeout: (callback, delayMs) => globalThis.setTimeout(callback, delayMs),
  clearTimeout: (handle) => globalThis.clearTimeout(handle),
};

/**
 * Bounds how long callers wait, aborts the underlying read at the deadline,
 * and also rejects promptly when its owner cancels the request.
 */
export function withRequestDeadline<T>(
  request: Promise<T>,
  controller: AbortController,
  timeoutMs = ADMIN_READ_TIMEOUT_MS,
  scheduler: DeadlineScheduler = browserScheduler,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    let timedOut = false;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const clear = () => {
      if (timer !== null) {
        scheduler.clearTimeout(timer);
        timer = null;
      }
      controller.signal.removeEventListener("abort", onAbort);
    };

    const onAbort = () => {
      if (settled) return;
      settled = true;
      clear();
      const error = new Error(timedOut ? "Read request timed out." : "Read request was cancelled.");
      error.name = timedOut ? "TimeoutError" : "AbortError";
      reject(error);
    };

    if (controller.signal.aborted) {
      onAbort();
      return;
    }

    controller.signal.addEventListener("abort", onAbort, { once: true });
    timer = scheduler.setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, timeoutMs);

    void request.then(
      (result) => {
        if (settled) return;
        settled = true;
        clear();
        resolve(result);
      },
      (error: unknown) => {
        if (settled) return;
        settled = true;
        clear();
        reject(error);
      },
    );
  });
}
