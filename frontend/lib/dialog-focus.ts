/**
 * DOM-only focus helpers for modal drawers.
 *
 * Kept free of React so the wrap-around math can be unit-tested with plain
 * Node. The drawer hook (`use-dialog-focus`) supplies the document access.
 */

export const FOCUSABLE_SELECTOR = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled]):not([type='hidden'])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  "[tabindex]:not([tabindex='-1'])",
].join(",");

/**
 * Next index while cycling Tab inside a closed set of focusable elements.
 * Returns -1 when there is nothing to focus.
 */
export function nextFocusIndex(
  activeIndex: number,
  count: number,
  shiftKey: boolean,
): number {
  if (count <= 0) return -1;
  if (activeIndex < 0) return shiftKey ? count - 1 : 0;
  return shiftKey
    ? (activeIndex - 1 + count) % count
    : (activeIndex + 1) % count;
}

/** Visible, focusable descendants of `root`, in DOM order. */
export function focusableElements(root: HTMLElement | null): HTMLElement[] {
  if (!root) return [];
  return Array.from(
    root.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR),
  ).filter(
    (element) =>
      element.getClientRects().length > 0 && !element.matches(":disabled"),
  );
}
