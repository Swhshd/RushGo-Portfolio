"use client";

import { useEffect, useRef, type RefObject } from "react";
import { focusableElements, nextFocusIndex } from "./dialog-focus";

interface DialogFocusOptions {
  /** Called for Escape and when the trap cannot find a focusable element. */
  onClose: () => void;
  /** While true, Escape is ignored (e.g. a request is in flight). */
  closeDisabled?: boolean;
}

/**
 * Modal behaviour shared by the admin drawers:
 * - moves initial focus inside the dialog (`[data-dialog-autofocus]` first),
 * - keeps Tab / Shift+Tab inside the dialog,
 * - closes on Escape unless disabled,
 * - locks background scroll,
 * - restores focus to the element that opened the dialog.
 *
 * The effect is mounted once per drawer; callbacks are read through refs so a
 * re-render never re-runs the trap.
 */
export function useDialogFocus(
  ref: RefObject<HTMLElement | null>,
  { onClose, closeDisabled = false }: DialogFocusOptions,
) {
  const onCloseRef = useRef(onClose);
  const closeDisabledRef = useRef(closeDisabled);
  // Survives the StrictMode mount→cleanup→mount cycle so the second setup
  // cannot mistake the dialog's own field for the opener.
  const returnFocusRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    onCloseRef.current = onClose;
  }, [onClose]);

  useEffect(() => {
    closeDisabledRef.current = closeDisabled;
  }, [closeDisabled]);

  useEffect(() => {
    const root = ref.current;
    if (!root) return;

    const activeBeforeMount = document.activeElement;
    if (
      activeBeforeMount instanceof HTMLElement &&
      !root.contains(activeBeforeMount)
    ) {
      returnFocusRef.current = activeBeforeMount;
    }
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    const initial =
      root.querySelector<HTMLElement>("[data-dialog-autofocus]") ??
      focusableElements(root)[0] ??
      root;
    initial.focus({ preventScroll: true });

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        if (closeDisabledRef.current) return;
        event.preventDefault();
        event.stopPropagation();
        onCloseRef.current();
        return;
      }
      if (event.key !== "Tab") return;

      const elements = focusableElements(root);
      if (elements.length === 0) {
        event.preventDefault();
        root?.focus();
        return;
      }

      const active = document.activeElement;
      const activeIndex =
        active instanceof HTMLElement ? elements.indexOf(active) : -1;
      const next = nextFocusIndex(activeIndex, elements.length, event.shiftKey);
      event.preventDefault();
      elements[next].focus();
    }

    document.addEventListener("keydown", onKeyDown, true);
    return () => {
      document.removeEventListener("keydown", onKeyDown, true);
      document.body.style.overflow = previousOverflow;
      // StrictMode mounts effects twice in development. The first cleanup
      // runs while the dialog is still in the document; restoring focus then
      // would blur the field the second mount is about to focus.
      if (!root.isConnected) {
        const target = returnFocusRef.current;
        returnFocusRef.current = null;
        if (target?.isConnected) {
          target.focus({ preventScroll: true });
        }
      }
    };
  }, [ref]);
}
