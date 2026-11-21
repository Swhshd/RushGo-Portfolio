"use client";

import {
  useEffect,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
  type RefObject,
} from "react";
import Link from "next/link";
import {
  CalendarDays,
  BarChart3,
  CircleAlert,
  Flag,
  LayoutGrid,
  Menu,
  Search,
  Send,
  Timer,
  X,
} from "lucide-react";
import { focusableElements, nextFocusIndex } from "@/lib/dialog-focus";
import { useI18n } from "@/lib/i18n";
import { LanguageSwitcher } from "@/components/language-switcher";

const DESKTOP_MEDIA_QUERY = "(min-width: 1024px)";

const NAV_ITEMS = [
  { href: "/admin", label: "admin.nav.overview", icon: LayoutGrid, soon: false },
  { href: "/admin/events", label: "admin.nav.events", icon: CalendarDays, soon: false },
  { href: "/admin/operations", label: "admin.nav.exceptions", icon: CircleAlert, soon: false },
  { href: "/admin/orders", label: "admin.nav.orderLookup", icon: Search, soon: false },
  { href: "/admin/ops", label: "admin.nav.cronJobs", icon: Timer, soon: false },
  { href: "/admin/outbox", label: "admin.nav.outboxEvents", icon: Send, soon: false },
  { href: "/admin/metrics", label: "admin.nav.metrics", icon: BarChart3, soon: false },
  { href: "/admin/flags", label: "admin.nav.featureFlags", icon: Flag, soon: false },
] as const;

type AdminNavigationLinksProps = {
  pathname: string;
  variant: "desktop" | "mobile";
  onCurrentPageClick?: () => void;
};

export function AdminNavigationLinks({
  pathname,
  variant,
  onCurrentPageClick,
}: AdminNavigationLinksProps) {
  const mobile = variant === "mobile";
  const { t } = useI18n();

  return (
    <div className="flex flex-col gap-1">
      <div className="px-2 pb-1.5 pt-3 text-[11px] font-semibold uppercase tracking-wider text-muted">
        {t("admin.nav.operations")}
      </div>
      {NAV_ITEMS.map((item) => {
        const active =
          item.href === "/admin"
            ? pathname === "/admin"
            : pathname.startsWith(item.href);
        const destination = item.soon ? "/admin/ops" : item.href;

        return (
          <Link
            key={item.href}
            href={destination}
            aria-disabled={item.soon || undefined}
            tabIndex={item.soon ? -1 : undefined}
            aria-current={active && !item.soon ? "page" : undefined}
            data-admin-nav-autofocus={mobile && active ? "" : undefined}
            onClick={() => {
              if (destination === pathname) onCurrentPageClick?.();
            }}
            className={`flex items-center gap-2.5 rounded-lg font-medium transition-colors ${
              mobile
                ? "min-h-11 px-3 py-2.5 text-[14px]"
                : "px-2.5 py-2 text-[13px]"
            } ${
              active && !item.soon
                ? "bg-tag-bg text-foreground font-semibold"
                : "text-muted hover:bg-tag-bg hover:text-foreground"
            } ${item.soon ? "opacity-80" : ""}`}
          >
            <item.icon size={16} className="shrink-0" />
            <span className="min-w-0 flex-1">{t(item.label)}</span>
            {item.soon && (
              <span className="rounded-full bg-tag-bg px-1.5 py-0.5 text-[10px]">
                {t("admin.nav.soon")}
              </span>
            )}
          </Link>
        );
      })}
    </div>
  );
}

type CloseReason = "manual" | "route" | "breakpoint" | "unmount";

export function AdminMobileNavigation({
  pathname,
  contentRef,
}: {
  pathname: string;
  contentRef: RefObject<HTMLElement | null>;
}) {
  const { t } = useI18n();
  const [isOpen, setIsOpen] = useState(false);
  const dialogRef = useRef<HTMLDialogElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const previousPathnameRef = useRef(pathname);
  const closeReasonRef = useRef<CloseReason>("manual");
  const previousBodyOverflowRef = useRef<string | null>(null);
  const focusFrameRef = useRef<number | null>(null);
  const closingRef = useRef(false);

  function restoreBodyScroll() {
    const previousOverflow = previousBodyOverflowRef.current;
    if (previousOverflow === null) return;

    previousBodyOverflowRef.current = null;
    if (document.body.style.overflow === "hidden") {
      document.body.style.overflow = previousOverflow;
    }
  }

  function scheduleFocus(callback: () => void) {
    if (focusFrameRef.current !== null) {
      cancelAnimationFrame(focusFrameRef.current);
    }
    focusFrameRef.current = requestAnimationFrame(() => {
      focusFrameRef.current = null;
      callback();
    });
  }

  function focusAfterClose(reason: CloseReason) {
    if (reason === "manual") {
      scheduleFocus(() => {
        const trigger = triggerRef.current;
        if (
          trigger?.isConnected &&
          trigger.getClientRects().length > 0 &&
          !window.matchMedia(DESKTOP_MEDIA_QUERY).matches
        ) {
          trigger.focus({ preventScroll: true });
        }
      });
      return;
    }

    if (reason === "route") {
      scheduleFocus(() => contentRef.current?.focus({ preventScroll: true }));
      return;
    }

    if (reason === "breakpoint") {
      scheduleFocus(() => {
        if (!window.matchMedia(DESKTOP_MEDIA_QUERY).matches) return;
        const currentLink = document.querySelector<HTMLElement>(
          "[data-admin-desktop-navigation] a[aria-current='page']",
        );
        currentLink?.focus({ preventScroll: true });
      });
    }
  }

  function closeNavigation(reason: CloseReason = "manual") {
    const dialog = dialogRef.current;
    if (!dialog?.open) return;
    closeReasonRef.current = reason;
    closingRef.current = true;
    dialog.close();
  }

  function trapDialogTab(event: ReactKeyboardEvent<HTMLDialogElement>) {
    if (event.key !== "Tab") return;
    const dialog = dialogRef.current;
    if (!dialog) return;

    const tabStops = focusableElements(dialog).filter(
      (element) => element.tabIndex >= 0,
    );
    if (tabStops.length === 0) {
      event.preventDefault();
      dialog.focus({ preventScroll: true });
      return;
    }

    const active = document.activeElement;
    const activeIndex =
      active instanceof HTMLElement ? tabStops.indexOf(active) : -1;
    const shouldWrap =
      activeIndex < 0 ||
      (event.shiftKey && activeIndex === 0) ||
      (!event.shiftKey && activeIndex === tabStops.length - 1);
    if (!shouldWrap) return;

    event.preventDefault();
    const nextIndex = nextFocusIndex(
      activeIndex,
      tabStops.length,
      event.shiftKey,
    );
    tabStops[nextIndex]?.focus({ preventScroll: true });
  }

  function openNavigation() {
    const dialog = dialogRef.current;
    if (
      !dialog ||
      dialog.open ||
      closingRef.current ||
      window.matchMedia(DESKTOP_MEDIA_QUERY).matches
    ) {
      return;
    }

    // The event and form drawers are modal too. The native dialog makes the
    // rest of the document inert, while this guard prevents nested scroll and
    // focus restoration when an existing drawer is open.
    const existingNativeModal = document.querySelector("dialog:modal");
    const otherModal = document.querySelector(
      '[role="dialog"][aria-modal="true"], [data-admin-ops-edit-drawer]',
    );
    if (existingNativeModal || otherModal) return;

    if (focusFrameRef.current !== null) {
      cancelAnimationFrame(focusFrameRef.current);
      focusFrameRef.current = null;
    }

    closeReasonRef.current = "manual";
    dialog.showModal();
    previousBodyOverflowRef.current = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    setIsOpen(true);
    dialog
      .querySelector<HTMLElement>("[data-admin-nav-autofocus]")
      ?.focus({ preventScroll: true });
  }

  useEffect(() => {
    if (previousPathnameRef.current === pathname) return;
    previousPathnameRef.current = pathname;
    const dialog = dialogRef.current;
    if (dialog?.open) {
      closeReasonRef.current = "route";
      closingRef.current = true;
      dialog.close();
    }
  }, [pathname]);

  useEffect(() => {
    const desktopMedia = window.matchMedia(DESKTOP_MEDIA_QUERY);
    function closeOnDesktop(event: MediaQueryListEvent) {
      const dialog = dialogRef.current;
      if (event.matches && dialog?.open) {
        closeReasonRef.current = "breakpoint";
        closingRef.current = true;
        dialog.close();
      }
    }

    desktopMedia.addEventListener("change", closeOnDesktop);
    return () => desktopMedia.removeEventListener("change", closeOnDesktop);
  }, []);

  useEffect(
    () => () => {
      closeReasonRef.current = "unmount";
      if (focusFrameRef.current !== null) {
        cancelAnimationFrame(focusFrameRef.current);
      }
      const dialog = dialogRef.current;
      if (dialog?.open) {
        closingRef.current = true;
        dialog.close();
      }
      const previousOverflow = previousBodyOverflowRef.current;
      previousBodyOverflowRef.current = null;
      if (
        previousOverflow !== null &&
        document.body.style.overflow === "hidden"
      ) {
        document.body.style.overflow = previousOverflow;
      }
    },
    [],
  );

  return (
    <>
      <header className="sticky top-0 z-20 flex h-14 shrink-0 items-center justify-between border-b border-border bg-card px-3 lg:hidden">
        <div className="flex min-w-0 items-center gap-2.5">
          <span className="grid h-7 w-7 shrink-0 place-items-center rounded-lg bg-primary font-mono text-xs font-bold text-graphite">
            RG
          </span>
          <span className="truncate font-mono text-sm font-bold tracking-tight">
            {t("admin.nav.console")}
          </span>
        </div>
        <LanguageSwitcher />
        <button
          ref={triggerRef}
          type="button"
          aria-label={t("admin.nav.open")}
          aria-expanded={isOpen}
          aria-controls="admin-mobile-navigation"
          onClick={openNavigation}
          className="grid h-11 w-11 shrink-0 place-items-center rounded-lg text-foreground hover:bg-tag-bg focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
        >
          <Menu size={20} aria-hidden="true" />
        </button>
      </header>

      <dialog
        ref={dialogRef}
        id="admin-mobile-navigation"
        aria-labelledby="admin-mobile-navigation-heading"
        onKeyDown={trapDialogTab}
        onCancel={(event) => {
          event.preventDefault();
          closeNavigation("manual");
        }}
        onClose={() => {
          const reason = closeReasonRef.current;
          closingRef.current = false;
          if (reason !== "unmount") setIsOpen(false);
          restoreBodyScroll();
          focusAfterClose(reason);
          closeReasonRef.current = "manual";
        }}
        className="fixed inset-0 m-0 h-[100dvh] max-h-none w-full max-w-none overflow-hidden border-0 bg-transparent p-0 text-foreground backdrop:bg-black/55 lg:hidden"
      >
        <div className="relative h-full w-full">
          <button
            type="button"
            aria-label={t("admin.nav.closeBackdrop")}
            tabIndex={-1}
            onClick={() => closeNavigation("manual")}
            className="absolute inset-0 h-full w-full cursor-default bg-transparent"
          />
          <section data-admin-navigation-panel className="relative z-10 flex h-full w-[min(20rem,calc(100vw_-_2rem))] max-w-[calc(100%_-_2rem)] flex-col border-r border-border bg-card shadow-xl">
            <div className="flex h-14 shrink-0 items-center justify-between border-b border-border px-4">
              <h2
                id="admin-mobile-navigation-heading"
                className="text-sm font-semibold"
              >
                {t("admin.layout.navigation")}
              </h2>
              <button
                type="button"
                aria-label={t("admin.nav.close")}
                onClick={() => closeNavigation("manual")}
                className="grid h-11 w-11 place-items-center rounded-lg text-muted hover:bg-tag-bg hover:text-foreground focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
              >
                <X size={18} aria-hidden="true" />
              </button>
            </div>
            <nav
              aria-label={t("admin.layout.navigation")}
              className="min-h-0 flex-1 overflow-y-auto px-3 py-2"
            >
              <AdminNavigationLinks
                pathname={pathname}
                variant="mobile"
                onCurrentPageClick={() => closeNavigation("manual")}
              />
            </nav>
            <div className="flex min-h-14 shrink-0 flex-col gap-3 border-t border-border px-5 py-3 text-xs text-muted">
              <div data-admin-mobile-language className="flex w-full items-center">
                <LanguageSwitcher />
              </div>
              <div className="flex items-center gap-2">
                <span className="h-2 w-2 shrink-0 rounded-full bg-muted" />
                <span className="break-words">{t("admin.layout.restrictedAccess")}</span>
              </div>
            </div>
          </section>
        </div>
      </dialog>
    </>
  );
}
