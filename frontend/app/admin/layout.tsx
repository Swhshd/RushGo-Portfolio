"use client";

/**
 * Admin shell — Ops sidebar + auth guard.
 *
 * Unauthenticated visitors are bounced to /login with a ?redirect= back
 * here; authenticated non-admins get a 403 panel. The customer Navbar
 * lives in the (customer) route group, so it never renders here.
 */

import { useEffect, useRef } from "react";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { Loader2 } from "lucide-react";
import { useAuth } from "@/lib/auth-context";
import { useI18n } from "@/lib/i18n";
import { LanguageSwitcher } from "@/components/language-switcher";
import {
  AdminMobileNavigation,
  AdminNavigationLinks,
} from "@/components/admin-navigation";

export default function AdminLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const { user, isLoading } = useAuth();
  const { t } = useI18n();
  const router = useRouter();
  const pathname = usePathname();
  const contentRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!isLoading && !user) {
      router.replace(`/login?redirect=${encodeURIComponent(pathname)}`);
    }
  }, [isLoading, user, router, pathname]);

  if (isLoading) {
    return (
      <div role="status" aria-live="polite" className="flex items-center justify-center gap-3 h-screen">
        <Loader2 size={32} className="animate-spin text-primary" />
        <span className="sr-only">{t("admin.layout.loadingSession")}</span>
      </div>
    );
  }

  if (!user) {
    return (
      <div role="status" aria-live="polite" className="flex items-center justify-center gap-3 h-screen">
        <Loader2 size={32} className="animate-spin text-primary" />
        <span className="sr-only">{t("admin.layout.loadingSession")}</span>
      </div>
    );
  }

  const eventRefundApprovalsPath = /^\/admin\/events\/[0-9a-f-]+\/refunds$/i.test(pathname);

  if (user.role !== "admin" && eventRefundApprovalsPath) {
    return <main className="min-h-screen min-w-0 bg-background">{children}</main>;
  }

  if (user.role !== "admin") {
    return (
      <div className="flex flex-col items-center justify-center h-screen gap-3 text-center px-6">
        <h1 className="text-2xl font-bold">{t("admin.layout.forbiddenTitle")}</h1>
        <p className="text-muted text-sm max-w-sm">
          {t("admin.layout.forbiddenBody", { email: user.email ?? "", role: user.role })}
        </p>
        <Link
          href="/"
          className="bg-primary hover:bg-primary-hover text-graphite font-medium rounded-full px-6 h-11 flex items-center text-sm transition-colors mt-2"
        >
          {t("admin.common.backToRushGo")}
        </Link>
      </div>
    );
  }

  return (
    <div className="flex min-h-screen min-w-0 flex-col lg:flex-row">
      {/* Sidebar */}
      <aside className="sticky top-0 hidden h-screen w-60 shrink-0 flex-col border-r border-border bg-card lg:flex">
        <div className="flex items-center gap-2.5 px-5 pt-5 pb-4">
          <div className="w-7 h-7 bg-primary text-graphite rounded-lg grid place-items-center font-mono font-bold text-xs">
            RG
          </div>
          <div>
            <div className="font-mono font-bold tracking-tight text-[15px]">
              RushGo
            </div>
            <div className="text-[11px] text-muted">{t("admin.layout.internalOpsConsole")}</div>
          </div>
        </div>

        <nav
          aria-label={t("admin.layout.navigation")}
          data-admin-desktop-navigation
          className="flex-1 px-3 py-2"
        >
          <AdminNavigationLinks pathname={pathname} variant="desktop" />
        </nav>

        <div className="flex flex-col gap-3 px-5 py-4 border-t border-border text-xs text-muted">
          <LanguageSwitcher />
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-muted shrink-0" />
            {t("admin.layout.restrictedAccess")}
          </div>
        </div>
      </aside>

      <AdminMobileNavigation pathname={pathname} contentRef={contentRef} />

      {/* Content — admin pages render their own topbar */}
      <div
        ref={contentRef}
        data-admin-content
        tabIndex={-1}
        className="flex min-w-0 flex-1 flex-col bg-background"
      >
        {children}
      </div>
    </div>
  );
}
