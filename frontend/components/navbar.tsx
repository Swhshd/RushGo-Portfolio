"use client";

import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { Search, User, LogOut, X } from "lucide-react";
import { useAuth } from "@/lib/auth-context";
import { useState, useSyncExternalStore } from "react";
import { discoveryCategories, discoveryHref, readDiscovery, type DiscoveryState } from "@/lib/discovery";
import { customerCategoryTranslationKey } from "@/lib/i18n/index";
import { useI18n } from "@/lib/i18n/index";
import { LanguageSwitcher } from "@/components/language-switcher";

const subscribeToHydration = () => () => {};
const clientSnapshot = () => true;
const serverSnapshot = () => false;

function DiscoverySearch({ state }: { state: DiscoveryState }) {
  const { t } = useI18n();
  const router = useRouter();
  const [draft, setDraft] = useState(state.search);
  const [submittedSearch, setSubmittedSearch] = useState(state.search);
  // Sync URL history without replacing the input node or a category-only draft.
  if (submittedSearch !== state.search) {
    setSubmittedSearch(state.search);
    setDraft(state.search);
  }
  function clear() {
    setDraft("");
    if (state.search) router.push(discoveryHref(state, { search: "" }));
  }
  return (
    <form role="search" aria-label={t("customer.navigation.search")} onSubmit={(event) => {
      event.preventDefault();
      setDraft(draft.trim());
      router.push(discoveryHref(state, { search: draft }));
    }} className="order-4 xl:order-3 w-full sm:flex-1 xl:max-w-sm flex items-center gap-2 bg-tag-bg border border-control-border rounded-lg px-3 h-10 focus-within:border-primary transition-colors">
      <button type="submit" aria-label={t("customer.navigation.searchButton")} className="text-muted hover:text-foreground shrink-0"><Search size={16} /></button>
      <input aria-label={t("customer.navigation.search")} name="search" type="search" placeholder={t("customer.navigation.searchPlaceholder")} value={draft}
        onChange={(event) => setDraft(event.target.value)}
        onKeyDown={(event) => { if (event.key === "Escape") { event.preventDefault(); clear(); } }}
        className="w-full min-w-0 bg-transparent text-sm text-foreground placeholder:text-muted outline-none" />
      {(draft || state.search) && <button type="button" aria-label={t("customer.navigation.clearSearch")} onClick={clear} className="text-muted hover:text-foreground shrink-0"><X size={16} /></button>}
    </form>
  );
}

export function Navbar() {
  const { t } = useI18n();
  const pathname = usePathname();
  const router = useRouter();
  const params = useSearchParams();
  const discovery = readDiscovery(params);
  const { user, isLoading, logout } = useAuth();
  const hydrated = useSyncExternalStore(subscribeToHydration, clientSnapshot, serverSnapshot);
  const [logoutError, setLogoutError] = useState(false);

  async function handleLogout() {
    setLogoutError(false);
    try { await logout(); router.push("/"); } catch { setLogoutError(true); }
  }

  return (
    <nav aria-label={t("customer.navigation.main")} className="flex flex-wrap items-center gap-x-6 gap-y-3 min-h-16 px-5 py-3 md:px-12 bg-card text-foreground border-b border-border w-full shrink-0">
        {logoutError && <p role="status" className="w-full text-sm text-danger-ink">{t("customer.navigation.logoutError")}</p>}
        <Link href="/" className="order-1 text-primary font-extrabold text-xl font-mono tracking-wide">
          RushGo
        </Link>
        <div className="order-3 xl:order-2 flex w-full sm:w-auto items-center justify-between sm:justify-start gap-4 md:gap-6">
          {discoveryCategories.map((link) => (
            <Link
              key={link.label}
              href={discoveryHref(discovery, { category: link.category })}
              aria-current={pathname === "/" && discovery.category === link.category ? "page" : undefined}
              className={`text-sm font-medium pb-1 border-b-2 transition-colors hover:text-primary ${
                pathname === "/" && discovery.category === link.category ? "text-primary border-primary" : "text-muted border-transparent"
              }`}
            >
              {customerCategoryTranslationKey(link.category) ? t(customerCategoryTranslationKey(link.category)!) : link.label}
            </Link>
          ))}
        </div>
      <DiscoverySearch state={discovery} />
      <div className="order-2 xl:order-4 ml-auto flex w-full flex-wrap items-center justify-end gap-3 sm:w-auto">
        <LanguageSwitcher />
        {!hydrated || isLoading ? null : user ? (
          <div className="flex min-w-0 flex-wrap items-center justify-end gap-3">
            <Link
              href="/my-tickets"
              aria-current={pathname === "/my-tickets" ? "page" : undefined}
              className="text-sm font-medium text-muted-on-dark hover:text-primary transition-colors"
            >
              {t("customer.navigation.myTickets")}
            </Link>
            <Link href="/favorites" aria-current={pathname === "/favorites" ? "page" : undefined} className="text-sm font-medium text-muted-on-dark hover:text-primary transition-colors">{t("customer.interests.manage")}</Link>
            <Link href="/notifications" aria-current={pathname === "/notifications" ? "page" : undefined} className="text-sm font-medium text-muted-on-dark hover:text-primary transition-colors">{t("customer.navigation.notifications")}</Link>
            <Link href="/work" aria-current={pathname === "/work" ? "page" : undefined} className="text-sm font-medium text-muted-on-dark hover:text-primary transition-colors">{t("customer.navigation.work")}</Link>
            <Link href="/account" aria-current={pathname === "/account" ? "page" : undefined} className="text-sm font-medium text-muted-on-dark hover:text-primary transition-colors">{t("customer.navigation.account")}</Link>
            {user.role === "admin" ? <Link href="/admin/events" className="text-sm font-medium text-muted-on-dark hover:text-primary transition-colors">{t("aftersales.manageEvents")}</Link> : null}
            <Link href="/aftersales" aria-current={pathname === "/aftersales" ? "page" : undefined} className="text-sm font-medium text-muted-on-dark hover:text-primary transition-colors">
              {t("aftersales.title")}
            </Link>
            <span className="hidden max-w-48 truncate lg:inline text-sm font-medium" title={user.name}>{user.name}</span>
            <button
              onClick={handleLogout}
              className="flex items-center justify-center w-9 h-9 rounded-lg bg-dark-surface border border-dark-border hover:text-primary transition-colors"
              title={t("customer.navigation.logout")}
              aria-label={t("customer.navigation.logout")}
            >
              <LogOut size={16} />
            </button>
          </div>
        ) : (
          <Link
            href="/login"
            className="flex items-center justify-center gap-2 bg-primary hover:bg-primary-hover text-graphite font-semibold rounded-lg px-5 h-10 text-sm transition-colors"
          >
            <User size={14} />
            {t("customer.navigation.signIn")}
          </Link>
        )}
      </div>
    </nav>
  );
}
