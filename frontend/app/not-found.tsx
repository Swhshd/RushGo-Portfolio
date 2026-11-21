"use client";

import Link from "next/link";
import { useI18n } from "@/lib/i18n/index";
import { LanguageSwitcher } from "@/components/language-switcher";

export default function NotFound() {
  const { t } = useI18n();
  return (
    <main className="relative flex min-h-[60vh] flex-col items-center justify-center gap-4 px-5 pt-16 text-center">
      <div className="absolute right-5 top-5">
        <LanguageSwitcher />
      </div>
      <p className="font-mono text-xs uppercase tracking-[0.2em] text-muted">404</p>
      <h1 className="text-3xl font-bold">{t("customer.notFound.title")}</h1>
      <p className="max-w-md text-sm text-muted">{t("customer.notFound.description")}</p>
      <Link href="/" className="rounded-lg bg-primary px-5 py-3 text-sm font-semibold text-graphite">
        {t("customer.notFound.home")}
      </Link>
    </main>
  );
}
