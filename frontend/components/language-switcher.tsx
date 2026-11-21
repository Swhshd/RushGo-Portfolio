"use client";

import { useI18n } from "@/lib/i18n/index";

export function LanguageSwitcher() {
  const { locale, setLocale, t } = useI18n();
  return (
    <label className="inline-flex min-w-0 items-center gap-2 text-xs font-medium text-muted">
      <span className="sr-only">{t("common.language.label")}</span>
      <select
        aria-label={t("common.language.label")}
        value={locale}
        onChange={(event) => setLocale(event.target.value as "en-US" | "zh-CN")}
        className="h-9 max-w-full rounded-lg border border-control-border bg-tag-bg px-2 text-xs text-foreground focus:border-primary focus:outline-none"
      >
        <option value="en-US">{t("common.language.english")}</option>
        <option value="zh-CN">简体中文</option>
      </select>
    </label>
  );
}
