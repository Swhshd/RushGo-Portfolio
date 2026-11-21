"use client";

import { createContext, useContext, useMemo, useSyncExternalStore, type ReactNode } from "react";
import { getI18n, type I18nDisplay, type Locale } from "./core";
import { getLocaleSnapshot, getServerLocaleSnapshot, setLocale, subscribeLocale } from "./locale-store";

export type I18nContextValue = I18nDisplay & { setLocale: (locale: Locale) => void };

const fallbackContextValue: I18nContextValue = {
  ...getI18n("en-US"),
  setLocale,
};
const I18nContext = createContext<I18nContextValue>(fallbackContextValue);

export function I18nProvider({ children }: { children: ReactNode }) {
  const locale = useSyncExternalStore(subscribeLocale, getLocaleSnapshot, getServerLocaleSnapshot);
  const display = getI18n(locale);
  const value = useMemo(() => ({ ...display, setLocale }), [display]);
  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}

export function useI18n(): I18nContextValue {
  return useContext(I18nContext);
}
