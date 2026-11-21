import { getI18n, isLocale, type Locale } from "./core";

export const LOCALE_STORAGE_KEY = "rushgo:locale";
export const DEFAULT_LOCALE: Locale = "en-US";

let currentLocale: Locale = DEFAULT_LOCALE;
let preferenceReconciled = false;
const listeners = new Set<() => void>();
let documentTitleObserver: MutationObserver | null = null;

function updateDocumentTitle(locale: Locale): void {
  if (typeof document === "undefined") return;
  const supportedLocales: Locale[] = ["en-US", "zh-CN"];
  const genericTitles = supportedLocales.map((supportedLocale) =>
    getI18n(supportedLocale).t("common.metadata.title"),
  );
  if (genericTitles.includes(document.title)) {
    const localizedTitle = getI18n(locale).t("common.metadata.title");
    if (document.title !== localizedTitle) document.title = localizedTitle;
  }
}

function updateDocumentLanguage(locale: Locale): void {
  if (typeof document === "undefined") return;
  document.documentElement.lang = locale;
  updateDocumentTitle(locale);
}

function startDocumentTitleObserver(): void {
  if (
    documentTitleObserver ||
    typeof document === "undefined" ||
    typeof MutationObserver === "undefined" ||
    !document.head
  ) return;
  documentTitleObserver = new MutationObserver(() => updateDocumentTitle(currentLocale));
  documentTitleObserver.observe(document.head, {
    childList: true,
    subtree: true,
    characterData: true,
  });
}

function stopDocumentTitleObserver(): void {
  documentTitleObserver?.disconnect();
  documentTitleObserver = null;
}

function publish(locale: Locale): void {
  if (currentLocale === locale) {
    updateDocumentLanguage(locale);
    return;
  }
  currentLocale = locale;
  updateDocumentLanguage(locale);
  listeners.forEach((listener) => listener());
}

function readPreference(): Locale {
  try {
    const stored = window.localStorage.getItem(LOCALE_STORAGE_KEY);
    return isLocale(stored) ? stored : DEFAULT_LOCALE;
  } catch {
    return DEFAULT_LOCALE;
  }
}

function handleStorage(event: StorageEvent): void {
  if (event.key !== LOCALE_STORAGE_KEY && event.key !== null) return;
  if (event.storageArea) {
    try {
      if (event.storageArea !== window.localStorage) return;
    } catch {
      return;
    }
  }
  preferenceReconciled = true;
  publish(isLocale(event.newValue) ? event.newValue : DEFAULT_LOCALE);
}

export function getLocaleSnapshot(): Locale {
  return currentLocale;
}

export function getServerLocaleSnapshot(): Locale {
  return DEFAULT_LOCALE;
}

export function subscribeLocale(listener: () => void): () => void {
  listeners.add(listener);
  if (listeners.size === 1) {
    if (typeof window !== "undefined") window.addEventListener("storage", handleStorage);
    startDocumentTitleObserver();
  }
  if (!preferenceReconciled && typeof window !== "undefined") {
    preferenceReconciled = true;
    publish(readPreference());
  }
  return () => {
    listeners.delete(listener);
    if (listeners.size === 0) {
      if (typeof window !== "undefined") window.removeEventListener("storage", handleStorage);
      stopDocumentTitleObserver();
    }
  };
}

export function setLocale(locale: Locale): void {
  if (!isLocale(locale)) return;
  preferenceReconciled = true;
  publish(locale);
  try {
    if (typeof window !== "undefined") window.localStorage.setItem(LOCALE_STORAGE_KEY, locale);
  } catch {
    // Keep the current page usable when browser preference storage is blocked.
  }
}
