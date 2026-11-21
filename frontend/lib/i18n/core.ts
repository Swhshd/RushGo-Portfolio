import { adminEn, adminZh } from "./admin";
import { commonEn, commonZh } from "./common";
import { customerEn, customerZh } from "./customer";
import { aftersalesEn, aftersalesZh } from "./aftersales";
import { workflowEn, workflowZh } from "./workflow";

export type Locale = "en-US" | "zh-CN";
export type TranslationParams = Record<string, string | number>;

const enMessages = { ...commonEn, ...customerEn, ...adminEn, ...aftersalesEn, ...workflowEn };
const zhMessages: Record<keyof typeof enMessages, string> = {
  ...commonZh,
  ...customerZh,
  ...adminZh,
  ...aftersalesZh,
  ...workflowZh,
};

export type TranslationKey = keyof typeof enMessages;

export const translationResources: Record<Locale, Record<TranslationKey, string>> = {
  "en-US": enMessages,
  "zh-CN": zhMessages,
};

export function isLocale(value: unknown): value is Locale {
  return value === "en-US" || value === "zh-CN";
}

export function customerCategoryTranslationKey(category: string): TranslationKey | null {
  switch (category.trim().toLowerCase()) {
    case "":
    case "discover":
      return "customer.navigation.discover";
    case "concerts":
      return "customer.navigation.concerts";
    case "sports":
      return "customer.navigation.sports";
    case "films":
      return "customer.navigation.films";
    default:
      return null;
  }
}

export interface I18nDisplay {
  locale: Locale;
  t: (key: TranslationKey, params?: TranslationParams) => string;
  text: (knownSafeMessage: string, fallbackKey?: TranslationKey) => string;
  number: (value: number, options?: Intl.NumberFormatOptions) => string;
  date: (value: string | number | Date | null | undefined, options?: Intl.DateTimeFormatOptions) => string;
  usdCents: (cents: number) => string;
}

const safeFrontendMessageKeys = new Map<string, TranslationKey>();
for (const [key, message] of Object.entries(enMessages) as [TranslationKey, string][]) {
  if (!safeFrontendMessageKeys.has(message)) safeFrontendMessageKeys.set(message, key);
}

function interpolate(template: string, params?: TranslationParams): string {
  return template.replace(/\{([a-zA-Z0-9_]+)\}/g, (placeholder, name: string) =>
    params && Object.hasOwn(params, name) ? String(params[name]) : placeholder,
  );
}

const displays = new Map<Locale, I18nDisplay>();

export function getI18n(locale: Locale): I18nDisplay {
  const cached = displays.get(locale);
  if (cached) return cached;
  const dictionary = translationResources[locale];
  const display: I18nDisplay = {
    locale,
    t: (key, params) => interpolate(dictionary[key], params),
    text: (knownSafeMessage, fallbackKey = "common.errors.unexpected") => {
      const exactKey = safeFrontendMessageKeys.get(knownSafeMessage);
      return interpolate(dictionary[exactKey ?? fallbackKey]);
    },
    number: (value, options) => new Intl.NumberFormat(locale, options).format(value),
    date: (value, options) => {
      if (value === null || value === undefined) return "";
      const date = value instanceof Date ? value : new Date(value);
      if (!Number.isFinite(date.getTime())) return "";
      return new Intl.DateTimeFormat(locale, options).format(date);
    },
    usdCents: (cents) => {
      if (!Number.isFinite(cents)) return "—";
      return new Intl.NumberFormat(locale, { style: "currency", currency: "USD" }).format(cents / 100);
    },
  };
  displays.set(locale, display);
  return display;
}
