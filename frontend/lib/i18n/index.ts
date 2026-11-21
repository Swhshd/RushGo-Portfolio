"use client";

export { I18nProvider, useI18n, type I18nContextValue } from "./context";
export { customerCategoryTranslationKey, getI18n, isLocale, translationResources, type I18nDisplay, type Locale, type TranslationKey, type TranslationParams } from "./core";
export { adminEn, adminZh } from "./admin";
export { commonEn, commonZh } from "./common";
export { customerEn, customerZh } from "./customer";
export { DEFAULT_LOCALE, LOCALE_STORAGE_KEY, getLocaleSnapshot, getServerLocaleSnapshot, setLocale, subscribeLocale } from "./locale-store";
