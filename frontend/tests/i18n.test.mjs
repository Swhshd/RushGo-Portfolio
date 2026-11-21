import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { browserOverrides, loadTypeScript } from "./render-typescript.mjs";
import { componentDriver, find } from "./recovery-driver.mjs";

function loadOrEmpty(file, overrides) {
  try {
    return loadTypeScript(file, overrides);
  } catch {
    return {};
  }
}

function createWindow(storageValues = new Map()) {
  const listeners = new Map();
  const storage = {
    getItem(key) { return storageValues.get(key) ?? null; },
    setItem(key, value) { storageValues.set(key, String(value)); },
  };
  return {
    storageValues,
    listeners,
    window: {
      localStorage: storage,
      addEventListener(type, listener) {
        if (!listeners.has(type)) listeners.set(type, new Set());
        listeners.get(type).add(listener);
      },
      removeEventListener(type, listener) { listeners.get(type)?.delete(listener); },
    },
  };
}

test("English and Simplified Chinese resources have identical keys and placeholders", () => {
  const { translationResources } = loadOrEmpty("lib/i18n/index.ts", { react: React });
  assert.ok(translationResources, "the shared i18n module must export combined resources");
  const englishKeys = Object.keys(translationResources["en-US"]).sort();
  const chineseKeys = Object.keys(translationResources["zh-CN"]).sort();
  assert.deepEqual(chineseKeys, englishKeys);
  const placeholders = (value) => [...value.matchAll(/\{([a-zA-Z0-9_]+)\}/g)].map((match) => match[1]).sort();
  for (const key of englishKeys) {
    assert.deepEqual(placeholders(translationResources["zh-CN"][key]), placeholders(translationResources["en-US"][key]), key);
  }
});

test("server and first hydration snapshots stay English before persisted preference reconciliation", () => {
  const store = loadOrEmpty("lib/i18n/locale-store.ts");
  assert.equal(typeof store.getServerLocaleSnapshot, "function", "the locale store must expose a stable server snapshot");
  assert.equal(store.getServerLocaleSnapshot(), "en-US");
  assert.equal(store.getLocaleSnapshot(), "en-US");
});

test("persisted locale is reconciled after subscription and same-origin storage changes notify subscribers", () => {
  const priorWindow = globalThis.window;
  const priorDocument = globalThis.document;
  const { storageValues, listeners, window } = createWindow(new Map([["rushgo:locale", "zh-CN"]]));
  globalThis.window = window;
  globalThis.document = { title: "RushGo — Event Ticketing", documentElement: { lang: "en-US" } };
  try {
    const store = loadOrEmpty("lib/i18n/locale-store.ts");
    assert.equal(typeof store.subscribeLocale, "function", "the locale store must support React subscriptions");
    let notifications = 0;
    const unsubscribe = store.subscribeLocale(() => { notifications += 1; });
    assert.equal(store.getLocaleSnapshot(), "zh-CN");
    assert.equal(globalThis.document.title, "RushGo — 活动票务");
    assert(notifications > 0, "hydration reconciliation should notify the current tab");

    store.setLocale("en-US");
    assert.equal(storageValues.get("rushgo:locale"), "en-US");
    assert.equal(store.getLocaleSnapshot(), "en-US");
    assert.equal(globalThis.document.documentElement.lang, "en-US");
    assert.equal(globalThis.document.title, "RushGo — Event Ticketing");

    globalThis.document.title = "Ticket details";
    store.setLocale("zh-CN");
    assert.equal(globalThis.document.title, "Ticket details", "locale changes must preserve a route-specific title");
    store.setLocale("en-US");
    globalThis.document.title = "RushGo — Event Ticketing";

    storageValues.set("rushgo:locale", "zh-CN");
    for (const listener of listeners.get("storage") ?? []) {
      listener({ key: "rushgo:locale", newValue: "en-US", storageArea: {} });
    }
    assert.equal(store.getLocaleSnapshot(), "en-US", "session or other storage events must be ignored");
    for (const listener of listeners.get("storage") ?? []) {
      listener({ key: "rushgo:locale", newValue: "zh-CN", storageArea: window.localStorage });
    }
    assert.equal(store.getLocaleSnapshot(), "zh-CN");
    assert.equal(globalThis.document.documentElement.lang, "zh-CN");
    assert.equal(globalThis.document.title, "RushGo — 活动票务");
    unsubscribe();
    assert.equal(listeners.get("storage")?.size, 0, "unsubscribing should remove the cross-tab listener");
  } finally {
    if (priorWindow === undefined) delete globalThis.window;
    else globalThis.window = priorWindow;
    if (priorDocument === undefined) delete globalThis.document;
    else globalThis.document = priorDocument;
  }
});

test("generic document title follows client navigation metadata while route titles stay owned by the route", () => {
  const priorWindow = globalThis.window;
  const priorDocument = globalThis.document;
  const priorMutationObserver = globalThis.MutationObserver;
  const observers = [];
  class FakeMutationObserver {
    constructor(callback) { this.callback = callback; observers.push(this); }
    observe(target, options) { this.target = target; this.options = options; }
    disconnect() { this.disconnected = true; }
  }
  const { window } = createWindow(new Map([["rushgo:locale", "zh-CN"]]));
  const document = {
    title: "RushGo — Event Ticketing",
    documentElement: { lang: "en-US" },
    head: {},
  };
  globalThis.window = window;
  globalThis.document = document;
  globalThis.MutationObserver = FakeMutationObserver;
  try {
    const store = loadOrEmpty("lib/i18n/locale-store.ts");
    const unsubscribe = store.subscribeLocale(() => {});
    assert.equal(document.title, "RushGo — 活动票务");
    assert.equal(observers.length, 1, "the active locale store should observe Next metadata updates");
    const observer = observers[0];
    assert.equal(observer.target, document.head);
    assert.deepEqual(observer.options, { childList: true, subtree: true, characterData: true });

    document.title = "RushGo — Event Ticketing";
    observer.callback([]);
    assert.equal(document.title, "RushGo — 活动票务", "generic English metadata should be reconciled to the selected locale");

    document.title = "Event details — RushGo";
    observer.callback([]);
    assert.equal(document.title, "Event details — RushGo", "route-specific titles should remain unchanged");

    unsubscribe();
    assert.equal(observer.disconnected, true, "the observer should stop when the final locale subscriber leaves");
  } finally {
    if (priorWindow === undefined) delete globalThis.window;
    else globalThis.window = priorWindow;
    if (priorDocument === undefined) delete globalThis.document;
    else globalThis.document = priorDocument;
    if (priorMutationObserver === undefined) delete globalThis.MutationObserver;
    else globalThis.MutationObserver = priorMutationObserver;
  }
});

test("invalid or inaccessible preference storage safely falls back to English", () => {
  const priorWindow = globalThis.window;
  const invalid = createWindow(new Map([["rushgo:locale", "fr-FR"]]));
  globalThis.window = invalid.window;
  try {
    const store = loadOrEmpty("lib/i18n/locale-store.ts");
    assert.equal(typeof store.subscribeLocale, "function", "the locale store must support safe preference reads");
    const unsubscribe = store.subscribeLocale(() => {});
    assert.equal(store.getLocaleSnapshot(), "en-US");
    unsubscribe();
  } finally {
    if (priorWindow === undefined) delete globalThis.window;
    else globalThis.window = priorWindow;
  }

  const inaccessible = createWindow();
  Object.defineProperty(inaccessible.window, "localStorage", {
    get() { throw new Error("storage unavailable"); },
  });
  globalThis.window = inaccessible.window;
  try {
    const store = loadOrEmpty("lib/i18n/locale-store.ts");
    assert.equal(typeof store.subscribeLocale, "function", "the locale store must tolerate unavailable storage");
    const unsubscribe = store.subscribeLocale(() => {});
    assert.equal(store.getLocaleSnapshot(), "en-US");
    store.setLocale("zh-CN");
    assert.equal(store.getLocaleSnapshot(), "zh-CN", "the current page should still switch when persistence is blocked");
    unsubscribe();
  } finally {
    if (priorWindow === undefined) delete globalThis.window;
    else globalThis.window = priorWindow;
  }
});

test("formatters follow locale while retaining USD and the same date instant", () => {
  const { getI18n } = loadOrEmpty("lib/i18n/index.ts", { react: React });
  assert.equal(typeof getI18n, "function", "the shared i18n module must expose pure display helpers");
  const english = getI18n("en-US");
  const chinese = getI18n("zh-CN");
  assert.equal(english.number(12345), new Intl.NumberFormat("en-US").format(12345));
  assert.equal(chinese.number(12345), new Intl.NumberFormat("zh-CN").format(12345));
  assert.equal(english.usdCents(123456), new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(1234.56));
  assert.equal(chinese.usdCents(123456), new Intl.NumberFormat("zh-CN", { style: "currency", currency: "USD" }).format(1234.56));
  const instant = "2026-11-01T12:34:00.000Z";
  const dateOptions = { dateStyle: "medium", timeStyle: "short", timeZone: "UTC" };
  assert.notEqual(english.date(instant, dateOptions), chinese.date(instant, dateOptions));
  assert.equal(english.date(instant, dateOptions), new Intl.DateTimeFormat("en-US", dateOptions).format(new Date(instant)));
  assert.equal(chinese.date(instant, dateOptions), new Intl.DateTimeFormat("zh-CN", dateOptions).format(new Date(instant)));
  assert.equal(chinese.date(null), "");
});

test("unknown backend text is hidden behind a localized fallback while reviewed frontend copy is translated exactly", () => {
  const { getI18n } = loadOrEmpty("lib/i18n/index.ts", { react: React });
  assert.equal(typeof getI18n, "function", "the shared i18n module must expose safe text mapping");
  const english = getI18n("en-US");
  const chinese = getI18n("zh-CN");
  const reviewed = "Invalid credentials.";
  assert.equal(english.text(reviewed), reviewed);
  assert.notEqual(chinese.text(reviewed), reviewed);
  const fixedBookingError = "Some selected seats are no longer available. Please choose available seats and try again.";
  assert.equal(english.text(fixedBookingError), fixedBookingError);
  assert.equal(chinese.text(fixedBookingError), "部分所选座位已不可用，请重新选择后重试。");
  const fixedAuthError = "The verification code is invalid.";
  assert.equal(chinese.text(fixedAuthError), "验证码无效。");
  const secretLikeBackendError = "pq: user_email=password=synthetic-secret";
  assert.notEqual(chinese.text(secretLikeBackendError), secretLikeBackendError);
  assert.equal(chinese.text(secretLikeBackendError), chinese.t("common.errors.unexpected"));
});

test("customer auth pages preserve baseline English label capitalization", () => {
  const registrationDriver = componentDriver("app/(customer)/register/page.tsx", {
    "next/navigation": { useRouter: () => ({ push() {} }), useSearchParams: () => new URLSearchParams() },
    "@/lib/auth-context": { useAuth: () => ({ register() {} }) },
    "@/lib/api/client": { apiClient: {} },
  });
  // The component driver resolves the route's Suspense child while executing
  // hooks so this inspects the actual registration form, not its fallback.
  const registration = registrationDriver.render();
  const label = (tree, id) => find(tree, node => node.type === "label" && node.props.htmlFor === id)?.props.children;
  assert.equal(find(registration, node => node.type === "h1")?.props.children, "Create Account");
  assert.equal(label(registration, "register-name"), "Full Name");
  assert.equal(label(registration, "register-confirm"), "Confirm Password");
  assert.equal(label(registration, "register-code"), "Verification Code");
  assert.equal(find(registration, node => node.type === "button" && node.props.type === "button")?.props.children, "Send Code");
  assert.equal(find(registration, node => node.props?.href === "/login")?.props.children, "Sign In");

  const forgot = componentDriver("app/(customer)/forgot-password/page.tsx", {
    "@/lib/api/client": { apiClient: {} },
  }).render();
  assert.equal(find(forgot, node => node.type === "h1")?.props.children, "Forgot Password");

  const reset = componentDriver("app/(customer)/reset-password/reset-form.tsx", {
    "next/navigation": { useRouter: () => ({ push() {} }) },
    "@/lib/auth-context": { useAuth: () => ({ resetPassword() {} }) },
  }).render();
  assert.equal(find(reset, node => node.type === "h1")?.props.children, "Reset Password");
  assert.equal(label(reset, "reset-password"), "New Password");
});

test("locale provider renders the English server snapshot with real React", () => {
  const { I18nProvider, useI18n } = loadOrEmpty("lib/i18n/index.ts", { react: React });
  assert.equal(typeof I18nProvider, "function", "the shared i18n module must provide a React context provider");
  assert.equal(typeof useI18n, "function");
  function Probe() {
    const { locale, t } = useI18n();
    return React.createElement("span", null, `${locale}:${t("common.language.label")}`);
  }
  const markup = renderToStaticMarkup(React.createElement(I18nProvider, null, React.createElement(Probe)));
  assert.match(markup, /en-US:Language/);
});

test("shared language switcher is labelled and exposes both requested choices", () => {
  const { LanguageSwitcher } = loadTypeScript("components/language-switcher.tsx", { react: React });
  const markup = renderToStaticMarkup(React.createElement(LanguageSwitcher));
  assert.match(markup, /aria-label="Language"/);
  assert.match(markup, /<option value="en-US" selected="">English<\/option>/);
  assert.match(markup, /<option value="zh-CN">简体中文<\/option>/);
});

test("global not-found route renders localized recovery copy and the shared language switcher", () => {
  const i18n = loadTypeScript("lib/i18n/index.ts", { react: React });
  const { default: NotFound } = loadTypeScript("app/not-found.tsx", {
    ...browserOverrides,
    "@/lib/i18n/index": { ...i18n, useI18n: () => i18n.getI18n("zh-CN") },
  });
  const markup = renderToStaticMarkup(React.createElement(NotFound));
  assert.match(markup, />404</);
  assert.match(markup, /页面不存在/);
  assert.match(markup, /返回首页/);
  assert.match(markup, /<a href="\/"/);
  assert.match(markup, /aria-label="语言"/);
  assert.match(markup, /<option value="en-US">English<\/option>/);
  assert.match(markup, /<option value="zh-CN" selected="">简体中文<\/option>/);
});
