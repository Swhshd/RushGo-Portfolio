import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { loadTypeScript } from "./render-typescript.mjs";

const Icon = () => null;
const iconStubs = {
  BarChart3: Icon,
  CalendarDays: Icon,
  CircleAlert: Icon,
  Flag: Icon,
  LayoutGrid: Icon,
  Menu: Icon,
  Search: Icon,
  Send: Icon,
  Timer: Icon,
  X: Icon,
};

function textContent(node) {
  if (node === null || node === undefined || typeof node === "boolean") return "";
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(textContent).join("");
  return node.props ? textContent(node.props.children) : "";
}

function all(tree, predicate, result = []) {
  if (!tree || typeof tree !== "object") return result;
  if (predicate(tree)) result.push(tree);
  for (const child of React.Children.toArray(tree.props?.children)) {
    all(child, predicate, result);
  }
  return result;
}

function renderNavigation(locale = "en-US") {
  const { getI18n } = loadTypeScript("lib/i18n/core.ts");
  const { AdminNavigationLinks } = loadTypeScript("components/admin-navigation.tsx", {
    "next/link": { __esModule: true, default: "a" },
    "lucide-react": iconStubs,
    "@/lib/dialog-focus": { focusableElements: () => [], nextFocusIndex: () => 0 },
    "@/components/language-switcher": { LanguageSwitcher: () => null },
    "@/lib/i18n": { useI18n: () => getI18n(locale) },
  });
  return AdminNavigationLinks({ pathname: "/admin", variant: "desktop" });
}

function linkWithLabel(tree, label) {
  return all(tree, (node) => node.type === "a" && textContent(node).includes(label))[0];
}

test("Outbox Events, Metrics, and Feature Flags open their active admin pages", () => {
  const tree = renderNavigation();
  const events = linkWithLabel(tree, "Events");
  const outbox = linkWithLabel(tree, "Outbox Events");
  const flags = linkWithLabel(tree, "Feature Flags");
  const metrics = linkWithLabel(tree, "Metrics");

  assert.ok(events, "the existing Events destination should remain discoverable");
  assert.equal(events.props.href, "/admin/events");
  assert.ok(outbox, "Outbox Events should appear in the admin navigation");
  assert.equal(outbox.props.href, "/admin/outbox");
  assert.equal(outbox.props["aria-disabled"], undefined);
  assert.equal(outbox.props.tabIndex, undefined);

  assert.ok(flags, "Feature Flags should open its usable page");
  assert.equal(flags.props.href, "/admin/flags");
  assert.equal(flags.props["aria-disabled"], undefined);
  assert.equal(flags.props.tabIndex, undefined);
  assert.ok(metrics, "Metrics should be discoverable in admin navigation");
  assert.equal(metrics.props.href, "/admin/metrics");
});

test("admin navigation labels follow the selected locale without changing destinations", () => {
  const tree = renderNavigation("zh-CN");
  const outbox = linkWithLabel(tree, "发件箱事件");
  const flags = linkWithLabel(tree, "功能开关");
  const metrics = linkWithLabel(tree, "指标");
  assert.equal(outbox.props.href, "/admin/outbox");
  assert.equal(flags.props.href, "/admin/flags");
  assert.equal(flags.props["aria-disabled"], undefined);
  assert.equal(metrics.props.href, "/admin/metrics");
});

test("mobile admin keeps the shared language control reachable inside the navigation drawer", () => {
  const { getI18n } = loadTypeScript("lib/i18n/core.ts");
  const { AdminMobileNavigation } = loadTypeScript("components/admin-navigation.tsx", {
    "next/link": { __esModule: true, default: "a" },
    "lucide-react": iconStubs,
    "@/lib/dialog-focus": { focusableElements: () => [], nextFocusIndex: () => 0 },
    "@/lib/i18n": { useI18n: () => getI18n("en-US") },
    "@/lib/i18n/index": { useI18n: () => getI18n("en-US") },
  });
  const markup = renderToStaticMarkup(
    React.createElement(AdminMobileNavigation, {
      pathname: "/admin",
      contentRef: React.createRef(),
    }),
  );
  assert.equal((markup.match(/aria-label="Language"/g) || []).length, 2);
  assert.match(markup, /data-admin-mobile-language/);
  assert.match(markup, /id="admin-mobile-navigation"/);
});
