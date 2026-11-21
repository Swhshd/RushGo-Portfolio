import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { loadTypeScript } from "./render-typescript.mjs";

const { getI18n } = loadTypeScript("lib/i18n/core.ts");

test("/admin renders its own page instead of redirecting to another section", () => {
  let redirectTarget;
  const Icon = () => null;
  const hooks = {
    ...React,
    useState(initial) {
      return [typeof initial === "function" ? initial() : initial, () => {}];
    },
    useRef(initial) {
      return { current: initial };
    },
    useEffect() {},
    useLayoutEffect() {},
  };
  const { default: AdminHomePage } = loadTypeScript("app/admin/page.tsx", {
    react: hooks,
    "next/navigation": {
      redirect(target) {
        redirectTarget = target;
      },
    },
    "next/link": { __esModule: true, default: "a" },
    "lucide-react": {
      AlertCircle: Icon,
      ArrowUpRight: Icon,
      CalendarDays: Icon,
      Loader2: Icon,
      RefreshCw: Icon,
      Send: Icon,
      Timer: Icon,
    },
    "@/lib/api/client": { apiClient: { getSessionVersion: () => 0 } },
    "@/lib/auth-context": { useAuth: () => ({ user: { id: "admin-fixture", role: "admin" } }) },
    "@/lib/i18n": { useI18n: () => getI18n("en-US") },
  });

  const page = AdminHomePage();

  assert.equal(
    redirectTarget,
    undefined,
    `/admin redirected to ${redirectTarget ?? "an unknown destination"}`,
  );
  assert.ok(page && React.isValidElement(page), "/admin should render an overview page");
});

test("admin forbidden shell copy follows locale and interpolates the account details", () => {
  const { default: AdminLayout } = loadTypeScript("app/admin/layout.tsx", {
    "next/navigation": { usePathname: () => "/admin", useRouter: () => ({ replace() {} }) },
    "next/link": { __esModule: true, default: "a" },
    "@/lib/auth-context": {
      useAuth: () => ({ user: { id: "staff-user", email: "staff@example.test", role: "staff" }, isLoading: false }),
    },
    "@/lib/i18n": { useI18n: () => getI18n("zh-CN") },
    "@/components/language-switcher": { LanguageSwitcher: () => null },
    "@/components/admin-navigation": { AdminMobileNavigation: () => null, AdminNavigationLinks: () => null },
  });
  const html = renderToStaticMarkup(React.createElement(AdminLayout, null, React.createElement("div", null, "Admin content")));
  assert.match(html, /403 — 仅限管理员/);
  assert.match(html, /staff@example\.test（staff）/);
  assert.match(html, /返回 RUSHGO/);
});
