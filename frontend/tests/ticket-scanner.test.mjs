import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { readFileSync } from "node:fs";
import { renderToStaticMarkup } from "react-dom/server";
import { browserOverrides, loadTypeScript } from "./render-typescript.mjs";
const { getI18n } = loadTypeScript("lib/i18n/core.ts");

test("scanner offers an online manual fallback and never treats the client as authority", () => {
  const clientReact = {
    ...React,
    useSyncExternalStore: (_subscribe, getSnapshot) => getSnapshot(),
  };
  const { default: Scanner } = loadTypeScript("app/(customer)/events/[id]/check-in/page.tsx", {
    ...browserOverrides,
    react: clientReact,
    "next/navigation": { useParams: () => ({ id: "event-1" }) },
    "@/lib/auth-context": { useAuth: () => ({ user: { id: "staff-1" }, isLoading: false }) },
    "@/lib/api/client": { apiClient: { checkInTicket: async () => ({ result: "accepted" }) } },
    "@/lib/i18n": { useI18n: () => getI18n("en-US") },
  });
  const html = renderToStaticMarkup(React.createElement(Scanner));
  assert.match(html, /Ticket check-in/);
  assert.match(html, /Manual ticket code/);
  assert.match(html, /Check ticket online/);
  assert.match(html, /Server result/);
  assert.doesNotMatch(html, /Admission accepted/);
  assert.doesNotMatch(html, /localStorage|sessionStorage/);
});

test("scanner check-in copy switches between English and Chinese", () => {
  const clientReact = {
    ...React,
    useSyncExternalStore: (_subscribe, getSnapshot) => getSnapshot(),
  };
  function render(locale) {
    const { default: Scanner } = loadTypeScript("app/(customer)/events/[id]/check-in/page.tsx", {
      ...browserOverrides,
      react: clientReact,
      "next/navigation": { useParams: () => ({ id: "event-1" }) },
      "@/lib/auth-context": { useAuth: () => ({ user: { id: "staff-1" }, isLoading: false }) },
      "@/lib/api/client": { apiClient: { checkInTicket: async () => ({ result: "accepted" }) } },
      "@/lib/i18n": { useI18n: () => getI18n(locale) },
    });
    return renderToStaticMarkup(React.createElement(Scanner));
  }

  const english = render("en-US");
  const chinese = render("zh-CN");
  assert.match(english, /Ready for an online ticket check\./);
  assert.match(english, /Manual ticket code/);
  assert.match(chinese, /可以开始在线核验票券。/);
  assert.match(chinese, /手动输入票券码/);
});

test("scanner checks optional camera APIs at runtime before enabling native scanning", () => {
  const page = readFileSync(new URL("../app/(customer)/events/[id]/check-in/page.tsx", import.meta.url), "utf8");
  assert.match(page, /typeof getUserMedia === "function"/);
  assert.match(page, /typeof Detector === "function"/);
  assert.match(page, /const cameraSupported = hydrated &&/);
  assert.doesNotMatch(page, /setCameraSupported/);
});

test("scanner exposes an explicit checking state and does not read a ref during render", () => {
  const page = readFileSync(new URL("../app/(customer)/events/[id]/check-in/page.tsx", import.meta.url), "utf8");
  assert.match(page, /kind: "checking"/);
  assert.match(page, /disabled=\{!credential\.trim\(\) \|\| scanState\.kind === "checking"\}/);
  assert.doesNotMatch(page, /disabled=\{[^}]*scanBusyRef\.current/);
});

test("scanner keeps its initial server and browser render stable while auth hydrates", () => {
  const page = readFileSync(new URL("../app/(customer)/events/[id]/check-in/page.tsx", import.meta.url), "utf8");
  assert.match(page, /useSyncExternalStore/);
  assert.match(page, /const hydrated = useSyncExternalStore\(/);
  assert.match(page, /if \(!hydrated \|\| authLoading\)/);
  assert.doesNotMatch(page, /<main\b|<\/main>/);
});

test("admin scanner assignments never display a prior event while the next event loads", () => {
  const source = readFileSync(new URL("../components/admin-event-detail-drawer.tsx", import.meta.url), "utf8");
  const panel = source.slice(source.indexOf("function ScannerAssignmentsPanel"));
  assert.match(panel, /state\.eventId === eventId && state\.reloadKey === reloadKey/);
  assert.match(panel, /setState\(\{ status: "ready", eventId, reloadKey, assignments \}\)/);
  assert.doesNotMatch(panel, /setState\(\{ status: "loading"/);
  assert.ok(
    source.includes("<ScannerAssignmentsPanel key={event.id} eventId={event.id} />"),
    "scanner assignment feedback must be scoped to the event component instance"
  );
});

test("wallet page distinguishes issued Tickets from confirmed booking history", () => {
  const page = readFileSync(new URL("../app/(customer)/my-tickets/page.tsx", import.meta.url), "utf8");
  const copy = readFileSync(new URL("../lib/i18n/customer.ts", import.meta.url), "utf8");
  const aftersales = readFileSync(new URL("../lib/i18n/aftersales.ts", import.meta.url), "utf8");
  assert.match(page, /listOwnedTickets/);
  assert.match(page, /customer\.wallet\.issuedTickets/);
  assert.match(page, /aftersales\.orders/);
  assert.match(aftersales, /"aftersales\.orders":/);
  assert.match(page, /customer\.wallet\.authorityNote/);
  assert.match(copy, /Orders keep their authoritative status\. Entry tickets appear above only after separate server-side issuance\./);
  assert.doesNotMatch(page, /Confirmed bookings are shown as tickets/);
});


test("scanner distinguishes permission, missing device and busy camera failures", () => {
  const { cameraFailure } = loadTypeScript("lib/scanner-camera.ts");
  for (const [name, expected] of [["NotAllowedError","permission"],["NotFoundError","device"],["NotReadableError","busy"],["Error","unsupported"]]) {
    const error = new Error("redacted"); error.name = name;
    assert.equal(cameraFailure(error), expected);
  }
});
