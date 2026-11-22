import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { adminRoutes, adminResponse } from "./gallery/admin.mjs";
import { customerRoutes, customerResponse } from "./gallery/customer.mjs";
import { commonResponse, ids, sampleTime } from "./gallery/sample.mjs";
import { desktop, findPages, makeGallery, makeIndex } from "./gallery/catalog.mjs";
import { frontendRequire, root, startGalleryApp, streamPath } from "./gallery/local-app.mjs";

const specs = [...customerRoutes, ...adminRoutes];
const paths = new Set(specs.map(s => new URL(s.url, "http://localhost").pathname));
const { chromium } = frontendRequire("@playwright/test");

async function openPage(browser, origin, spec, viewport) {
  const context = await browser.newContext({
    viewport, deviceScaleFactor: 1, locale: "zh-CN", timezoneId: "Asia/Shanghai",
    colorScheme: "dark", reducedMotion: "reduce", serviceWorkers: "block",
    isMobile: viewport.width < 600, hasTouch: viewport.width < 600,
  });
  try {
    const issues = new Set();
    await context.routeWebSocket("**/*", ws => ws.close());
    await context.route("**/*", async route => {
      const request = route.request();
      const url = new URL(request.url());
      const method = request.method();
      const path = url.pathname;
      if (url.origin !== origin) return route.abort("blockedbyclient");
      // Resume is POST even when displaying existing state. Supply one fixed
      // snapshot in the browser. This request never goes to the local server.
      const resumeSnapshot = method === "POST" && spec.route === "/checkout" &&
        path === "/api/bookings/" + ids.pendingBooking + "/resume";
      if (method !== "GET" && method !== "HEAD" && !resumeSnapshot) {
        issues.add("blocked " + method + " " + path);
        return route.fulfill({ status: 405, json: { error: "read-only gallery" } });
      }
      if (path === streamPath) return route.continue();
      if (path.startsWith("/api/")) {
        const input = { path, query: url.searchParams, role: spec.role, method };
        const data = commonResponse(input) ?? (spec.role === "admin" ? adminResponse(input) : customerResponse(input));
        if (data === undefined) issues.add("missing fixture " + path);
        return route.fulfill({ status: data === undefined ? 404 : 200, json: data ?? { error: "no such fixture" } });
      }
      if (paths.has(path) || path.startsWith("/_next/static/")) return route.continue();
      return route.abort("blockedbyclient");
    });
    await context.addInitScript(({ origin, signedIn }) => {
      if (location.origin !== origin) return;
      localStorage.setItem("rushgo:locale", "zh-CN");
      if (signedIn) localStorage.setItem("access_token", "offline-gallery-unusable");
    }, { origin, signedIn: spec.role !== "guest" });
    if (spec.setup) await spec.setup(context);
    const page = await context.newPage();
    page.setDefaultTimeout(8000);
    page.on("pageerror", error => issues.add("page error: " + error.message.slice(0, 200)));
    await page.clock.setFixedTime(new Date(sampleTime));
    await page.goto(origin + (spec.entryUrl ?? spec.url), { waitUntil: "domcontentloaded" });
    await page.waitForFunction(() => document.documentElement.lang === "zh-CN");
    if (spec.navigateFromEntry) await spec.navigateFromEntry(page);
    if (spec.interact) await spec.interact(page);
    await spec.ready(page);
    const expected = new URL(spec.url, origin);
    if (page.url() !== expected.href) throw Error("Unexpected navigation for " + spec.route);
    await page.evaluate(async () => {
      await document.fonts.ready;
      await Promise.all([...document.images].filter(img => {
        const rect = img.getBoundingClientRect();
        return rect.top < innerHeight && rect.bottom > 0 && rect.width > 0;
      }).map(img => img.decode()));
    });
    const visible = await page.locator("body").innerText();
    if (/Application error:|This page could not be found|offline-gallery-unusable|postgres(?:ql)?:\/\/|(?:sk|rk)_(?:live|test)_[a-z0-9]{12,}|pi_[a-z0-9_]+_secret_|Bearer [a-z0-9._-]+/i.test(visible)) {
      throw Error("Unexpected error page or credential-like text in the viewport");
    }
    if (issues.size) throw Error([...issues].join(", "));
    return { context, page };
  } catch (error) {
    await context.close();
    throw error;
  }
}

async function main() {
  const args = process.argv.slice(2);
  const preview = args[0] === "--preview" && args.length === 2 ? specs.find(s => s.route === args[1]) : undefined;
  if (args.length && !preview) throw Error("Usage: node docs/demo/capture-gallery.mjs [--preview /route]");
  const pages = await findPages(root);
  if (new Set(specs.map(s => s.route)).size !== pages.size || specs.length !== pages.size ||
      specs.some(s => !pages.has(s.route)) || new Set(specs.map(s => s.slug)).size !== specs.length) {
    throw Error("Gallery specifications must cover every page.tsx exactly once with unique filenames");
  }
  const git = (...args) => execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
  const revision = git("log", "-1", "--format=%H", "--", "frontend");
  const index = makeIndex(specs, pages, revision);
  const channel = process.env.GALLERY_BROWSER_CHANNEL ||
    (!existsSync(chromium.executablePath()) && process.platform === "win32" ? "msedge" : undefined);
  const browser = await chromium.launch({ headless: !preview, channel });
  let app;
  try {
    app = await startGalleryApp(paths);
    if (preview) {
      const target = preview.aliasOf ? specs.find(s => s.route === preview.aliasOf) : preview;
      const { page } = await openPage(browser, app.origin, target, desktop);
      console.log("Local read-only preview. Close the page to finish.");
      await page.waitForEvent("close", { timeout: 0 });
      return;
    }
    if (git("status", "--porcelain", "--", "frontend")) throw Error("Capture from a clean, freshly built frontend checkout");
    const outputs = new Map();
    const hashes = new Set();
    const failures = [];
    for (const [i, spec] of specs.entries()) {
      if (spec.aliasOf) continue;
      for (const shot of index[i].screenshots) {
        let context;
        try {
          const opened = await openPage(browser, app.origin, spec, shot.viewport);
          context = opened.context;
          const buffer = await opened.page.screenshot({ animations: "disabled" });
          const hash = createHash("sha256").update(buffer).digest("hex");
          if (hashes.has(hash)) throw Error("Duplicate image from a different page or viewport");
          hashes.add(hash);
          outputs.set(shot.file, buffer);
          console.log("Captured", shot.file);
        } catch (error) {
          failures.push(shot.file);
          console.error(spec.route, shot.viewport.width, error.message);
        } finally {
          await context?.close();
        }
      }
    }
    if (failures.length) throw Error("Gallery incomplete: " + failures.join(", "));
    outputs.set("docs/demo/screenshot-index.json", JSON.stringify(index, null, 2) + "\n");
    outputs.set("docs/SCREENSHOTS.md", makeGallery(specs, index));
    for (const [file, content] of outputs) {
      const path = join(root, file);
      await mkdir(dirname(path), { recursive: true });
      await writeFile(path, content);
    }
    console.log("Saved", hashes.size, "captures for", specs.length, "page routes");
  } finally {
    await browser.close();
    await app?.close();
  }
}
main().catch(error => { console.error(error.message); process.exitCode = 1; });
