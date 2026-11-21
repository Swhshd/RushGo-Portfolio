import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { browserOverrides, loadTypeScript } from "./render-typescript.mjs";

const discovery = () => loadTypeScript("lib/discovery.ts");

test("URL restores canonical category and trimmed search, including old mixed-case links", () => {
  const { readDiscovery } = discovery();
  assert.deepEqual(readDiscovery(new URLSearchParams("category=Concerts&search=++RushGo++")), { category: "concerts", search: "RushGo", dateFrom: "", dateTo: "", location: "", minPriceCents: undefined, maxPriceCents: undefined, sort: "date_asc", page: 1 });
  assert.deepEqual(readDiscovery(new URLSearchParams("category=unknown&search=+")), { category: "", search: "", dateFrom: "", dateTo: "", location: "", minPriceCents: undefined, maxPriceCents: undefined, sort: "date_asc", page: 1 });
});

test("Discover clears only category; each category and clear search preserve the other URL filter", () => {
  const { discoveryHref } = discovery();
  assert.equal(discoveryHref({ category: "sports", search: "RushGo" }, { category: "" }), "/?search=RushGo");
  for (const category of ["concerts", "sports", "films"]) {
    assert.equal(discoveryHref({ category: "", search: "" }, { category }), `/?category=${category}`);
  }
  assert.equal(discoveryHref({ category: "concerts", search: "RushGo" }, { search: "" }), "/?category=concerts");
  assert.equal(discoveryHref({ category: "films", search: "" }, { search: "  Hall  " }), "/?category=films&search=Hall");
  assert.equal(discoveryHref({ category: "films", search: "Films" }, { search: "" }), "/?category=films");
  assert.equal(discoveryHref({ category: "films", search: "" }, { category: "" }), "/");
});

test("Films search and category selection stay independently composable", () => {
  const { discoveryHref, readDiscovery } = discovery();
  for (const [state, update, expected] of [
    [{ category: "", search: "" }, { category: "films" }, "/?category=films"],
    [{ category: "", search: "" }, { search: "  Films  " }, "/?search=Films"],
    [{ category: "films", search: "" }, { search: "Films" }, "/?category=films&search=Films"],
    [{ category: "films", search: "Films" }, { search: "Premiere" }, "/?category=films&search=Premiere"],
    [{ category: "films", search: "Films" }, { category: "sports" }, "/?category=sports&search=Films"],
  ]) {
    assert.equal(discoveryHref(state, update), expected);
    const restored = readDiscovery(new URL(expected, "http://test.local").searchParams);
    assert.equal(discoveryHref(restored), expected);
  }
});

test("discovery restores and serializes server filters without changing them across category or page links", () => {
  const { discoveryHref, readDiscovery } = discovery();
  const restored = readDiscovery(new URLSearchParams("category=films&search=stage&date_from=2026-12-01&date_to=2026-12-31&location=shanghai&min_price_cents=5000&max_price_cents=25000&sort=price_asc&page=3"));
  assert.equal(discoveryHref(restored), "/?category=films&search=stage&date_from=2026-12-01&date_to=2026-12-31&location=shanghai&min_price_cents=5000&max_price_cents=25000&sort=price_asc&page=3");
  assert.equal(discoveryHref(restored, { category: "sports" }), "/?category=sports&search=stage&date_from=2026-12-01&date_to=2026-12-31&location=shanghai&min_price_cents=5000&max_price_cents=25000&sort=price_asc");
  assert.equal(discoveryHref(restored, { page: 4 }), "/?category=films&search=stage&date_from=2026-12-01&date_to=2026-12-31&location=shanghai&min_price_cents=5000&max_price_cents=25000&sort=price_asc&page=4");
  assert.equal(readDiscovery(new URLSearchParams("date_from=2026-02-30&min_price_cents=-1&sort=other&page=1001")).dateFrom, "");
  assert.equal(readDiscovery(new URLSearchParams("date_from=2026-02-30&min_price_cents=-1&sort=other&page=1001")).sort, "date_asc");
  assert.equal(readDiscovery(new URLSearchParams("date_from=2026-02-30&min_price_cents=-1&sort=other&page=1001")).page, 1);
});

test("unavailable presentation flags retain basic discovery and drop hidden advanced filters", () => {
  const { discoveryStateForFeature, readDiscovery } = discovery();
  const state = readDiscovery(new URLSearchParams("category=films&search=stage&date_from=2026-12-01&date_to=2026-12-31&location=shanghai&min_price_cents=5000&max_price_cents=25000&sort=price_asc&page=3"));
  assert.deepEqual(discoveryStateForFeature(state, false), {
    category: "films", search: "stage", dateFrom: "", dateTo: "", location: "",
    minPriceCents: undefined, maxPriceCents: undefined, sort: "date_asc", page: 1,
  });
  assert.equal(discoveryStateForFeature(state, true), state);
});

test("discovery requests one bounded server-filtered page", async () => {
  const original = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (url) => {
    requests.push(url);
    return { ok: true, status: 200, json: async () => ({ events: [{ id: "first" }], total: 41, page: 2, page_size: 20 }) };
  };
  try {
    const { loadDiscoveryPage } = loadTypeScript("lib/discovery.ts", { "./api/transformers": { transformEvent: (event) => event } });
    const result = await loadDiscoveryPage({ category: "concerts", search: "Demo Arena", dateFrom: "2026-12-01", dateTo: "2026-12-31", location: "Shanghai", minPriceCents: 5000, maxPriceCents: 25000, sort: "price_asc", page: 2 });
    assert.deepEqual(result.events.map(e => e.id), ["first"]);
    assert.equal(result.total, 41);
    assert.equal(result.page, 2);
    assert.equal(requests.length, 1);
    assert.deepEqual(requests, ["/api/events?category=concerts&search=Demo+Arena&date_from=2026-12-01&date_to=2026-12-31&location=Shanghai&min_price_cents=5000&max_price_cents=25000&sort=price_asc&page=2&page_size=20"]);
  } finally { globalThis.fetch = original; }
});

test("empty API results stay empty and backend failures are exposed rather than fake results", async () => {
  const original = globalThis.fetch;
  try {
    globalThis.fetch = async () => ({ ok: true, status: 200, json: async () => ({ events: [], total: 0, page: 1, page_size: 20 }) });
    assert.deepEqual(await discovery().loadDiscoveryPage({ category: "sports", search: "missing" }), { events: [], total: 0, page: 1, pageSize: 20 });
    globalThis.fetch = async () => ({ ok: false, status: 500, json: async () => ({ error: "offline" }) });
    await assert.rejects(discovery().loadDiscoveryPage({ category: "", search: "title" }), /offline/);
  } finally { globalThis.fetch = original; }
});

test("all event list reads stay bounded to one page", async () => {
  const original = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (url) => {
    requests.push(url);
    const page = new URL(url, "http://test.local").searchParams.get("page") || "1";
    return { ok: true, status: 200, json: async () => ({ events: [{ id: page === "1" ? "first" : "last" }], total: 2, page: Number(page), page_size: 50 }) };
  };
  try {
    const { apiClient } = loadTypeScript("lib/api/client.ts");
    const catalog = await apiClient.listEvents({ page_size: 50 });
    assert.deepEqual(catalog.events.map(e => e.id), ["first"]);
    assert.equal(catalog.page_size, 50);
    assert.equal(requests.length, 1);
    requests.length = 0;
    assert.deepEqual((await apiClient.listEvents({ page: 2, page_size: 50 })).events.map(e => e.id), ["last"]);
    assert.equal(requests.length, 1);
  } finally { globalThis.fetch = original; }
});

test("Navbar reflects URL active category, search value, real form and explicit clear command", () => {
  const { Navbar } = loadTypeScript("components/navbar.tsx", {
    ...browserOverrides,
    "next/navigation": { usePathname: () => "/", useRouter: () => ({ push() {} }), useSearchParams: () => new URLSearchParams("category=sports&search=Hall") },
    "@/lib/auth-context": { useAuth: () => ({ user: null, isLoading: false, logout: async () => {} }) },
  });
  const html = renderToStaticMarkup(React.createElement(Navbar));
  assert.match(html, /aria-current="page"[^>]*>Sports</);
  assert.match(html, /value="Hall"/);
  assert.match(html, /<form[^>]*role="search"/);
  assert.match(html, /aria-label="Clear search"/);
  assert.match(html, /<button[^>]*type="button"[^>]*aria-label="Clear search"/);
});

test("Navbar category links do not create implicit search text", () => {
  const { Navbar } = loadTypeScript("components/navbar.tsx", {
    ...browserOverrides,
    "next/navigation": { usePathname: () => "/", useRouter: () => ({ push() {} }), useSearchParams: () => new URLSearchParams() },
    "@/lib/auth-context": { useAuth: () => ({ user: null, isLoading: false, logout: async () => {} }) },
  });
  const html = renderToStaticMarkup(React.createElement(Navbar));
  assert.match(html, /href="\/\?category=films"[^>]*>Films</);
  assert.match(html, /name="search"[^>]*value=""/);
  assert.doesNotMatch(html, /search=Films/);
});

test("event breadcrumbs use canonical category URLs without implicit search", () => {
  for (const category of ["Concerts", "Sports", "Films"]) {
    const event = { id: "demo", title: "Demo Event", category, tiers: [] };
    const { default: EventDetailPage } = loadTypeScript("app/(customer)/events/[id]/page.tsx", {
      ...browserOverrides,
      "next/navigation": { useParams: () => ({ id: "demo" }), useRouter: () => ({ push() {} }) },
      "@/lib/auth-context": { useAuth: () => ({ user: null, isLoading: false }) },
      "@/lib/booking-context": { useBooking: () => ({ getEvent: () => event, isLoading: false }) },
    });
    const html = renderToStaticMarkup(React.createElement(EventDetailPage));
    assert.match(html, new RegExp(`href="/\\?category=${category.toLowerCase()}"`));
    assert.doesNotMatch(html, /search=/);
  }
});
