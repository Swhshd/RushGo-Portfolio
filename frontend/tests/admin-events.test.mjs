import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

const admin = () => loadTypeScript("lib/admin-events.ts");

const validForm = () => ({
  title: "  Midnight Arena  ",
  description: " Live show ",
  category: "concerts",
  venue: "RushGo Arena",
  location: "Ho Chi Minh City",
  date: "2026-11-01T19:30:00",
  imageUrl: "/images/arena.jpg",
  publicationStatus: "draft",
  salesStatus: "scheduled",
  salesStartAt: "",
  salesEndAt: "",
  tiers: [
    { name: " Floor ", price: "49.99", quantity: "120" },
    { name: "Balcony", price: "25", quantity: "300" },
  ],
});

test("dollar strings convert to integer cents without float drift", () => {
  const { priceToCents } = admin();
  assert.equal(priceToCents("49.99"), 4999);
  assert.equal(priceToCents("49.9"), 4990);
  assert.equal(priceToCents("50"), 5000);
  assert.equal(priceToCents("0.01"), 1);
  assert.equal(priceToCents(" 12.50 "), 1250);
  for (const invalid of ["", "0", "0.00", "-1", "1.234", "abc", "1,000", "1e3"]) {
    assert.equal(priceToCents(invalid), null, `expected null for ${invalid}`);
  }
});

test("tier quantities accept only positive whole int32 values", () => {
  const { parseQuantity } = admin();
  assert.equal(parseQuantity("1"), 1);
  assert.equal(parseQuantity("0002"), 2);
  assert.equal(parseQuantity("2147483647"), 2147483647);
  for (const invalid of ["", "0", "-1", "1.5", "abc", "2147483648"]) {
    assert.equal(parseQuantity(invalid), null, `expected null for ${invalid}`);
  }
});

test("RFC3339 and datetime-local conversion round-trips seconds and milliseconds", () => {
  const { toDateTimeLocal, toRfc3339 } = admin();
  const iso = "2026-11-01T11:30:45.123Z";
  const local = toDateTimeLocal(iso);
  assert.match(local, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$/);
  assert.equal(toRfc3339(local), iso);

  const whole = "2026-11-01T11:30:45Z";
  const wholeLocal = toDateTimeLocal(whole);
  assert.match(wholeLocal, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/);
  assert.equal(toRfc3339(wholeLocal), "2026-11-01T11:30:45.000Z");

  assert.equal(toRfc3339("not-a-date"), null);
  assert.equal(toRfc3339(""), null);
  assert.equal(toDateTimeLocal("not-a-date"), "");
});

test("create validation reports every missing field and a valid form passes", () => {
  const { emptyEventForm, hasErrors, validateEventForm } = admin();
  const emptyErrors = validateEventForm(emptyEventForm(), { requireTiers: true });
  assert.ok(emptyErrors.title);
  assert.ok(emptyErrors.category);
  assert.ok(emptyErrors.venue);
  assert.ok(emptyErrors.location);
  assert.ok(emptyErrors.date);
  assert.ok(emptyErrors.tierRows);
  assert.equal(hasErrors(emptyErrors), true);

  const validErrors = validateEventForm(validForm(), { requireTiers: true });
  assert.deepEqual(validErrors, {});
  assert.equal(hasErrors(validErrors), false);
});

test("tier rows surface field-level errors without rejecting the event fields", () => {
  const { validateEventForm } = admin();
  const form = validForm();
  form.tiers = [
    { name: "", price: "0", quantity: "1.5" },
    { name: "Floor", price: "10.00", quantity: "10" },
  ];
  const errors = validateEventForm(form, { requireTiers: true });
  assert.equal(errors.title, undefined);
  assert.equal(errors.tierRows[0].name, "admin.events.validation.tierNameRequired");
  assert.ok(errors.tierRows[0].price);
  assert.ok(errors.tierRows[0].quantity);
  assert.deepEqual(errors.tierRows[1], {});
});

test("edit mode ignores tier drafts but still validates the event fields", () => {
  const { validateEventForm } = admin();
  const form = validForm();
  form.tiers = [{ name: "", price: "", quantity: "" }];
  assert.deepEqual(validateEventForm(form, { requireTiers: false }), {});
  form.date = "";
  assert.ok(validateEventForm(form, { requireTiers: false }).date);
});

test("create payload sends trimmed fields, integer cents and RFC3339", () => {
  const { buildCreatePayload } = admin();
  const payload = buildCreatePayload(validForm());
  assert.deepEqual(payload, {
    title: "Midnight Arena",
    description: "Live show",
    category: "concerts",
    venue: "RushGo Arena",
    location: "Ho Chi Minh City",
    date: new Date("2026-11-01T19:30:00").toISOString(),
    image_url: "/images/arena.jpg",
    publication_status: "draft",
    sales_status: "scheduled",
    sales_start_at: "",
    sales_end_at: "",
    tiers: [
      { name: "Floor", price_cents: 4999, total_quantity: 120 },
      { name: "Balcony", price_cents: 2500, total_quantity: 300 },
    ],
  });
  const invalid = validForm();
  invalid.date = "";
  assert.equal(buildCreatePayload(invalid), null);
  const invalidTier = validForm();
  invalidTier.tiers[0].price = "0";
  assert.equal(buildCreatePayload(invalidTier), null);
});

test("sales lifecycle validates paired windows and builds explicit clear-window updates", () => {
  const { buildSalesUpdatePayload, hasErrors, validateEventForm } = admin();
  const form = validForm();
  form.publicationStatus = "published";
  form.salesStatus = "paused";
  form.salesStartAt = "2026-11-01T10:00";
  const incomplete = validateEventForm(form, { requireTiers: true });
  assert.equal(incomplete.salesWindow, "admin.events.validation.salesWindowPair");
  assert.equal(hasErrors(incomplete), true);

  form.salesEndAt = "2026-11-01T12:00";
  assert.deepEqual(validateEventForm(form, { requireTiers: true }), {});
  assert.deepEqual(buildSalesUpdatePayload(form), {
    publication_status: "published",
    sales_status: "paused",
    sales_start_at: new Date(form.salesStartAt).toISOString(),
    sales_end_at: new Date(form.salesEndAt).toISOString(),
    clear_sales_window: false,
  });

  form.salesStartAt = "";
  form.salesEndAt = "";
  assert.equal(buildSalesUpdatePayload(form).clear_sales_window, true);
});

test("update payload always carries every editable field and never tiers", () => {
  const { buildUpdatePayload } = admin();
  const payload = buildUpdatePayload(validForm());
  assert.deepEqual(Object.keys(payload).sort(), [
    "category",
    "date",
    "description",
    "image_url",
    "location",
    "title",
    "venue",
  ]);
  assert.equal(payload.title, "Midnight Arena");
  assert.equal(payload.image_url, "/images/arena.jpg");
});

test("eventToForm preserves stored fields so an edit cannot blank them", () => {
  const { eventToForm, buildUpdatePayload } = admin();
  const event = {
    id: "e1",
    title: "Stored title",
    description: "Stored description",
    category: "sports",
    venue: "Stored venue",
    location: "Stored city",
    date: "2026-12-05T10:15:30Z",
    image_url: "https://example.test/poster.png",
    status: "active",
    tiers: [],
    created_at: "2026-01-01T00:00:00Z",
  };
  const form = eventToForm(event);
  const payload = buildUpdatePayload(form);
  assert.equal(payload.title, event.title);
  assert.equal(payload.description, event.description);
  assert.equal(payload.category, event.category);
  assert.equal(payload.venue, event.venue);
  assert.equal(payload.location, event.location);
  assert.equal(payload.image_url, event.image_url);
  assert.equal(payload.date, new Date(event.date).toISOString());
  assert.deepEqual(form.tiers, []);

  // Millisecond precision survives an unrelated edit.
  const precise = { ...event, date: "2026-12-05T10:15:30.123Z" };
  const preciseForm = eventToForm(precise);
  assert.equal(buildUpdatePayload(preciseForm).date, precise.date);
});

test("editing cannot silently clear fields the gateway ignores when empty", () => {
  const { validateEventForm } = admin();
  const original = { description: "Stored", imageUrl: "/stored.png" };
  const form = validForm();
  form.description = "";
  form.imageUrl = "   ";
  const errors = validateEventForm(form, { requireTiers: false, original });
  assert.ok(errors.description);
  assert.ok(errors.imageUrl);

  const unchanged = validForm();
  assert.deepEqual(
    validateEventForm(unchanged, { requireTiers: false, original }),
    {},
  );

  const emptyOriginal = { description: "", imageUrl: "" };
  const cleared = validForm();
  cleared.description = "";
  cleared.imageUrl = "";
  assert.deepEqual(
    validateEventForm(cleared, { requireTiers: false, original: emptyOriginal }),
    {},
  );
});

test("page window keeps server totals and reports zero pages for empty results", () => {
  const { pageWindow } = admin();
  assert.deepEqual(pageWindow(1, 20, 0), {
    page: 1,
    pageSize: 20,
    total: 0,
    totalPages: 0,
    start: 0,
    end: 0,
    hasPrev: false,
    hasNext: false,
  });

  assert.deepEqual(pageWindow(2, 20, 45), {
    page: 2,
    pageSize: 20,
    total: 45,
    totalPages: 3,
    start: 21,
    end: 40,
    hasPrev: true,
    hasNext: true,
  });

  const last = pageWindow(3, 20, 45);
  assert.equal(last.start, 41);
  assert.equal(last.end, 45);
  assert.equal(last.hasNext, false);

  // Out-of-range requests clamp for display; the caller still owns fetching.
  const clamped = pageWindow(99, 20, 45);
  assert.equal(clamped.page, 3);
  assert.equal(clamped.start, 41);
  assert.equal(clamped.end, 45);

  // An empty result never claims a page exists.
  assert.equal(pageWindow(7, 20, 0).totalPages, 0);
  assert.equal(pageWindow(7, 20, 0).hasNext, false);
});

test("non-finite prices render as unavailable rather than a fabricated zero", () => {
  const { formatCents } = admin();
  assert.equal(formatCents(Number.NaN), "—");
  assert.equal(formatCents(Number.POSITIVE_INFINITY), "—");
  assert.equal(formatCents(2500), "$25.00");
});

test("admin event display helpers honor the selected locale", () => {
  const { formatCents, formatEventDate, tierSummary } = admin();
  assert.notEqual(formatCents(2500, "zh-CN"), formatCents(2500, "en-US"));
  assert.notEqual(
    formatEventDate("2026-11-01T11:30:00Z", "zh-CN"),
    formatEventDate("2026-11-01T11:30:00Z", "en-US"),
  );
  const event = { tiers: [{ price_cents: 2500 }], id: "e1" };
  assert.equal(tierSummary(event, "en-US"), "1 tier · from $25.00");
  assert.equal(tierSummary(event, "zh-CN"), `1 个票档 · 最低 ${formatCents(2500, "zh-CN")}`);
  assert.equal(tierSummary({ tiers: [] }, "zh-CN"), "无票档");
});

test("page inventory sums only the events on the supplied page", () => {
  const { pageInventory } = admin();
  const stats = pageInventory([
    {
      tiers: [
        { available_quantity: 10, total_quantity: 12 },
        { available_quantity: 5, total_quantity: 5 },
      ],
    },
    { tiers: [{ available_quantity: 0, total_quantity: 20 }] },
    { tiers: [] },
  ]);
  assert.deepEqual(stats, { tiers: 3, availableSeats: 15 });
  assert.deepEqual(pageInventory([]), { tiers: 0, availableSeats: 0 });
});

test("update payload keeps the stored RFC3339 instant when the date is untouched", () => {
  const { buildUpdatePayload, toDateTimeLocal } = admin();
  const originalDate = "2026-12-05T10:15:30.123Z";
  const form = validForm();
  form.date = toDateTimeLocal(originalDate);

  assert.equal(buildUpdatePayload(form, originalDate).date, originalDate);

  form.date = toDateTimeLocal("2026-12-06T00:00:00Z");
  assert.equal(
    buildUpdatePayload(form, originalDate).date,
    new Date(form.date).toISOString(),
  );
});

test("admin list forwards search, category and page to the server", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  const calls = [];
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = async (url) => {
      calls.push(url);
      return {
        ok: true,
        status: 200,
        json: async () => ({ events: [], total: 0, page: 2, page_size: 20 }),
      };
    };
    await apiClient.listEventsPage({
      search: "rush go",
      category: "concerts",
      page: 2,
      page_size: 20,
    });
    assert.equal(
      calls[0],
      "/api/events?category=concerts&search=rush+go&page=2&page_size=20",
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("admin list reads one bounded page instead of catalog auto-pagination", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  const calls = [];
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = async (url) => {
      calls.push(url);
      return {
        ok: true,
        status: 200,
        json: async () => ({
          events: [{ id: "a" }],
          total: 250,
          page: 1,
          page_size: 100,
        }),
      };
    };
    const page = await apiClient.listEventsPage({ page: 1, page_size: 100 });
    assert.equal(page.total, 250);
    assert.equal(calls.length, 1);
    assert.match(calls[0], /\/api\/events\?page=1&page_size=100$/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("admin client writes use the gateway paths and surface 403 as an error", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  const calls = [];
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = async (url, options = {}) => {
      calls.push({ url, options });
      return {
        ok: true,
        status: 201,
        json: async () => ({ id: "created" }),
      };
    };
    const created = await apiClient.createEvent({
      title: "T",
      description: "",
      category: "concerts",
      venue: "V",
      location: "L",
      date: "2026-11-01T11:30:00Z",
      image_url: "",
      tiers: [{ name: "GA", price_cents: 1000, total_quantity: 10 }],
    });
    assert.equal(created.id, "created");
    assert.equal(calls[0].url, "/api/events");
    assert.equal(calls[0].options.method, "POST");
    assert.equal(
      JSON.parse(calls[0].options.body).tiers[0].price_cents,
      1000,
    );

    await apiClient.updateEvent("e1", {
      title: "T2",
      description: "",
      category: "films",
      venue: "V",
      location: "L",
      date: "2026-11-02T11:30:00Z",
      image_url: "",
    });
    assert.equal(calls[1].url, "/api/events/e1");
    assert.equal(calls[1].options.method, "PUT");

    globalThis.fetch = async () => ({
      ok: false,
      status: 403,
      json: async () => ({ error: "admin access required" }),
    });
    await assert.rejects(
      () =>
        apiClient.updateEvent("e1", {
          title: "T2",
          description: "",
          category: "films",
          venue: "V",
          location: "L",
          date: "2026-11-02T11:30:00Z",
          image_url: "",
        }),
      (err) => err.status === 403 && /admin access required/.test(err.message),
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("admin event lifecycle uses the admin read path, stable create key, and sales patch contract", async () => {
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  const calls = [];
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = async (url, options = {}) => {
      calls.push({ url, options });
      return { ok: true, status: 200, json: async () => ({ events: [], total: 0, id: "event-id" }) };
    };
    await apiClient.listAdminEventsPage({ search: "draft", page: 2, page_size: 20 });
    await apiClient.createEvent({
      title: "T", description: "", category: "concerts", venue: "V", location: "L",
      date: "2026-11-01T11:30:00Z", image_url: "", publication_status: "draft",
      sales_status: "scheduled", sales_start_at: "", sales_end_at: "",
      tiers: [{ name: "GA", price_cents: 1000, total_quantity: 10 }],
    }, "123e4567-e89b-12d3-a456-426614174000");
    await apiClient.updateEventSales("event-id", {
      publication_status: "published", sales_status: "paused", clear_sales_window: true,
    });
    await apiClient.getAdminEvent("event-id");

    assert.equal(calls[0].url, "/api/admin/events?search=draft&page=2&page_size=20");
    assert.equal(calls[1].url, "/api/events");
    assert.equal(calls[1].options.headers["Idempotency-Key"], "123e4567-e89b-12d3-a456-426614174000");
    assert.equal(calls[2].url, "/api/admin/events/event-id/sales");
    assert.equal(calls[2].options.method, "PATCH");
    assert.deepEqual(JSON.parse(calls[2].options.body), { publication_status: "published", sales_status: "paused", clear_sales_window: true });
    assert.equal(calls[3].url, "/api/admin/events/event-id");
  } finally {
    globalThis.fetch = originalFetch;
  }
});
