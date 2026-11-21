import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { loadTypeScript } from "./render-typescript.mjs";

const { getI18n } = loadTypeScript("lib/i18n/core.ts");
const i18nOverride = (locale = "en-US") => ({ "@/lib/i18n": { useI18n: () => getI18n(locale) } });

const event = {
  id: "779c2c8d-1157-41e1-93d9-e727a200e8ea",
  title: "Reliability Night",
  description: "A live demo of owner-safe reservations.",
  category: "concerts",
  venue: "RushGo Arena",
  location: "Ho Chi Minh City",
  date: "2026-12-15T20:00:00Z",
  image_url: "",
  status: "active",
  tiers: [
    {
      id: "54bf523d-2d10-4629-ac25-93c20bd420f0",
      event_id: "779c2c8d-1157-41e1-93d9-e727a200e8ea",
      name: "Floor",
      price_cents: 2500,
      total_quantity: 12,
      available_quantity: 10,
      version: 3,
    },
  ],
  created_at: "2026-10-08T17:07:01Z",
};

test("detail view renders only authoritative fields and invents no sold count", () => {
  const { EventDetailContent } = loadTypeScript(
    "components/admin-event-detail-drawer.tsx",
    i18nOverride(),
  );
  const html = renderToStaticMarkup(
    React.createElement(EventDetailContent, { event }),
  );

  assert.match(html, /Reliability Night/);
  assert.match(html, /A live demo of owner-safe reservations\./);
  assert.match(html, /RushGo Arena/);
  assert.match(html, /Ho Chi Minh City/);
  assert.match(html, /\$25\.00/);
  assert.match(html, /10 available of 12 total/);
  assert.match(html, /active/);
  assert.match(html, new RegExp(event.id));
  assert.doesNotMatch(html, /<img/i);
  // total - available is not a sales ledger; never display it as "sold".
  assert.doesNotMatch(html, /sold/i);
});

test("event detail and scanner assignment copy follows locale while preserving dynamic fields", () => {
  const { EventDetailContent } = loadTypeScript(
    "components/admin-event-detail-drawer.tsx",
    i18nOverride("zh-CN"),
  );
  const html = renderToStaticMarkup(React.createElement(EventDetailContent, { event }));
  assert.match(html, /活动 ID/);
  assert.match(html, /检票员分配/);
  assert.match(html, /Reliability Night/);
  assert.match(html, new RegExp(event.id));
  assert.match(html, /Ho Chi Minh City/);
});

test("detail drawer uses dialog semantics and a loading status", () => {
  const { EventDetailDrawer } = loadTypeScript(
    "components/admin-event-detail-drawer.tsx",
    i18nOverride(),
  );
  const html = renderToStaticMarkup(
    React.createElement(EventDetailDrawer, {
      eventId: event.id,
      onClose() {},
      onEdit() {},
    }),
  );

  assert.match(html, /role="dialog"/);
  assert.match(html, /aria-modal="true"/);
  assert.match(
    html,
    /aria-labelledby="admin-event-detail-heading admin-event-detail-title"/,
  );
  assert.match(html, /id="admin-event-detail-heading"/);
  assert.match(html, /role="status"/);
  assert.match(html, /Loading event details/);
});

test("admin event detail exposes event-scoped scanner assignment controls", () => {
  const { EventDetailContent } = loadTypeScript(
    "components/admin-event-detail-drawer.tsx",
    i18nOverride(),
  );
  const html = renderToStaticMarkup(
    React.createElement(EventDetailContent, { event }),
  );

  assert.match(html, /Scanner assignments/);
  assert.match(html, /Staff user ID/);
  assert.match(html, /Grant scanner access/);
  assert.match(html, /Loading scanner assignments/);
  assert.match(html, /Only administrators can manage scanner access/);
});

test("form drawer wires field errors to aria-invalid and aria-describedby", () => {
  const { EventFormDrawer } = loadTypeScript(
    "components/admin-event-form-drawer.tsx",
    i18nOverride(),
  );
  const form = {
    title: "",
    description: "",
    category: "",
    venue: "",
    location: "",
    date: "",
    imageUrl: "",
    tiers: [{ name: "", price: "", quantity: "" }],
  };
  const html = renderToStaticMarkup(
    React.createElement(EventFormDrawer, {
      editing: null,
      form,
      errors: { title: "admin.events.validation.titleRequired" },
      submitting: false,
      submitError: null,
      onField() {},
      onFieldBlur() {},
      onTier() {},
      onTierBlur() {},
      onAddTier() {},
      onRemoveTier() {},
      onClose() {},
      onSubmit() {},
    }),
  );

  assert.match(html, /role="dialog"/);
  assert.match(html, /aria-modal="true"/);
  assert.match(html, /aria-labelledby="admin-event-form-heading"/);
  assert.match(
    html,
    /id="event-title"[^>]*aria-invalid="true"[^>]*aria-describedby="event-title-error"/,
  );
  assert.match(html, /id="event-title-error"/);
  assert.match(html, /Title is required\./);
  // An untouched form field must not be marked invalid.
  assert.doesNotMatch(
    html,
    /id="event-venue"[^>]*aria-invalid="true"/,
  );
});

test("form labels and semantic validation errors render in the selected locale", () => {
  const form = {
    title: "",
    description: "",
    category: "",
    venue: "",
    location: "",
    date: "",
    imageUrl: "",
    tiers: [{ name: "", price: "", quantity: "" }],
  };
  function render(locale) {
    const { EventFormDrawer } = loadTypeScript(
      "components/admin-event-form-drawer.tsx",
      i18nOverride(locale),
    );
    return renderToStaticMarkup(React.createElement(EventFormDrawer, {
      editing: null,
      form,
      errors: { title: "admin.events.validation.titleRequired" },
      submitting: false,
      submitError: null,
      onField() {},
      onFieldBlur() {},
      onTier() {},
      onTierBlur() {},
      onAddTier() {},
      onRemoveTier() {},
      onClose() {},
      onSubmit() {},
    }));
  }
  const en = render("en-US");
  const zh = render("zh-CN");
  assert.match(en, />Title</);
  assert.match(en, /Title is required\./);
  assert.match(zh, />标题</);
  assert.match(zh, /请填写标题。/);
});

test("edit form keeps a stored non-canonical category selectable", () => {
  const { EventFormDrawer } = loadTypeScript(
    "components/admin-event-form-drawer.tsx",
    i18nOverride(),
  );
  const form = {
    title: "Theater night",
    description: "",
    category: "theater",
    venue: "Hall",
    location: "City",
    date: "2026-12-05T10:15:30",
    imageUrl: "",
    tiers: [],
  };
  const html = renderToStaticMarkup(
    React.createElement(EventFormDrawer, {
      editing: { ...event, category: "theater" },
      form,
      errors: {},
      submitting: false,
      submitError: null,
      onField() {},
      onFieldBlur() {},
      onTier() {},
      onTierBlur() {},
      onAddTier() {},
      onRemoveTier() {},
      onClose() {},
      onSubmit() {},
    }),
  );

  assert.match(html, /<option value="theater"[^>]*selected[^>]*>theater<\/option>/);
});

test("all form controls are locked while a write is in flight", () => {
  const { EventFormDrawer } = loadTypeScript(
    "components/admin-event-form-drawer.tsx",
    i18nOverride(),
  );
  const form = {
    title: "",
    description: "",
    category: "",
    venue: "",
    location: "",
    date: "",
    imageUrl: "",
    tiers: [{ name: "", price: "", quantity: "" }],
  };
  const html = renderToStaticMarkup(
    React.createElement(EventFormDrawer, {
      editing: null,
      form,
      errors: {},
      submitting: true,
      submitError: null,
      onField() {},
      onFieldBlur() {},
      onTier() {},
      onTierBlur() {},
      onAddTier() {},
      onRemoveTier() {},
      onClose() {},
      onSubmit() {},
    }),
  );

  assert.match(html, /<fieldset[^>]*disabled=""/);
  assert.match(html, /<button[^>]*disabled=""[^>]*>.*?Create event/s);
});

test("form drawer shows no errors for an untouched create form", () => {
  const { EventFormDrawer } = loadTypeScript(
    "components/admin-event-form-drawer.tsx",
    i18nOverride(),
  );
  const form = {
    title: "",
    description: "",
    category: "",
    venue: "",
    location: "",
    date: "",
    imageUrl: "",
    tiers: [{ name: "", price: "", quantity: "" }],
  };
  const html = renderToStaticMarkup(
    React.createElement(EventFormDrawer, {
      editing: null,
      form,
      errors: {},
      submitting: false,
      submitError: null,
      onField() {},
      onFieldBlur() {},
      onTier() {},
      onTierBlur() {},
      onAddTier() {},
      onRemoveTier() {},
      onClose() {},
      onSubmit() {},
    }),
  );

  assert.doesNotMatch(html, /aria-invalid="true"/);
  assert.doesNotMatch(html, /role="alert"/);
});
