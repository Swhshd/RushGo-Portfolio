import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { loadTypeScript } from "./render-typescript.mjs";

test("confirmed ticket information contains no inactive View Ticket action", () => {
  const { buildWalletOrder } = loadTypeScript("lib/wallet.ts");
  const { WalletOrderCard } = loadTypeScript("components/wallet-order-card.tsx");
  const event = {
    id: "event",
    title: "Confirmed event",
    description: "",
    category: "concerts",
    venue: "RushGo Arena",
    location: "Demo City",
    date: "2099-01-01T00:00:00Z",
    image_url: "",
    status: "active",
    tiers: [{ id: "tier", event_id: "event", name: "Floor", price_cents: 1000, total_quantity: 10, available_quantity: 10, version: 1 }],
    created_at: "2026-01-01T00:00:00Z",
  };
  const order = buildWalletOrder(
    {
      id: "ticket",
      event_id: "event",
      user_id: "user",
      status: "CONFIRMED",
      total_amount_cents: 1000,
      created_at: "2026-01-01T00:00:00Z",
      items: [{ id: "item", ticket_tier_id: "tier", tier_name: "", quantity: 1, unit_price_cents: 1000, seat_ids: [] }],
    },
    event,
  );
  const html = renderToStaticMarkup(
    React.createElement(WalletOrderCard, {
      order,
      expanded: false,
      onToggle() {},
    }),
  );
  assert.match(html, /Confirmed event/);
  assert.match(html, /\$10\.00/);
  assert.match(html, /View order details/);
  assert.doesNotMatch(html, /<button[^>]*>.*?View Ticket/s);
});

test("venue sections expose keyboard actions and disabled sold-out states", () => {
  const { VenueMap } = loadTypeScript("components/venue-map.tsx");
  const html = renderToStaticMarkup(React.createElement(VenueMap, { layout: { sections: [{ id: "floor", name: "Floor", path: "M 0 0 L 10 10", availableSeats: 2, accountingReady: true, labelPosition: { x: 5, y: 5 } }, { id: "sold", name: "Balcony", availableSeats: 0, accountingReady: true, labelPosition: { x: 15, y: 5 } }] }, onSectionClick() {} }));
  assert.match(html, /role="button"[^>]*aria-label="Select Floor"[^>]*tabindex="0"/);
  assert.match(html, /aria-label="Balcony, Sold out"[^>]*tabindex="-1"[^>]*aria-disabled="true"/);
});
