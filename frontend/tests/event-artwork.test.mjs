import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { loadTypeScript } from "./render-typescript.mjs";

test("artwork uses supplied local or remote sources and trims whitespace", () => {
  const { resolveEventArtwork } = loadTypeScript("lib/event-artwork.ts");
  assert.equal(
    resolveEventArtwork("https://example.test/live.jpg", "RushGo Live — Reliability Night"),
    "https://example.test/live.jpg",
  );
  assert.equal(resolveEventArtwork("  /artwork/rushgo-premiere-night.png  ", "Any event"), "/artwork/rushgo-premiere-night.png");
});

test("missing API images render fallback even for branded demo titles", () => {
  const { EventArtwork } = loadTypeScript("components/event-artwork.tsx");
  for (const title of ["RushGo Live — Reliability Night", "RushGo Arena Finals", "RushGo Premiere Night"]) {
    for (const src of [null, undefined, "", "  "]) {
      const html = renderToStaticMarkup(React.createElement(EventArtwork, { title, src }));
      assert.match(html, /data-event-artwork="fallback"/);
      assert.doesNotMatch(html, /<img\b/);
    }
  }
});

test("supplied image data renders independently of the event title", () => {
  const { EventArtwork } = loadTypeScript("components/event-artwork.tsx");
  const html = renderToStaticMarkup(React.createElement(EventArtwork, {
    title: "An event renamed by its organizer", src: "/artwork/rushgo-arena-finals.png",
  }));
  assert.match(html, /data-event-artwork="image"/);
  assert.match(html, /<img[^>]*src="\/artwork\/rushgo-arena-finals.png"/);
});

test("unknown events with no artwork retain fallback behavior", () => {
  const { resolveEventArtwork } = loadTypeScript("lib/event-artwork.ts");
  for (const title of ["Unknown Event", "constructor", "toString", "__proto__"]) {
    assert.equal(resolveEventArtwork(undefined, title), "");
  }
});
