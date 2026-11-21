import { createServer } from "node:http";

const port = 8765;
const createdAt = "2026-11-22T00:00:00Z";

const events = [
  {
    id: "local-sample-afterglow-sessions",
    title: "Afterglow Sessions",
    description: "An evening of new electronic music in an intimate downtown venue.",
    category: "concerts",
    venue: "The Lantern Room",
    location: "Seattle, WA",
    date: "2026-12-12T19:30:00Z",
    image_url: "",
    status: "published",
    publication_status: "published",
    sales_status: "paused",
    tiers: [{ id: "local-sample-afterglow-tier", name: "General Admission", price_cents: 3200, total_quantity: 0, available_quantity: 0, accounting_ready: false }],
    created_at: createdAt,
  },
  {
    id: "local-sample-harbor-jazz-weekend",
    title: "Harbor Jazz Weekend",
    description: "Three nights of small ensembles and late sets by the waterfront.",
    category: "concerts",
    venue: "Pier Hall",
    location: "Portland, OR",
    date: "2027-01-16T20:00:00Z",
    image_url: "",
    status: "published",
    publication_status: "published",
    sales_status: "paused",
    tiers: [{ id: "local-sample-harbor-jazz-tier", name: "Evening Pass", price_cents: 4500, total_quantity: 0, available_quantity: 0, accounting_ready: false }],
    created_at: createdAt,
  },
  {
    id: "local-sample-arena-finals-night",
    title: "Arena Finals Night",
    description: "A fictional championship showcase at the city arena.",
    category: "sports",
    venue: "Northline Arena",
    location: "Vancouver, BC",
    date: "2027-02-06T18:00:00Z",
    image_url: "",
    status: "published",
    publication_status: "published",
    sales_status: "paused",
    tiers: [{ id: "local-sample-arena-finals-tier", name: "Upper Level", price_cents: 2800, total_quantity: 0, available_quantity: 0, accounting_ready: false }],
    created_at: createdAt,
  },
];

function sendJson(response, status, body) {
  response.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    "X-RushGo-Demo-Data": "fictional-read-only-sample",
  });
  response.end(JSON.stringify(body));
}

const server = createServer((request, response) => {
  const url = new URL(request.url ?? "/", `http://127.0.0.1:${port}`);

  if (request.method !== "GET") {
    sendJson(response, 405, { error: "demo fixture is read-only" });
    return;
  }

  if (url.pathname === "/api/features") {
    sendJson(response, 200, {
      source_state: "available",
      flags: { discovery_advanced_filters: false },
    });
    return;
  }

  if (url.pathname === "/api/events") {
    const category = url.searchParams.get("category")?.toLowerCase();
    const search = url.searchParams.get("search")?.trim().toLowerCase();
    const matchingEvents = events.filter((event) => {
      const categoryMatches = !category || event.category === category;
      const searchableText = `${event.title} ${event.description} ${event.venue} ${event.location}`.toLowerCase();
      return categoryMatches && (!search || searchableText.includes(search));
    });
    const page = Number(url.searchParams.get("page") || "1");
    const pageSize = Number(url.searchParams.get("page_size") || "20");
    const offset = (page - 1) * pageSize;

    sendJson(response, 200, {
      events: matchingEvents.slice(offset, offset + pageSize),
      total: matchingEvents.length,
      page,
      page_size: pageSize,
    });
    return;
  }

  sendJson(response, 404, { error: "endpoint not included in the demo fixture" });
});

server.listen(port, "127.0.0.1", () => {
  process.stdout.write(`Read-only sample API listening at http://127.0.0.1:${port}\n`);
});

server.on("error", (error) => {
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
});
