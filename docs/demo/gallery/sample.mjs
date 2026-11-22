// All identifiers and people in this dataset are fictional.
export const ids = Object.fromEntries(
  ["event", "tier", "seat", "booking", "ticket", "buyer", "staff", "admin", "refund", "saga", "payment", "pendingBooking"]
    .map((name, i) => [name, "00000000-0000-4000-8000-" + String(i + 1).padStart(12, "0")]),
);

export const sampleTime = "2027-05-20T03:00:00Z";
export const event = {
  id: ids.event,
  title: "海港爵士之夜",
  description: "在海港音乐厅，与钢琴、低音提琴和鼓手共度一个爵士三重奏之夜。",
  category: "concerts",
  venue: "海港音乐厅",
  location: "海港市 · 滨水艺术区",
  date: "2027-06-12T11:30:00Z",
  date_version: 1,
  image_url: "",
  status: "published",
  publication_status: "published",
  sales_status: "selling",
  sales_start_at: "2027-04-01T01:00:00Z",
  sales_end_at: "2027-06-12T10:30:00Z",
  tiers: [{ id: ids.tier, event_id: ids.event, name: "池座", price_cents: 4800, total_quantity: 24, available_quantity: 23, version: 2, accounting_ready: true }],
  created_at: "2027-04-01T01:00:00Z",
};

const profiles = {
  buyer: { name: "林澄", email: "buyer@example.test", role: "user" },
  staff: { name: "周宁", email: "staff@example.test", role: "user" },
  admin: { name: "许岚", email: "admin@example.test", role: "admin" },
};

export function commonResponse({ path, role, query }) {
  if (path === "/api/users/me" && profiles[role]) {
    return { ...profiles[role], id: ids[role], locale: "zh", created_at: "2027-04-02T08:00:00Z" };
  }
  if (path === "/api/features") return { source_state: "available", flags: { discovery_advanced_filters: false } };
  if (path === "/api/events/" + ids.event) return event;
  if (path === "/api/events") {
    const search = (query.get("search") || "").trim().toLowerCase();
    const category = query.get("category");
    const matches = (!category || category === event.category) &&
      [event.title, event.description, event.venue, event.location].some(value => value.toLowerCase().includes(search));
    return { events: matches ? [event] : [], total: matches ? 1 : 0, page: 1, page_size: 20 };
  }
}
