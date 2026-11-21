import { apiClient } from "./api/client";
import { transformEvent } from "./api/transformers";
import type { Event } from "./types";

export const discoveryCategories = [
  { category: "", label: "Discover" },
  { category: "concerts", label: "Concerts" },
  { category: "sports", label: "Sports" },
  { category: "films", label: "Films" },
] as const;

export type DiscoverySort = "date_asc" | "date_desc" | "price_asc" | "price_desc";

export interface DiscoveryState {
  category: string;
  search: string;
  dateFrom: string;
  dateTo: string;
  location: string;
  minPriceCents?: number;
  maxPriceCents?: number;
  sort: DiscoverySort;
  page: number;
}

export interface DiscoveryPage {
  events: Event[];
  total: number;
  page: number;
  pageSize: number;
}

export const emptyDiscovery: DiscoveryState = {
  category: "",
  search: "",
  dateFrom: "",
  dateTo: "",
  location: "",
  sort: "date_asc",
  page: 1,
};

export function discoveryStateForFeature(state: DiscoveryState, advancedAvailable: boolean): DiscoveryState {
  if (advancedAvailable) return state;
  return { ...state, dateFrom: "", dateTo: "", location: "", minPriceCents: undefined, maxPriceCents: undefined, sort: "date_asc", page: 1 };
}

function validDateOnly(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const date = new Date(`${value}T00:00:00.000Z`);
  return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10) === value;
}

function readPrice(value: string | null): number | undefined {
  if (value === null || !/^\d+$/.test(value)) return undefined;
  const number = Number(value);
  return Number.isSafeInteger(number) ? number : undefined;
}

function readPage(value: string | null): number {
  if (!value || !/^\d+$/.test(value)) return 1;
  const page = Number(value);
  return Number.isSafeInteger(page) && page >= 1 && page <= 1000 ? page : 1;
}

export function readDiscovery(params: Pick<URLSearchParams, "get">): DiscoveryState {
  const category = (params.get("category") || "").trim().toLowerCase();
  const sortValue = params.get("sort");
  const dateFrom = (params.get("date_from") || "").trim();
  const dateTo = (params.get("date_to") || "").trim();
  return {
    category: discoveryCategories.some((item) => item.category === category) ? category : "",
    search: (params.get("search") || "").trim(),
    dateFrom: validDateOnly(dateFrom) ? dateFrom : "",
    dateTo: validDateOnly(dateTo) ? dateTo : "",
    location: (params.get("location") || "").trim(),
    minPriceCents: readPrice(params.get("min_price_cents")),
    maxPriceCents: readPrice(params.get("max_price_cents")),
    sort: sortValue === "date_desc" || sortValue === "price_asc" || sortValue === "price_desc" ? sortValue : "date_asc",
    page: readPage(params.get("page")),
  };
}

export function discoveryHref(state: DiscoveryState, update: Partial<DiscoveryState> = {}): string {
  const changed = Object.keys(update).length > 0;
  const next = { ...emptyDiscovery, ...state, ...update, page: update.page ?? (changed ? 1 : state.page ?? 1) };
  const params = new URLSearchParams();
  if (next.category) params.set("category", next.category);
  if (next.search.trim()) params.set("search", next.search.trim());
  if (next.dateFrom) params.set("date_from", next.dateFrom);
  if (next.dateTo) params.set("date_to", next.dateTo);
  if (next.location.trim()) params.set("location", next.location.trim());
  if (next.minPriceCents !== undefined) params.set("min_price_cents", String(next.minPriceCents));
  if (next.maxPriceCents !== undefined) params.set("max_price_cents", String(next.maxPriceCents));
  if (next.sort !== "date_asc") params.set("sort", next.sort);
  if (next.page > 1) params.set("page", String(next.page));
  const query = params.toString();
  return query ? `/?${query}` : "/";
}

export async function loadDiscoveryPage(state: DiscoveryState, signal?: AbortSignal): Promise<DiscoveryPage> {
  const result = await apiClient.listEventsPage({
    category: state.category,
    search: state.search,
    ...(state.dateFrom ? { date_from: state.dateFrom } : {}),
    ...(state.dateTo ? { date_to: state.dateTo } : {}),
    ...(state.location ? { location: state.location } : {}),
    ...(state.minPriceCents !== undefined ? { min_price_cents: state.minPriceCents } : {}),
    ...(state.maxPriceCents !== undefined ? { max_price_cents: state.maxPriceCents } : {}),
    sort: state.sort,
    page: state.page,
    page_size: 20,
  }, signal);
  return {
    events: result.events.map(transformEvent),
    total: result.total,
    page: result.page,
    pageSize: result.page_size,
  };
}
