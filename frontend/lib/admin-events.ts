import type {
  ApiCreateEventInput,
  ApiEvent,
  ApiUpdateEventSalesInput,
  ApiUpdateEventInput,
} from "./api/types";

/**
 * Pure form helpers for the admin event console.
 *
 * The gateway accepts price_cents (int64) and RFC3339 dates; the form works
 * in dollars and datetime-local strings. All conversions live here so they
 * can be unit-tested without a browser or an API server.
 */

export interface EventTierDraft {
  name: string;
  price: string; // dollars, at most two decimals
  quantity: string; // positive integer
}

export interface EventFormState {
  title: string;
  description: string;
  category: string;
  venue: string;
  location: string;
  date: string; // datetime-local value (local time)
  imageUrl: string;
  publicationStatus: "draft" | "published";
  salesStatus: "scheduled" | "selling" | "paused" | "ended" | "cancelled";
  salesStartAt: string;
  salesEndAt: string;
  tiers: EventTierDraft[];
}

export interface EventTierErrors {
  name?: EventFormErrorKey;
  price?: EventFormErrorKey;
  quantity?: EventFormErrorKey;
}

export type EventFormErrorKey =
  | "admin.events.validation.titleRequired"
  | "admin.events.validation.categoryRequired"
  | "admin.events.validation.venueRequired"
  | "admin.events.validation.locationRequired"
  | "admin.events.validation.invalidDate"
  | "admin.events.validation.descriptionCannotClear"
  | "admin.events.validation.imageCannotClear"
  | "admin.events.validation.addTier"
  | "admin.events.validation.tierNameRequired"
  | "admin.events.validation.invalidPrice"
  | "admin.events.validation.invalidQuantity"
  | "admin.events.validation.invalidSalesStart"
  | "admin.events.validation.invalidSalesEnd"
  | "admin.events.validation.salesWindowPair"
  | "admin.events.validation.salesWindowOrder";

export interface EventFormErrors {
  title?: EventFormErrorKey;
  description?: EventFormErrorKey;
  category?: EventFormErrorKey;
  venue?: EventFormErrorKey;
  location?: EventFormErrorKey;
  date?: EventFormErrorKey;
  imageUrl?: EventFormErrorKey;
  salesStartAt?: EventFormErrorKey;
  salesEndAt?: EventFormErrorKey;
  salesWindow?: EventFormErrorKey;
  tiers?: EventFormErrorKey;
  tierRows?: EventTierErrors[];
}

export const MAX_TIER_QUANTITY = 2147483647;

export function emptyTierDraft(): EventTierDraft {
  return { name: "", price: "", quantity: "" };
}

export function emptyEventForm(): EventFormState {
  return {
    title: "",
    description: "",
    category: "",
    venue: "",
    location: "",
    date: "",
    imageUrl: "",
    publicationStatus: "draft",
    salesStatus: "scheduled",
    salesStartAt: "",
    salesEndAt: "",
    tiers: [emptyTierDraft()],
  };
}

export function eventToForm(event: ApiEvent): EventFormState {
  return {
    title: event.title ?? "",
    description: event.description ?? "",
    category: event.category ?? "",
    venue: event.venue ?? "",
    location: event.location ?? "",
    date: toDateTimeLocal(event.date),
    imageUrl: event.image_url ?? "",
    publicationStatus: event.publication_status === "published" ? "published" : "draft",
    salesStatus: event.sales_status === "selling" || event.sales_status === "paused" || event.sales_status === "ended" || event.sales_status === "cancelled" ? event.sales_status : "scheduled",
    salesStartAt: event.sales_start_at ? toDateTimeLocal(event.sales_start_at) : "",
    salesEndAt: event.sales_end_at ? toDateTimeLocal(event.sales_end_at) : "",
    tiers: [],
  };
}

export function eventCreatePayloadToForm(payload: ApiCreateEventInput): EventFormState {
  return {
    title: payload.title,
    description: payload.description,
    category: payload.category,
    venue: payload.venue,
    location: payload.location,
    date: toDateTimeLocal(payload.date),
    imageUrl: payload.image_url,
    publicationStatus: payload.publication_status ?? "draft",
    salesStatus: payload.sales_status ?? "scheduled",
    salesStartAt: payload.sales_start_at ? toDateTimeLocal(payload.sales_start_at) : "",
    salesEndAt: payload.sales_end_at ? toDateTimeLocal(payload.sales_end_at) : "",
    tiers: payload.tiers.map((tier) => ({
      name: tier.name,
      price: `${Math.floor(tier.price_cents / 100)}.${String(tier.price_cents % 100).padStart(2, "0")}`,
      quantity: String(tier.total_quantity),
    })),
  };
}

/** Dollars string → integer cents, or null when the input is not a valid price. */
export function priceToCents(raw: string): number | null {
  const value = raw.trim();
  if (!/^\d+(?:\.\d{1,2})?$/.test(value)) return null;
  const [dollars, fraction = ""] = value.split(".");
  const cents = Number(dollars) * 100 + Number((fraction + "00").slice(0, 2));
  if (!Number.isSafeInteger(cents) || cents <= 0) return null;
  return cents;
}

export function formatCents(cents: number, locale: "en-US" | "zh-CN" = "en-US"): string {
  if (!Number.isFinite(cents)) return "—";
  return new Intl.NumberFormat(locale, { style: "currency", currency: "USD" }).format(cents / 100);
}

export interface PageWindow {
  page: number;
  pageSize: number;
  total: number;
  totalPages: number;
  start: number;
  end: number;
  hasPrev: boolean;
  hasNext: boolean;
}

/**
 * Page maths for a server-backed list. `total` must come from the API; this
 * helper never invents a count. An empty result reports zero pages.
 */
export function pageWindow(
  page: number,
  pageSize: number,
  total: number,
): PageWindow {
  const safeSize = Number.isFinite(pageSize)
    ? Math.max(1, Math.floor(pageSize))
    : 1;
  const safeTotal = Number.isFinite(total) ? Math.max(0, Math.floor(total)) : 0;
  const totalPages = Math.ceil(safeTotal / safeSize);
  const requested = Number.isFinite(page) ? Math.floor(page) : 1;
  const safePage =
    totalPages === 0 ? 1 : Math.min(Math.max(1, requested), totalPages);
  const start = safeTotal === 0 ? 0 : (safePage - 1) * safeSize + 1;
  const end = safeTotal === 0 ? 0 : Math.min(safePage * safeSize, safeTotal);
  return {
    page: safePage,
    pageSize: safeSize,
    total: safeTotal,
    totalPages,
    start,
    end,
    hasPrev: safePage > 1,
    hasNext: safePage < totalPages,
  };
}

/**
 * Authoritative inventory figures for exactly the events passed in (one
 * server page), never a platform-wide total.
 */
export function pageInventory(events: ApiEvent[]): {
  tiers: number;
  availableSeats: number;
} {
  return events.reduce(
    (acc, event) => {
      const tiers = event.tiers ?? [];
      acc.tiers += tiers.length;
      for (const tier of tiers) {
        if (Number.isFinite(tier.available_quantity)) {
          acc.availableSeats += Math.max(0, tier.available_quantity);
        }
      }
      return acc;
    },
    { tiers: 0, availableSeats: 0 },
  );
}

/** Positive int32 string → number, or null. */
export function parseQuantity(raw: string): number | null {
  const value = raw.trim();
  if (!/^\d+$/.test(value)) return null;
  const quantity = Number(value);
  if (!Number.isSafeInteger(quantity) || quantity < 1 || quantity > MAX_TIER_QUANTITY) {
    return null;
  }
  return quantity;
}

/** Local datetime-local value → RFC3339 UTC string, or null. */
export function toRfc3339(local: string): string | null {
  if (!local.trim()) return null;
  const date = new Date(local);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

/** RFC3339 → datetime-local value in local time, preserving seconds and ms. */
export function toDateTimeLocal(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "";
  const pad = (part: number) => String(part).padStart(2, "0");
  const milliseconds = date.getMilliseconds();
  const fraction = milliseconds ? `.${String(milliseconds).padStart(3, "0")}` : "";
  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}` +
    `T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}` +
    fraction
  );
}

export function validateEventForm(
  form: EventFormState,
  options: {
    requireTiers: boolean;
    original?: { description?: string; imageUrl?: string };
  },
): EventFormErrors {
  const errors: EventFormErrors = {};
  if (!form.title.trim()) errors.title = "admin.events.validation.titleRequired";
  if (!form.category.trim()) errors.category = "admin.events.validation.categoryRequired";
  if (!form.venue.trim()) errors.venue = "admin.events.validation.venueRequired";
  if (!form.location.trim()) errors.location = "admin.events.validation.locationRequired";
  if (!toRfc3339(form.date)) errors.date = "admin.events.validation.invalidDate";
  const salesStart = form.salesStartAt?.trim() ?? "";
  const salesEnd = form.salesEndAt?.trim() ?? "";
  const salesStartRfc = salesStart ? toRfc3339(salesStart) : "";
  const salesEndRfc = salesEnd ? toRfc3339(salesEnd) : "";
  if (salesStart && !salesStartRfc) errors.salesStartAt = "admin.events.validation.invalidSalesStart";
  if (salesEnd && !salesEndRfc) errors.salesEndAt = "admin.events.validation.invalidSalesEnd";
  if (Boolean(salesStart) !== Boolean(salesEnd)) errors.salesWindow = "admin.events.validation.salesWindowPair";
  if (salesStartRfc && salesEndRfc && Date.parse(salesEndRfc) <= Date.parse(salesStartRfc)) {
    errors.salesWindow = "admin.events.validation.salesWindowOrder";
  }
  // The gateway ignores empty strings on update, so clearing one would be
  // silently dropped while the console reported success.
  if (options.original?.description && !form.description.trim()) {
    errors.description = "admin.events.validation.descriptionCannotClear";
  }
  if (options.original?.imageUrl && !form.imageUrl.trim()) {
    errors.imageUrl = "admin.events.validation.imageCannotClear";
  }

  if (options.requireTiers) {
    if (form.tiers.length === 0) {
      errors.tiers = "admin.events.validation.addTier";
    } else {
      const rows = form.tiers.map<EventTierErrors>((tier) => {
        const row: EventTierErrors = {};
        if (!tier.name.trim()) row.name = "admin.events.validation.tierNameRequired";
        if (priceToCents(tier.price) === null) {
          row.price = "admin.events.validation.invalidPrice";
        }
        if (parseQuantity(tier.quantity) === null) {
          row.quantity = "admin.events.validation.invalidQuantity";
        }
        return row;
      });
      if (rows.some((row) => Object.keys(row).length > 0)) {
        errors.tierRows = rows;
      }
    }
  }
  return errors;
}

export function hasErrors(errors: EventFormErrors): boolean {
  return Object.keys(errors).some((key) => {
    if (key === "tierRows") {
      return (errors.tierRows ?? []).some((row) => Object.keys(row).length > 0);
    }
    return errors[key as keyof EventFormErrors] !== undefined;
  });
}

export function buildCreatePayload(
  form: EventFormState,
): ApiCreateEventInput | null {
  const date = toRfc3339(form.date);
  if (!date || form.tiers.length === 0) return null;
  const salesStartAt = form.salesStartAt?.trim() ? toRfc3339(form.salesStartAt) : "";
  const salesEndAt = form.salesEndAt?.trim() ? toRfc3339(form.salesEndAt) : "";
  if ((form.salesStartAt?.trim() && !salesStartAt) || (form.salesEndAt?.trim() && !salesEndAt) || Boolean(salesStartAt) !== Boolean(salesEndAt) || (salesStartAt && salesEndAt && Date.parse(salesEndAt) <= Date.parse(salesStartAt))) return null;
  const tiers = form.tiers.map((tier) => ({
    name: tier.name.trim(),
    price_cents: priceToCents(tier.price) ?? 0,
    total_quantity: parseQuantity(tier.quantity) ?? 0,
  }));
  if (tiers.some((tier) => tier.price_cents <= 0 || tier.total_quantity <= 0)) {
    return null;
  }
  return {
    title: form.title.trim(),
    description: form.description.trim(),
    category: form.category.trim(),
    venue: form.venue.trim(),
    location: form.location.trim(),
    date,
    image_url: form.imageUrl.trim(),
    publication_status: form.publicationStatus ?? "draft",
    sales_status: form.salesStatus ?? "scheduled",
    sales_start_at: salesStartAt || "",
    sales_end_at: salesEndAt || "",
    tiers,
  };
}

export function buildSalesUpdatePayload(form: EventFormState): ApiUpdateEventSalesInput | null {
  const startInput = form.salesStartAt?.trim() ?? "";
  const endInput = form.salesEndAt?.trim() ?? "";
  const start = startInput ? toRfc3339(startInput) : "";
  const end = endInput ? toRfc3339(endInput) : "";
  if ((startInput && !start) || (endInput && !end) || Boolean(start) !== Boolean(end) || (start && end && Date.parse(end) <= Date.parse(start))) return null;
  return {
    publication_status: form.publicationStatus,
    sales_status: form.salesStatus,
    sales_start_at: start || undefined,
    sales_end_at: end || undefined,
    clear_sales_window: !start && !end,
  };
}

/**
 * The gateway PUT is full-field for the text fields (empty values are ignored
 * server-side), so the console always sends every editable field to avoid
 * accidentally leaving a stale value behind.
 */
export function buildUpdatePayload(
  form: EventFormState,
  originalDate?: string,
): ApiUpdateEventInput | null {
  // If the datetime-local field still represents the stored instant, send the
  // original RFC3339 verbatim so seconds/fractional seconds cannot drift
  // through the form's local-time round trip.
  const date =
    originalDate && toDateTimeLocal(originalDate) === form.date
      ? originalDate
      : toRfc3339(form.date);
  if (!date) return null;
  return {
    title: form.title.trim(),
    description: form.description.trim(),
    category: form.category.trim(),
    venue: form.venue.trim(),
    location: form.location.trim(),
    date,
    image_url: form.imageUrl.trim(),
  };
}

export function formatEventDate(iso: string, locale: "en-US" | "zh-CN" = "en-US"): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return iso;
  return date.toLocaleString(locale, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function tierSummary(event: ApiEvent, locale: "en-US" | "zh-CN" = "en-US"): string {
  const tiers = event.tiers ?? [];
  if (tiers.length === 0) return locale === "zh-CN" ? "无票档" : "No tiers";
  const cheapest = tiers.reduce(
    (min, tier) => Math.min(min, tier.price_cents),
    Number.POSITIVE_INFINITY,
  );
  const count = new Intl.NumberFormat(locale).format(tiers.length);
  return locale === "zh-CN"
    ? `${count} 个票档 · 最低 ${formatCents(cheapest, locale)}`
    : `${count} tier${tiers.length === 1 ? "" : "s"} · from ${formatCents(cheapest, locale)}`;
}
