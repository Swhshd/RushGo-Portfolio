"use client";

/**
 * Admin > Events (V1.1)
 *
 * Lists events with server-backed pagination, search and category filtering,
 * opens a read-only detail drawer, and creates/edits events through the
 * gateway's admin-only endpoints (POST /api/events, PUT /api/events/:id).
 * Authorization is enforced server-side by AdminOnly; this page only mirrors
 * the 401/403 responses. DELETE is intentionally not part of this console.
 *
 * The gateway ignores status/sort parameters, so this page never offers a
 * platform-wide status filter or a fake server sort. Status filtering is
 * explicitly limited to the rows on the current page.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import {
  Activity,
  AlertCircle,
  CalendarDays,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  Eye,
  Layers,
  Loader2,
  Pencil,
  Plus,
  Search,
  Ticket,
  X,
} from "lucide-react";
import { apiClient } from "@/lib/api/client";
import type { ApiEvent } from "@/lib/api/types";
import { discoveryCategories } from "@/lib/discovery";
import {
  buildCreatePayload,
  buildSalesUpdatePayload,
  buildUpdatePayload,
  emptyEventForm,
  emptyTierDraft,
  eventToForm,
  eventCreatePayloadToForm,
  hasErrors,
  pageInventory,
  pageWindow,
  validateEventForm,
  type EventFormErrors,
  type EventFormState,
  type EventTierErrors,
  type EventTierDraft,
} from "@/lib/admin-events";
import { EventDetailDrawer } from "@/components/admin-event-detail-drawer";
import { EventFormDrawer } from "@/components/admin-event-form-drawer";
import { isAdminEventOperationsId } from "@/lib/admin-event-operations";
import { useI18n, type TranslationKey } from "@/lib/i18n";
import { clearPendingEventCreate, readPendingEventCreate, savePendingEventCreate, type PendingEventCreate } from "@/lib/event-create-draft";

const PAGE_SIZE = 20;
const SEARCH_DEBOUNCE_MS = 300;

const DISCOVERY_CATEGORY_OPTIONS = discoveryCategories.filter(
  (item) => item.category,
);

function errorMessage(err: unknown): TranslationKey {
  const apiErrorCode =
    typeof err === "object" && err !== null && "code" in err
      ? (err as { code?: unknown }).code
      : undefined;
  if (apiErrorCode === "EVENT_DATE_CONFLICT") return "admin.events.dateConflict";
  if (apiErrorCode === "EVENT_REFUND_TIME_PENDING") return "admin.events.refundTimePending";
  const status =
    typeof err === "object" && err !== null && "status" in err
      ? (err as { status?: number }).status
      : undefined;
  if (status === 403) {
    return "admin.events.accessRequired";
  }
  if (status === 401) {
    return "admin.events.sessionExpired";
  }
  return "admin.events.requestFailed";
}

export default function AdminEventsPage() {
  const { t, number, date, usdCents } = useI18n();
  const [events, setEvents] = useState<ApiEvent[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [query, setQuery] = useState("");
  const [appliedQuery, setAppliedQuery] = useState("");
  const [category, setCategory] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<TranslationKey | null>(null);

  const [detailId, setDetailId] = useState<string | null>(null);

  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<ApiEvent | null>(null);
  const [form, setForm] = useState<EventFormState>(emptyEventForm);
  const [touched, setTouched] = useState<Set<string>>(new Set());
  const [submitAttempted, setSubmitAttempted] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<TranslationKey | null>(null);
  const [pendingCreate, setPendingCreate] = useState<PendingEventCreate | null>(null);

  const [toast, setToast] = useState<TranslationKey | null>(null);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const requestSeq = useRef(0);
  const toastTimer = useRef<number | null>(null);
  const toastGeneration = useRef(0);

  useEffect(() => {
    let active = true;
    void Promise.resolve().then(() => {
      const pending = readPendingEventCreate();
      if (!active || !pending) return;
      setPendingCreate(pending);
      setForm(eventCreatePayloadToForm(pending.payload));
      setFormOpen(true);
    });
    return () => { active = false; };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const openLinkedEvent = () => {
      const linkedEventId = new URLSearchParams(window.location.search).get("eventId");
      setDetailId(isAdminEventOperationsId(linkedEventId) ? linkedEventId : null);
    };
    openLinkedEvent();
    window.addEventListener("popstate", openLinkedEvent);
    return () => window.removeEventListener("popstate", openLinkedEvent);
  }, []);

  const requestPage = useCallback(
    (targetPage: number, search: string, selectedCategory: string) =>
      apiClient.listAdminEventsPage({
        page: targetPage,
        page_size: PAGE_SIZE,
        search: search || undefined,
        category: selectedCategory || undefined,
      }),
    [],
  );
  useEffect(() => {
    const nextQuery = query.trim();
    // Do not reset pagination for a no-op edit (mount, whitespace, same text).
    if (nextQuery === appliedQuery) return;
    const timer = window.setTimeout(() => {
      requestSeq.current += 1;
      setLoading(true);
      setAppliedQuery(nextQuery);
      setPage(1);
      // The status filter only covers the current page; a new query replaces
      // that page, so carrying the old status filter over would mislead.
      setStatusFilter("");
    }, SEARCH_DEBOUNCE_MS);
    return () => window.clearTimeout(timer);
  }, [query, appliedQuery]);

  useEffect(() => {
    const seq = ++requestSeq.current;

    void requestPage(page, appliedQuery, category)
      .then((result) => {
        if (seq !== requestSeq.current) return;
        // A bounded server page, never catalog auto-pagination.
        const nextEvents = result.events ?? [];
        const nextTotal = result.total ?? 0;
        setEvents(nextEvents);
        setTotal(nextTotal);
        setLoadError(null);

        const nextWindow = pageWindow(page, PAGE_SIZE, nextTotal);
        if (nextTotal > 0 && page > nextWindow.totalPages) {
          setPage(nextWindow.totalPages);
        } else if (nextTotal === 0 && page !== 1) {
          setPage(1);
        } else {
          setLoading(false);
        }
      })
      .catch((err) => {
        if (seq !== requestSeq.current) return;
        // Never keep stale rows on screen after a failed request.
        setEvents([]);
        setTotal(0);
        setLoadError(errorMessage(err));
        setLoading(false);
      });

    return () => {
      requestSeq.current += 1;
    };
  }, [requestPage, page, appliedQuery, category, refreshNonce]);

  useEffect(
    () => () => {
      toastGeneration.current += 1;
      if (toastTimer.current !== null) {
        window.clearTimeout(toastTimer.current);
        toastTimer.current = null;
      }
    },
    [],
  );

  const pager = pageWindow(page, PAGE_SIZE, total);
  const inventory = useMemo(() => pageInventory(events), [events]);

  const statuses = useMemo(
    () =>
      Array.from(new Set(events.map((event) => event.status).filter(Boolean))).sort(),
    [events],
  );

  const categoryOptions = useMemo(() => {
    const seen = new Set<string>(
      DISCOVERY_CATEGORY_OPTIONS.map((item) => item.category),
    );
    const extra = events
      .map((event) => event.category)
      .filter((value) => value && !seen.has(value));
    return [
      ...DISCOVERY_CATEGORY_OPTIONS,
      ...Array.from(new Set(extra)).map((value) => ({
        category: value,
        label: value,
      })),
    ];
  }, [events]);

  const visible = useMemo(
    () =>
      statusFilter
        ? events.filter((event) => event.status === statusFilter)
        : events,
    [events, statusFilter],
  );

  const isFiltered = Boolean(appliedQuery || category);
  const searchPending = query.trim() !== appliedQuery;
  // A failed request must not turn into a factual "0 events" card, and a
  // loading request must not present the previous query's totals as current.
  const statsUnavailable = Boolean(loadError);
  // Pending debounce also means the displayed totals belong to the old query.
  const statsLoading = loading || searchPending;

  const isCreate = editing === null;
  const validation = useMemo(
    () =>
      validateEventForm(form, {
        requireTiers: isCreate,
        original: editing
          ? { description: editing.description, imageUrl: editing.image_url }
          : undefined,
      }),
    [form, isCreate, editing],
  );

  const displayedErrors = useMemo<EventFormErrors>(() => {
    const show = (key: string) => submitAttempted || touched.has(key);
    const out: EventFormErrors = {};
    const fields = [
      "title",
      "description",
      "category",
      "venue",
      "location",
      "date",
      "imageUrl",
      "salesStartAt",
      "salesEndAt",
      "salesWindow",
    ] as const;
    for (const field of fields) {
      const message = validation[field];
      if (show(field) && message) out[field] = message;
    }
    if (show("tiers") && validation.tiers) out.tiers = validation.tiers;
    if (validation.tierRows) {
      const rows = validation.tierRows.map((row, index) => {
        const visibleRow: EventTierErrors = {};
        for (const key of ["name", "price", "quantity"] as const) {
          if (show(`tier-${index}-${key}`) && row[key]) {
            visibleRow[key] = row[key]!;
          }
        }
        return visibleRow;
      });
      if (rows.some((row) => Object.keys(row).length > 0)) {
        out.tierRows = rows;
      }
    }
    return out;
  }, [validation, touched, submitAttempted]);

  function notify(message: TranslationKey) {
    const generation = ++toastGeneration.current;
    if (toastTimer.current !== null) {
      window.clearTimeout(toastTimer.current);
    }
    setToast(message);
    toastTimer.current = window.setTimeout(() => {
      if (generation !== toastGeneration.current) return;
      toastTimer.current = null;
      setToast(null);
    }, 3200);
  }

  function markTouched(field: string) {
    setTouched((current) => {
      if (current.has(field)) return current;
      const next = new Set(current);
      next.add(field);
      return next;
    });
  }

  function openCreate() {
    setEditing(null);
    setForm(pendingCreate ? eventCreatePayloadToForm(pendingCreate.payload) : emptyEventForm());
    setTouched(new Set());
    setSubmitAttempted(false);
    setSubmitError(null);
    setFormOpen(true);
  }

  function openEdit(event: ApiEvent) {
    closeDetail();
    setEditing(event);
    setForm(eventToForm(event));
    setTouched(new Set());
    setSubmitAttempted(false);
    setSubmitError(null);
    setFormOpen(true);
  }

  function closeDetail() {
    setDetailId(null);
    if (typeof window === "undefined") return;
    const current = new URL(window.location.href);
    if (!current.searchParams.has("eventId")) return;
    current.searchParams.delete("eventId");
    window.history.replaceState(
      window.history.state,
      "",
      `${current.pathname}${current.search}${current.hash}`,
    );
  }

  function closeForm() {
    if (submitting) return;
    setFormOpen(false);
    setEditing(null);
    setSubmitError(null);
  }

  function updateField<K extends keyof EventFormState>(
    field: K,
    value: EventFormState[K],
  ) {
    setForm((current) => ({ ...current, [field]: value }));
  }

  function updateTier(
    index: number,
    field: keyof EventTierDraft,
    value: string,
  ) {
    setForm((current) => ({
      ...current,
      tiers: current.tiers.map((tier, row) =>
        row === index ? { ...tier, [field]: value } : tier,
      ),
    }));
  }

  function addTier() {
    setForm((current) => ({
      ...current,
      tiers: [...current.tiers, emptyTierDraft()],
    }));
  }

  function removeTier(index: number) {
    setForm((current) => ({
      ...current,
      tiers: current.tiers.filter((_, row) => row !== index),
    }));
    setTouched(new Set());
  }

  function focusFirstInvalidField() {
    window.setTimeout(() => {
      document
        .querySelector<HTMLElement>('[role="dialog"] [aria-invalid="true"]')
        ?.focus();
    }, 0);
  }

  async function submit() {
    if (submitting) return;
    const creating = editing === null;
    setSubmitAttempted(true);
    setSubmitError(null);
    if (hasErrors(validation)) {
      focusFirstInvalidField();
      return;
    }

    setSubmitting(true);
    try {
      if (creating) {
        const payload = buildCreatePayload(form);
        if (!payload) {
          setSubmitError("admin.events.formCheckTiers");
          return;
        }
        let pending = pendingCreate;
        if (!pending) {
          try {
            pending = savePendingEventCreate(payload);
            setPendingCreate(pending);
          } catch {
            setSubmitError("admin.events.form.createDraftStorageError");
            return;
          }
        }
        const created = await apiClient.createEvent(pending.payload, pending.idempotencyKey);
        clearPendingEventCreate();
        setPendingCreate(null);
        setFormOpen(false);
        setEditing(null);
        setQuery("");
        setAppliedQuery("");
        setCategory("");
        setStatusFilter("");
        setPage(1);
        requestSeq.current += 1;
        setLoading(true);
        setRefreshNonce((nonce) => nonce + 1);
        notify("admin.events.created");
        // The list is date-ordered, so the new event may live on another page;
        // open its authoritative detail instead of implying it is on page 1.
        if (created?.id) setDetailId(created.id);
      } else {
        const payload = buildUpdatePayload(form, editing.date);
        if (!payload) {
          setSubmitError("admin.events.formCheckFields");
          return;
        }
        const dateChanged =
          payload.date !== undefined &&
          Date.parse(payload.date) !== Date.parse(editing.date);
        const salesPayload = buildSalesUpdatePayload(form);
        if (!salesPayload) {
          setSubmitError("admin.events.formCheckFields");
          return;
        }
        const updated = await apiClient.updateEvent(editing.id, {
          ...payload,
          date: dateChanged ? payload.date : undefined,
          expected_date_version: dateChanged ? editing.date_version : undefined,
        });
        setEditing(updated);
        await apiClient.updateEventSales(editing.id, salesPayload);
        setFormOpen(false);
        setEditing(null);
        requestSeq.current += 1;
        setLoading(true);
        setRefreshNonce((nonce) => nonce + 1);
        notify("admin.events.updated");
      }
    } catch (err) {
      setSubmitError(errorMessage(err));
    } finally {
      setSubmitting(false);
    }
  }

  function goToPage(nextPage: number) {
    const target = pageWindow(nextPage, PAGE_SIZE, total);
    if (target.totalPages === 0) return;
    requestSeq.current += 1;
    setLoading(true);
    setStatusFilter("");
    setPage(target.page);
  }

  function clearFilters() {
    const serverQueryChanges = Boolean(appliedQuery || category) || page !== 1;
    if (serverQueryChanges) {
      requestSeq.current += 1;
      setLoading(true);
    }
    setQuery("");
    setAppliedQuery("");
    setCategory("");
    setStatusFilter("");
    setPage(1);
  }

  return (
    <div className="flex min-h-[calc(100dvh_-_3.5rem)] flex-col lg:min-h-screen">
      <header className="sticky top-14 z-10 flex h-14 items-center justify-between border-b border-border bg-card px-3 sm:px-6 lg:top-0">
        <div className="flex items-center gap-1.5 text-sm text-muted">
          <span>{t("admin.nav.operations")}</span>
          <ChevronRight size={12} />
          <strong className="font-semibold text-foreground">{t("admin.events.title")}</strong>
        </div>
        <div className="flex items-center gap-3">
          <span className="rounded-md border border-border bg-tag-bg px-2 py-0.5 font-mono text-[11px] text-muted">
            {t("admin.events.adminOnly")}
          </span>
          <div
            className="grid h-[30px] w-[30px] place-items-center rounded-full border border-border bg-tag-bg text-xs font-semibold"
            title={t("admin.nav.console")}
          >
            OA
          </div>
        </div>
      </header>

      <main className="flex min-w-0 w-full max-w-[1280px] flex-col gap-4 p-3 sm:p-6">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h1 className="text-[22px] font-bold tracking-tight">{t("admin.events.title")}</h1>
            <p className="mt-0.5 max-w-[620px] text-[13px] text-muted">
              {t("admin.events.description")}
            </p>
          </div>
          <button
            type="button"
            onClick={openCreate}
            className="flex h-11 items-center gap-2 rounded-full bg-primary px-[18px] text-[13px] font-semibold text-graphite transition-colors hover:bg-primary-hover"
          >
            <Plus size={15} />
            {t("admin.events.newEvent")}
          </button>
        </div>

        <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
          <StatCard
            icon={<CalendarDays size={14} />}
            label={isFiltered ? t("admin.events.matchingEvents") : t("admin.events.totalEvents")}
            value={statsLoading || statsUnavailable ? "—" : number(total)}
            hint={
              statsLoading
                ? t("admin.events.loadingEvents")
                : statsUnavailable
                  ? t("admin.events.apiUnavailable")
                  : isFiltered
                    ? t("admin.events.serverCountCurrent")
                    : t("admin.events.authoritativeCount")
            }
          />
          <StatCard
            icon={<Layers size={14} />}
            label={t("admin.events.pageRange")}
            value={
              statsLoading || statsUnavailable || total === 0
                ? "—"
                : `${pager.start}–${pager.end}`
            }
            hint={
              statsLoading
                ? t("admin.events.loadingEvents")
                : statsUnavailable
                  ? t("admin.events.apiUnavailable")
                  : total === 0
                    ? t("admin.events.noMatchingEvents")
                    : t("admin.events.pageSummary", { page: number(pager.page), pages: number(pager.totalPages), count: number(PAGE_SIZE) })
            }
          />
          <StatCard
            icon={<Ticket size={14} />}
            label={t("admin.events.tierAvailability")}
            value={
              statsLoading || statsUnavailable
                ? "—"
                : number(inventory.availableSeats)
            }
            hint={
              statsLoading
                ? t("admin.events.loadingEvents")
                : statsUnavailable
                  ? t("admin.events.apiUnavailable")
                  : inventory.tiers === 1
                    ? t("admin.events.pageInventorySummaryOne")
                    : t("admin.events.pageInventorySummaryMany", { count: number(inventory.tiers) })
            }
          />
        </div>

        <div className="flex flex-wrap items-center gap-2.5">
          <div className="flex h-11 min-w-[200px] max-w-[340px] flex-1 items-center gap-2 rounded-lg border border-control-border bg-card px-3">
            <Search size={14} className="shrink-0 text-muted" />
            <input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder={t("admin.events.searchPlaceholder")}
              aria-label={t("admin.events.searchLabel")}
              className="w-full min-w-0 border-none bg-transparent text-[13px] text-foreground"
            />
            {query && (
              <button
                type="button"
                onClick={() => {
                  const fetchWillChange = Boolean(appliedQuery) || page !== 1;
                  if (fetchWillChange) {
                    requestSeq.current += 1;
                    setLoading(true);
                  }
                  setQuery("");
                  setAppliedQuery("");
                  setStatusFilter("");
                  setPage(1);
                }}
                className="grid h-11 w-11 shrink-0 place-items-center rounded text-muted hover:text-foreground"
                aria-label={t("admin.events.clearSearch")}
              >
                <X size={13} />
              </button>
            )}
          </div>

          <select
            value={category}
            onChange={(event) => {
              requestSeq.current += 1;
              setLoading(true);
              setCategory(event.target.value);
              setStatusFilter("");
              setPage(1);
            }}
            aria-label={t("admin.events.filterCategory")}
            className="h-11 rounded-lg border border-control-border bg-card px-3 text-[13px] text-foreground focus:border-primary"
          >
            <option value="">{t("admin.events.allCategories")}</option>
            {categoryOptions.map((item) => (
              <option key={item.category} value={item.category}>
                {item.label}
              </option>
            ))}
          </select>

          {statuses.length > 1 && (
            <select
              value={statusFilter}
              onChange={(event) => setStatusFilter(event.target.value)}
              aria-label={t("admin.events.filterPageStatus")}
              className="h-11 rounded-lg border border-control-border bg-card px-3 text-[13px] text-foreground focus:border-primary"
            >
              <option value="">{t("admin.events.allStatusesPage")}</option>
              {statuses.map((status) => (
                <option key={status} value={status}>
                  {status}
                </option>
              ))}
            </select>
          )}

          <span
            role="status"
            aria-live="polite"
            className="flex items-center gap-1.5 text-xs text-muted"
          >
            {loading && <Loader2 size={12} className="animate-spin" />}
            {loading
              ? t("admin.events.loadingEvents")
              : loadError
                ? t("admin.events.eventsUnavailable")
                : searchPending
                  ? t("admin.events.searching")
                  : total === 0
                    ? t("admin.events.noMatchingEventsStatus")
                    : t("admin.events.showingRangeOf", { start: number(pager.start), end: number(pager.end), total: number(total) })}
            {!loadError && statusFilter
              ? t("admin.events.onThisPage", { count: number(visible.length) })
              : ""}
          </span>

          {isFiltered || statusFilter ? (
            <button
              type="button"
              onClick={clearFilters}
              className="min-h-11 px-2 text-xs font-semibold text-muted underline underline-offset-2 hover:text-foreground"
            >
              {t("admin.events.clearFilters")}
            </button>
          ) : null}
        </div>

        {statusFilter ? (
          <p className="text-[11px] text-muted">
            {t("admin.events.pageStatusScope", { count: number(events.length) })}
          </p>
        ) : (
          <p className="text-[11px] text-muted">
            {t("admin.events.dateOrderScope")}
          </p>
        )}

        <div className="overflow-hidden rounded-xl border border-border bg-card">
          {loadError && (
            <div className="flex flex-wrap items-start justify-between gap-2 border-b border-danger/30 bg-danger-bg px-4 py-3 text-[13px] text-danger-ink">
              <span className="flex min-w-0 flex-1 items-start gap-2 break-words">
                <AlertCircle size={14} className="mt-0.5 shrink-0" />
                {t(loadError)}
              </span>
              <button
                type="button"
                onClick={() => {
                  requestSeq.current += 1;
                  setLoading(true);
                  setRefreshNonce((nonce) => nonce + 1);
                }}
                className="min-h-11 shrink-0 px-3 font-semibold underline underline-offset-2"
              >
                {t("admin.events.retry")}
              </button>
            </div>
          )}

          <div
            role="region"
            tabIndex={0}
            aria-label={t("admin.events.eventTable")}
            className="max-w-full overflow-x-auto"
          >
            <table
              className="w-full border-collapse"
              aria-busy={loading}
              aria-label={t("admin.events.title")}
            >
              <thead>
                <tr className="bg-background">
                  {[
                    t("admin.events.event"),
                    t("admin.events.category"),
                    t("admin.events.venue"),
                    t("admin.events.date"),
                    t("admin.events.tiers"),
                    t("admin.events.status"),
                  ].map((heading) => (
                    <th
                      key={heading}
                      className="whitespace-nowrap border-b border-border px-4 py-3 text-left text-[11px] font-semibold uppercase tracking-wider text-muted"
                    >
                      {heading}
                    </th>
                  ))}
                  <th className="border-b border-border px-4 py-3 text-right text-[11px] font-semibold uppercase tracking-wider text-muted">
                    {t("admin.events.actions")}
                  </th>
                </tr>
              </thead>
              <tbody className={loading && events.length > 0 ? "opacity-80" : ""}>
                {!loading &&
                  visible.map((event) => (
                    <tr
                      key={event.id}
                      className="transition-colors hover:bg-tag-bg/60"
                    >
                      <td className="border-b border-border px-4 py-3.5">
                        <button
                          type="button"
                          onClick={() => setDetailId(event.id)}
                          className="inline-flex min-h-11 items-center text-left font-semibold text-[13px] hover:underline"
                        >
                          {event.title}
                        </button>
                        <div className="mt-0.5 font-mono text-[11px] text-muted">
                          {event.id.slice(0, 8)}
                        </div>
                      </td>
                      <td className="border-b border-border px-4 py-3.5 text-[13px] capitalize">
                        {event.category || "—"}
                      </td>
                      <td className="border-b border-border px-4 py-3.5 text-[13px]">
                        <div>{event.venue}</div>
                        <div className="text-xs text-muted">
                          {event.location}
                        </div>
                      </td>
                      <td className="whitespace-nowrap border-b border-border px-4 py-3.5 text-xs text-muted">
                        {date(event.date, { year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" })}
                      </td>
                      <td className="whitespace-nowrap border-b border-border px-4 py-3.5 text-xs text-muted">
                        {(event.tiers ?? []).length === 0
                          ? t("admin.events.form.noTiers")
                          : (event.tiers ?? []).length === 1
                            ? t("admin.events.tierSummaryOne", {
                                price: usdCents(Math.min(...event.tiers.map((tier) => tier.price_cents))),
                              })
                            : t("admin.events.tierSummaryMany", {
                                count: number(event.tiers.length),
                                price: usdCents(Math.min(...event.tiers.map((tier) => tier.price_cents))),
                              })}
                      </td>
                      <td className="border-b border-border px-4 py-3.5">
                        <span className="inline-flex items-center gap-1.5 rounded-full bg-tag-bg px-2.5 py-[3px] text-xs font-semibold capitalize text-muted">
                          <span className="h-1.5 w-1.5 rounded-full bg-current" />
                          {event.status || "—"}
                        </span>
                      </td>
                      <td className="whitespace-nowrap border-b border-border px-4 py-3.5 text-right">
                        <button
                          type="button"
                          onClick={() => setDetailId(event.id)}
                          className="inline-flex h-11 items-center gap-1.5 rounded-lg px-2 text-[13px] text-muted transition-colors hover:bg-tag-bg hover:text-foreground"
                        >
                          <Eye size={13} />
                          {t("admin.events.view")}
                        </button>
                        <button
                          type="button"
                          onClick={() => openEdit(event)}
                          className="ml-1 inline-flex h-11 items-center gap-1.5 rounded-lg px-2 text-[13px] text-muted transition-colors hover:bg-tag-bg hover:text-foreground"
                        >
                          <Pencil size={13} />
                          {t("admin.events.edit")}
                        </button>
                        {isAdminEventOperationsId(event.id) ? (
                          <Link
                            href={`/admin/events/${encodeURIComponent(event.id)}/operations`}
                            aria-label={t("admin.events.viewOperationsFor", { title: event.title })}
                            className="ml-1 inline-flex h-11 items-center gap-1.5 rounded-lg px-2 text-[13px] text-muted transition-colors hover:bg-tag-bg hover:text-foreground"
                          >
                            <Activity size={13} aria-hidden="true" />
                            {t("admin.events.operations")}
                          </Link>
                        ) : null}
                      </td>
                    </tr>
                  ))}

              </tbody>
            </table>
          </div>

          {loading && events.length === 0 && (
            <div
              data-admin-table-state
              role="status"
              aria-live="polite"
              className="px-4 py-12 text-center text-[13px] text-muted"
            >
              <span className="inline-flex items-center gap-2">
                <Loader2 size={14} className="animate-spin" />
                {t("admin.events.loadingEvents")}
              </span>
            </div>
          )}

          {!loading && events.length > 0 && visible.length === 0 && (
            <div
              data-admin-table-state
              className="px-4 py-12 text-center text-[13px] text-muted"
            >
              {t("admin.events.noEventsWithStatus", { status: statusFilter })}
            </div>
          )}

          {!loading && events.length === 0 && (
            <div
              data-admin-table-state
              className="px-4 py-12 text-center text-[13px] text-muted"
            >
              {loadError ? (
                t("admin.events.couldNotLoad")
              ) : isFiltered ? (
                <span>
                  {t("admin.events.noFilterMatches")} {" "}
                  <button
                    type="button"
                    onClick={clearFilters}
                    className="min-h-11 px-3 font-semibold underline underline-offset-2"
                  >
                    {t("admin.events.clearFilters")}
                  </button>
                </span>
              ) : (
                t("admin.events.noEventsYet")
              )}
            </div>
          )}

          <div className="flex flex-wrap items-center justify-between gap-3 border-t border-border px-4 py-3">
            <span className="text-xs text-muted">
              {loading
                ? t("admin.events.loading")
                : loadError
                  ? t("admin.events.eventsUnavailable")
                  : total === 0
                    ? t("admin.events.noEvents")
                    : t("admin.events.showingRange", { start: number(pager.start), end: number(pager.end), total: number(total) })}
            </span>
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => goToPage(pager.page - 1)}
                disabled={!pager.hasPrev || loading}
                className="inline-flex h-11 items-center gap-1 rounded-full border border-border px-3 text-xs font-medium text-muted transition-colors hover:bg-tag-bg hover:text-foreground disabled:opacity-40"
              >
                <ChevronLeft size={13} />
                {t("admin.events.previous")}
              </button>
              <span className="text-xs text-muted">
                {loading
                  ? "…"
                  : loadError
                    ? t("admin.events.unavailable")
                    : pager.totalPages === 0
                      ? t("admin.events.noPages")
                      : t("admin.events.pageSummary", { page: number(pager.page), pages: number(pager.totalPages), count: number(PAGE_SIZE) })}
              </span>
              <button
                type="button"
                onClick={() => goToPage(pager.page + 1)}
                disabled={!pager.hasNext || loading}
                className="inline-flex h-11 items-center gap-1 rounded-full border border-border px-3 text-xs font-medium text-muted transition-colors hover:bg-tag-bg hover:text-foreground disabled:opacity-40"
              >
                {t("admin.events.next")}
                <ChevronRight size={13} />
              </button>
            </div>
          </div>
        </div>
      </main>

      {detailId && (
        <EventDetailDrawer
          key={detailId}
          eventId={detailId}
          onClose={closeDetail}
          onEdit={openEdit}
        />
      )}

      {formOpen && (
        <EventFormDrawer
          editing={editing}
          form={form}
          errors={displayedErrors}
          submitting={submitting}
          createRetryLocked={Boolean(pendingCreate) && !editing}
          submitError={submitError}
          onField={updateField}
          onFieldBlur={markTouched}
          onTier={updateTier}
          onTierBlur={(index, field) => markTouched(`tier-${index}-${field}`)}
          onAddTier={addTier}
          onRemoveTier={removeTier}
          onClose={closeForm}
          onSubmit={submit}
        />
      )}

      {toast && (
        <div
          role="status"
          aria-live="polite"
          className="fixed bottom-6 left-1/2 z-[60] flex -translate-x-1/2 items-center gap-2.5 rounded-lg border border-border bg-tag-bg px-5 py-2.5 text-[13px] text-foreground shadow-lg"
        >
          <CheckCircle2 size={14} className="text-success" />
          {t(toast)}
        </div>
      )}
    </div>
  );
}

function StatCard({
  icon,
  label,
  value,
  hint,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  hint: string;
}) {
  return (
    <div className="rounded-xl border border-border bg-card px-4 py-3">
      <div className="flex items-center gap-1.5 text-[11px] font-semibold uppercase tracking-wider text-muted">
        {icon}
        {label}
      </div>
      <div className="mt-1 font-mono text-[20px] font-semibold tabular-nums">
        {value}
      </div>
      <div className="mt-0.5 text-[11px] text-muted">{hint}</div>
    </div>
  );
}
