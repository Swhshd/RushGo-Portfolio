"use client";

import { Suspense, useEffect, useRef, useState, type FormEvent } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import {
  AlertCircle,
  ArrowLeft,
  Loader2,
  RefreshCw,
  Send,
} from "lucide-react";
import { apiClient } from "@/lib/api/client";
import {
  classifyAdminOutboxReadError,
  formatPendingAge,
  isOutboxResponseForSource,
  type AdminOutboxReadErrorKind,
} from "@/lib/admin-outbox-view";
import { useAuth } from "@/lib/auth-context";
import { withRequestDeadline } from "@/lib/request-deadline";
import type {
  ApiAdminOutboxFilterEventType,
  ApiAdminOutboxEventsQuery,
  ApiAdminOutboxRecord,
  ApiAdminOutboxRecordedStatus,
  ApiAdminOutboxSource,
  ApiNotificationOutboxEventType,
} from "@/lib/api/types";
import { useI18n, type TranslationKey } from "@/lib/i18n";

const PAGE_SIZE = 20;
const MAX_RANGE_MS = 7 * 24 * 60 * 60 * 1000;
const BOOKING_EVENT_TYPES: ApiAdminOutboxFilterEventType[] = [
  "BookingConfirmed",
  "BookingCancelled",
  "BookingFailed",
  "Other",
];
const NOTIFICATION_EVENT_TYPES: ApiNotificationOutboxEventType[] = [
  "booking_confirmation",
  "booking_failure",
  "booking_cancellation",
  "welcome_email",
];
const BOOKING_STATUS_OPTIONS: ApiAdminOutboxRecordedStatus[] = ["pending", "published"];
const NOTIFICATION_STATUS_OPTIONS: ApiAdminOutboxRecordedStatus[] = [
  "pending",
  "delivering",
  "sent",
  "retryable",
  "unknown",
  "attention",
];

type StatusFilter = "all" | ApiAdminOutboxRecordedStatus;
type FilterDraft = {
  source: ApiAdminOutboxSource;
  status: StatusFilter;
  eventType: ApiAdminOutboxFilterEventType | "";
  from: string;
  to: string;
};
type AppliedFilter = {
  source: ApiAdminOutboxSource;
  status: StatusFilter;
  eventType: ApiAdminOutboxFilterEventType | "";
  from: string;
  to: string;
};
type SourceFilterState = {
  draft: FilterDraft;
  applied: AppliedFilter;
};
type OutboxPageSnapshot = {
  ownerKey: string;
  source: ApiAdminOutboxSource;
  records: ApiAdminOutboxRecord[];
  nextCursor: string;
  hasMore: boolean;
  observedAt: string | null;
  observedAtByRecord: Record<string, string>;
  attemptedAt: string | null;
  hasSuccessfulRead: boolean;
  loading: boolean;
  refreshing: boolean;
  loadErrorKind: AdminOutboxReadErrorKind | null;
  refreshErrorKind: AdminOutboxReadErrorKind | null;
  loadMoreErrorKind: AdminOutboxReadErrorKind | null;
  loadingMore: boolean;
};

function loadingOutboxPage(ownerKey: string, source: ApiAdminOutboxSource): OutboxPageSnapshot {
  return {
    ownerKey,
    source,
    records: [],
    nextCursor: "",
    hasMore: false,
    observedAt: null,
    observedAtByRecord: {},
    attemptedAt: null,
    hasSuccessfulRead: false,
    loading: true,
    refreshing: false,
    loadErrorKind: null,
    refreshErrorKind: null,
    loadMoreErrorKind: null,
    loadingMore: false,
  };
}

function dateTimeLocalValue(date: Date): string {
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 16);
}

function defaultFilterDraft(source: ApiAdminOutboxSource = "booking"): FilterDraft {
  const to = new Date();
  const from = new Date(to.getTime() - 24 * 60 * 60 * 1000);
  return {
    source,
    status: "all",
    eventType: "",
    from: dateTimeLocalValue(from),
    to: dateTimeLocalValue(to),
  };
}

function toQuery(filter: AppliedFilter, cursor?: string): ApiAdminOutboxEventsQuery {
  return {
    source: filter.source,
    status: filter.status,
    ...(filter.eventType ? { event_type: filter.eventType } : {}),
    from: new Date(filter.from).toISOString(),
    to: new Date(filter.to).toISOString(),
    limit: PAGE_SIZE,
    ...(cursor ? { cursor } : {}),
  };
}

function validateFilter(filter: FilterDraft): TranslationKey | null {
  if (!filter.from || !filter.to) return "admin.outbox.validation.bothTimes";
  const from = new Date(filter.from).getTime();
  const to = new Date(filter.to).getTime();
  if (!Number.isFinite(from) || !Number.isFinite(to)) return "admin.outbox.validation.timeRange";
  if (from >= to) return "admin.outbox.validation.order";
  if (to - from > MAX_RANGE_MS) return "admin.outbox.validation.maximumRange";
  return null;
}

function isAbortError(error: unknown): boolean {
  return typeof error === "object" && error !== null && "name" in error &&
    (error as { name?: string }).name === "AbortError";
}

function httpStatus(error: unknown): number | undefined {
  return typeof error === "object" && error !== null && "status" in error
    ? (error as { status?: number }).status
    : undefined;
}

function errorName(error: unknown): string | undefined {
  return typeof error === "object" && error !== null && "name" in error
    ? (error as { name?: string }).name
    : undefined;
}

function isTransientLoadMoreError(error: unknown): boolean {
  const status = httpStatus(error);
  return classifyAdminOutboxReadError(error) === "unavailable" &&
    (status === 500 || status === 503 || errorName(error) === "TimeoutError");
}

function formatDate(value: string, locale: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(locale, {
    dateStyle: "medium",
    timeStyle: "medium",
    timeZone: "UTC",
  }).format(date) + " UTC";
}

function outboxStatusKey(
  source: ApiAdminOutboxSource,
  status: ApiAdminOutboxRecordedStatus,
): TranslationKey {
  if (source === "notification" && status === "sent") return "admin.outbox.status.adapterAccepted";
  switch (status) {
    case "pending": return "admin.outbox.status.pending";
    case "published": return "admin.outbox.status.published";
    case "delivering": return "admin.outbox.status.delivering";
    case "sent": return "admin.outbox.status.sent";
    case "retryable": return "admin.outbox.status.retryable";
    case "unknown": return "admin.outbox.status.unknown";
    case "attention": return "admin.outbox.status.attention";
  }
}

function outboxErrorKey(
  kind: AdminOutboxReadErrorKind,
  context: "first" | "more" = "first",
): TranslationKey {
  if (kind === "capacity") return context === "more" ? "admin.outbox.error.capacityMore" : "admin.outbox.error.capacityFirst";
  if (kind === "session") return "admin.outbox.error.session";
  if (kind === "forbidden") return "admin.outbox.error.forbidden";
  if (kind === "filter") return "admin.outbox.error.filter";
  return context === "more" ? "admin.outbox.error.unavailableMore" : "admin.outbox.error.unavailableFirst";
}

function OutboxRow({ record, observedAt }: { record: ApiAdminOutboxRecord; observedAt: string | null }) {
  const { t, locale } = useI18n();
  const pendingAge = record.recorded_status === "pending" && observedAt
    ? formatPendingAge(record.created_at, observedAt, locale)
    : null;
  return (
    <tr className="border-t border-border align-top">
      <td className="max-w-52 break-words px-3 py-3 text-sm font-semibold sm:px-4">
        {record.event_type}
      </td>
      <td className="whitespace-nowrap px-3 py-3 text-sm sm:px-4">
        {record.source_service === "notification" ? t("admin.outbox.source.notificationService") : t("admin.outbox.source.bookingService")}
      </td>
      <td className="px-3 py-3 sm:px-4">
        <span className="inline-flex max-w-full break-words rounded-full border border-border px-2.5 py-1 text-xs font-semibold">
        {t(outboxStatusKey(record.source_service, record.recorded_status))}
        </span>
        <p className="mt-1 max-w-44 whitespace-normal break-words text-[11px] text-muted">{t("admin.outbox.ownerRecordedStatus")}</p>
        {record.source_service === "notification" && record.recorded_status === "sent" && (
          <p className="mt-1 max-w-48 whitespace-normal break-words text-[11px] text-muted">{t("admin.outbox.adapterAcceptanceOnly")}</p>
        )}
        {record.recorded_status === "pending" && (
          <p className="mt-1 max-w-48 whitespace-normal break-words text-[11px] text-muted">
            {pendingAge
              ? t("admin.outbox.pendingAge", { age: pendingAge })
              : t("admin.outbox.pendingAgeUnavailable")}
          </p>
        )}
      </td>
      <td className="min-w-48 px-3 py-3 text-sm text-muted sm:px-4">
        <time dateTime={record.created_at}>{formatDate(record.created_at, locale)}</time>
      </td>
      <td className="min-w-52 px-3 py-3 text-xs text-muted sm:px-4">
        {record.source_service === "notification"
          ? t("admin.outbox.sourceOutcomeNotification")
          : t("admin.outbox.sourceOutcomeBooking")}
      </td>
      <td className="max-w-64 break-all px-3 py-3 font-mono text-[11px] text-muted sm:px-4">
        {record.event_id}
      </td>
    </tr>
  );
}

function AdminOutboxContent() {
  const { t, locale, number } = useI18n();
  const router = useRouter();
  const searchParams = useSearchParams();
  const requestedSource: ApiAdminOutboxSource = searchParams.get("source") === "notification" ? "notification" : "booking";
  const { user } = useAuth();
  const [draft, setDraft] = useState<FilterDraft>(() => defaultFilterDraft(requestedSource));
  const [filter, setFilter] = useState<AppliedFilter | null>(null);
  const [filterError, setFilterError] = useState<TranslationKey | null>(null);
  const [pageSnapshot, setPageSnapshot] = useState<OutboxPageSnapshot | null>(null);
  const [queryRevision, setQueryRevision] = useState(0);
  const [reloadNonce, setReloadNonce] = useState(0);
  const sourceFilterStates = useRef<Partial<Record<ApiAdminOutboxSource, SourceFilterState>>>({});
  const requestSequence = useRef(0);
  const loadMoreController = useRef<AbortController | null>(null);
  const didInitialize = useRef(false);
  const pendingSource = useRef<ApiAdminOutboxSource | null>(null);
  const lastRequestedSource = useRef(requestedSource);
  const sessionVersion = apiClient.getSessionVersion();
  const userId = user?.id ?? null;
  const userRole = user?.role ?? null;
  const authorized = Boolean(userId && userRole === "admin");

  const filterKey = filter ? JSON.stringify([filter.source, filter.status, filter.eventType, filter.from, filter.to]) : "";
  const ownerKey = JSON.stringify([sessionVersion, userId, userRole, filterKey, queryRevision]);
  const activeSource = pendingSource.current ?? requestedSource;
  const displayDraft: FilterDraft = draft.source === activeSource
    ? draft
    : sourceFilterStates.current[activeSource]?.draft ?? defaultFilterDraft(activeSource);
  const currentPage: OutboxPageSnapshot = filter && !authorized
    ? {
      ...loadingOutboxPage(ownerKey, activeSource),
      loading: false,
      loadErrorKind: "session",
    }
    : filter?.source === activeSource && pageSnapshot?.ownerKey === ownerKey && pageSnapshot.source === activeSource
      ? pageSnapshot
      : loadingOutboxPage(ownerKey, activeSource);
  const {
    records,
    hasMore,
    observedAt,
    observedAtByRecord,
    attemptedAt,
    hasSuccessfulRead,
    loading,
    refreshing,
    loadErrorKind,
    refreshErrorKind,
    loadMoreErrorKind,
    loadingMore,
  } = currentPage;

  useEffect(() => {
    if (didInitialize.current) return;
    didInitialize.current = true;
    const defaults = defaultFilterDraft(requestedSource);
    sourceFilterStates.current[requestedSource] = { draft: defaults, applied: defaults };
    setDraft(defaults);
    setFilter(defaults);
  }, [requestedSource]);

  useEffect(() => {
    const urlSourceChanged = lastRequestedSource.current !== requestedSource;
    lastRequestedSource.current = requestedSource;
    if (pendingSource.current) {
      if (pendingSource.current === requestedSource || urlSourceChanged) {
        pendingSource.current = null;
      } else {
        return;
      }
    }
    if (!didInitialize.current || !filter || filter.source === requestedSource) return;
    let nextSourceState = sourceFilterStates.current[requestedSource];
    if (!nextSourceState) {
      const defaults = defaultFilterDraft(requestedSource);
      nextSourceState = { draft: defaults, applied: defaults };
      sourceFilterStates.current[requestedSource] = nextSourceState;
    }
    requestSequence.current++;
    loadMoreController.current?.abort();
    loadMoreController.current = null;
    setFilterError(null);
    setDraft(nextSourceState.draft);
    setFilter(nextSourceState.applied);
    setQueryRevision((value) => value + 1);
    setReloadNonce((value) => value + 1);
  }, [filter, requestedSource]);

  useEffect(() => {
    if (!filter || filter.source !== requestedSource || filter.source !== activeSource || !authorized) return;
    const sequence = ++requestSequence.current;
    const expectedVersion = sessionVersion;
    const expectedOwnerKey = ownerKey;
    const controller = new AbortController();
    let current = true;
    const isCurrent = () => current &&
      sequence === requestSequence.current &&
      expectedVersion === apiClient.getSessionVersion();

    loadMoreController.current?.abort();
    loadMoreController.current = null;

    void withRequestDeadline(
      apiClient.listAdminOutboxEvents(toQuery(filter), controller.signal),
      controller,
    )
      .then((result) => {
        if (!isCurrent()) return;
        if (!isOutboxResponseForSource(result, filter.source, PAGE_SIZE)) throw new Error("invalid Outbox response");
        const next: OutboxPageSnapshot = {
          ...loadingOutboxPage(expectedOwnerKey, filter.source),
          records: result.records,
          nextCursor: result.next_cursor,
          hasMore: result.has_more,
          observedAt: result.observed_at,
          observedAtByRecord: Object.fromEntries(result.records.map((record) => [record.event_id, result.observed_at])),
          attemptedAt: new Date().toISOString(),
          hasSuccessfulRead: true,
          loading: false,
          refreshing: false,
          refreshErrorKind: null,
        };
        setPageSnapshot((previous) => isCurrent() ? next : previous);
      })
      .catch((error: unknown) => {
        if (!isCurrent()) return;
        const errorKind = classifyAdminOutboxReadError(error);
        setPageSnapshot((previous) => {
          if (!isCurrent()) return previous;
          const base = previous?.ownerKey === expectedOwnerKey && previous.source === filter.source
            ? previous
            : loadingOutboxPage(expectedOwnerKey, filter.source);
          const attemptedAt = new Date().toISOString();
          if (base.hasSuccessfulRead && errorKind !== "session" && errorKind !== "forbidden") {
            return {
              ...base,
              attemptedAt,
              loading: false,
              refreshing: false,
              loadErrorKind: null,
              refreshErrorKind: errorKind,
              loadingMore: false,
              loadMoreErrorKind: null,
            };
          }
          return {
            ...loadingOutboxPage(expectedOwnerKey, filter.source),
            attemptedAt,
            loading: false,
            loadErrorKind: errorKind,
          };
        });
      });

    return () => {
      current = false;
      controller.abort();
      loadMoreController.current?.abort();
      loadMoreController.current = null;
    };
  }, [activeSource, authorized, filter, filterKey, ownerKey, reloadNonce, requestedSource, sessionVersion]);

  function clearPageForNewQuery() {
    requestSequence.current++;
    loadMoreController.current?.abort();
    loadMoreController.current = null;
  }

  function updateDraft(patch: Partial<FilterDraft>) {
    const nextDraft: FilterDraft = { ...displayDraft, ...patch, source: activeSource };
    const previous = sourceFilterStates.current[activeSource];
    sourceFilterStates.current[activeSource] = {
      draft: nextDraft,
      applied: filter?.source === activeSource ? filter : previous?.applied ?? nextDraft,
    };
    setDraft(nextDraft);
  }

  function changeSource(source: ApiAdminOutboxSource) {
    if (source === activeSource) return;
    const currentSourceState = sourceFilterStates.current[activeSource];
    sourceFilterStates.current[activeSource] = {
      draft: displayDraft,
      applied: filter?.source === activeSource ? filter : currentSourceState?.applied ?? displayDraft,
    };
    let nextSourceState = sourceFilterStates.current[source];
    if (!nextSourceState) {
      const defaults = defaultFilterDraft(source);
      nextSourceState = { draft: defaults, applied: defaults };
      sourceFilterStates.current[source] = nextSourceState;
    }
    pendingSource.current = source;
    setFilterError(null);
    clearPageForNewQuery();
    setDraft(nextSourceState.draft);
    setFilter(nextSourceState.applied);
    setQueryRevision((value) => value + 1);
    setReloadNonce((value) => value + 1);
    router.push(source === "notification" ? "/admin/outbox?source=notification" : "/admin/outbox");
  }

  function applyFilters(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const error = validateFilter(displayDraft);
    if (error) {
      setFilterError(error);
      return;
    }
    setFilterError(null);
    clearPageForNewQuery();
    const appliedFilter: AppliedFilter = { ...displayDraft };
    sourceFilterStates.current[activeSource] = { draft: displayDraft, applied: appliedFilter };
    setFilter(appliedFilter);
    setQueryRevision((value) => value + 1);
    setReloadNonce((value) => value + 1);
  }

  function refresh() {
    if (!filter || loading || refreshing) return;
    clearPageForNewQuery();
    const expectedOwnerKey = ownerKey;
    const expectedSource = filter.source;
    setPageSnapshot((previous) => {
      if (previous?.ownerKey !== expectedOwnerKey || previous.source !== expectedSource) return previous;
      return {
        ...previous,
        loading: !previous.hasSuccessfulRead,
        refreshing: previous.hasSuccessfulRead,
        loadErrorKind: null,
        refreshErrorKind: null,
        loadMoreErrorKind: null,
        loadingMore: false,
      };
    });
    setReloadNonce((value) => value + 1);
  }

  async function loadMore() {
    if (!filter || currentPage.source !== filter.source || !currentPage.hasMore || !currentPage.nextCursor || currentPage.loadingMore || currentPage.refreshing || !authorized) return;
    const sequence = requestSequence.current;
    const expectedVersion = sessionVersion;
    const expectedOwnerKey = ownerKey;
    const cursor = currentPage.nextCursor;
    const controller = new AbortController();
    loadMoreController.current?.abort();
    loadMoreController.current = controller;
    setPageSnapshot((previous) => previous?.ownerKey === expectedOwnerKey && previous.source === filter.source
      ? { ...previous, loadingMore: true, loadMoreErrorKind: null }
      : previous);

    const isCurrent = () =>
      sequence === requestSequence.current &&
      expectedVersion === apiClient.getSessionVersion();

    let cancelled = false;
    try {
      const result = await withRequestDeadline(
        apiClient.listAdminOutboxEvents(toQuery(filter, cursor), controller.signal),
        controller,
      );
      if (!isCurrent()) return;
      if (!isOutboxResponseForSource(result, filter.source, PAGE_SIZE)) throw new Error("invalid Outbox response");
      setPageSnapshot((previous) => {
        if (!isCurrent()) return previous;
        const base = previous?.ownerKey === expectedOwnerKey && previous.source === filter.source ? previous : currentPage;
        if (base.source !== filter.source) return previous;
        return {
          ...base,
          records: [...base.records, ...result.records],
          nextCursor: result.next_cursor,
          hasMore: result.has_more,
          observedAt: result.observed_at,
          observedAtByRecord: {
            ...base.observedAtByRecord,
            ...Object.fromEntries(result.records.map((record) => [record.event_id, result.observed_at])),
          },
        };
      });
    } catch (error) {
      if (isAbortError(error)) {
        cancelled = true;
      } else if (isCurrent()) {
        setPageSnapshot((previous) => {
          if (!isCurrent()) return previous;
          const base = previous?.ownerKey === expectedOwnerKey && previous.source === filter.source ? previous : currentPage;
          if (base.source !== filter.source) return previous;
          if (isTransientLoadMoreError(error)) {
            return {
              ...base,
              loadMoreErrorKind: classifyAdminOutboxReadError(error),
            };
          }
          return {
            ...loadingOutboxPage(expectedOwnerKey, filter.source),
            attemptedAt: new Date().toISOString(),
            loading: false,
            loadErrorKind: classifyAdminOutboxReadError(error),
          };
        });
      }
    } finally {
      if (!cancelled && isCurrent()) {
        setPageSnapshot((previous) => {
          if (!isCurrent()) return previous;
          const base = previous?.ownerKey === expectedOwnerKey && previous.source === filter.source ? previous : currentPage;
          if (base.source !== filter.source) return previous;
          return { ...base, loadingMore: false };
        });
      }
      if (loadMoreController.current === controller) loadMoreController.current = null;
    }
  }

  const recordCountLabel = records.length === 1
    ? t("admin.outbox.countOne")
    : t("admin.outbox.countMany", { count: number(records.length) });
  const sourceEventTypes = activeSource === "notification" ? NOTIFICATION_EVENT_TYPES : BOOKING_EVENT_TYPES;
  const sourceStatusOptions = activeSource === "notification" ? NOTIFICATION_STATUS_OPTIONS : BOOKING_STATUS_OPTIONS;
  const sourceName = t(activeSource === "notification" ? "admin.outbox.source.notification" : "admin.outbox.source.booking");
  const hasLoadError = loadErrorKind !== null;
  const loadErrorText = loadErrorKind ? t(outboxErrorKey(loadErrorKind), { source: sourceName }) : "";
  const loadMoreErrorText = loadMoreErrorKind ? t(outboxErrorKey(loadMoreErrorKind, "more"), { source: sourceName }) : "";

  return (
    <main className="min-w-0 flex-1 px-3 py-5 sm:px-6 sm:py-7 lg:px-9 lg:py-8">
      <div className="mx-auto w-full max-w-7xl space-y-6">
        <header className="flex min-w-0 flex-wrap items-start justify-between gap-4">
          <div className="min-w-0">
            <Link href="/admin" className="inline-flex min-h-11 items-center gap-2 text-xs font-semibold text-muted underline underline-offset-4 hover:text-foreground focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary">
              <ArrowLeft size={14} aria-hidden="true" /> {t("admin.outbox.backToOverview")}
            </Link>
            <p className="mt-3 text-xs font-semibold uppercase tracking-[0.16em] text-primary">{t("admin.outbox.readOnlySource")}</p>
            <h1 className="mt-1 break-words text-2xl font-bold tracking-tight sm:text-3xl">{t("admin.outbox.title")}</h1>
            <p className="mt-2 max-w-3xl break-words text-sm leading-relaxed text-muted">
              {activeSource === "notification"
                ? t("admin.outbox.notificationGuidance")
                : t("admin.outbox.bookingGuidance")}
            </p>
            <p className="mt-1 max-w-3xl break-words text-xs text-muted">
              {t("admin.outbox.resultsCanChange")}
            </p>
          </div>
          <button
            type="button"
            onClick={refresh}
            disabled={loading || refreshing || !filter}
            className="inline-flex min-h-11 shrink-0 items-center justify-center gap-2 rounded-full border border-border bg-card px-4 text-sm font-semibold text-foreground transition-colors hover:bg-tag-bg focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary disabled:cursor-wait disabled:opacity-60"
          >
            <RefreshCw size={15} className={loading || refreshing ? "animate-spin" : ""} aria-hidden="true" />
            {refreshing ? t("admin.common.refreshing") : t("admin.outbox.refresh")}
          </button>
        </header>

        <section className="min-w-0 rounded-xl border border-border bg-card p-4 sm:p-5" aria-labelledby="outbox-filters-heading">
          <div className="mb-4 flex items-start gap-3">
            <span className="grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-tag-bg text-primary">
              <Send size={17} aria-hidden="true" />
            </span>
            <div className="min-w-0">
              <h2 id="outbox-filters-heading" className="text-base font-bold">{t("admin.outbox.filtersHeading", { source: sourceName })}</h2>
              <p className="mt-1 break-words text-xs text-muted">{t("admin.outbox.timeRangeLimit")}</p>
              <p className="mt-1 break-words text-xs text-muted">{t("admin.outbox.applyDescription")}</p>
            </div>
          </div>
          <form onSubmit={applyFilters} className="grid min-w-0 grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-5">
            <div className="min-w-0">
              <label htmlFor="outbox-source" className="mb-1.5 block text-xs font-semibold text-muted">{t("admin.outbox.sourceLabel")}</label>
              <select
                id="outbox-source"
                aria-label={t("admin.outbox.sourceLabel")}
                value={activeSource}
                onChange={(event) => changeSource(event.target.value as ApiAdminOutboxSource)}
                className="h-11 w-full min-w-0 rounded-lg border border-control-border bg-background px-3 text-sm text-foreground focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
              >
                <option value="booking">{t("admin.outbox.source.bookingService")}</option>
                <option value="notification">{t("admin.outbox.source.notification")}</option>
              </select>
            </div>
            <div className="min-w-0">
              <label htmlFor="outbox-status" className="mb-1.5 block text-xs font-semibold text-muted">{t("admin.outbox.recordStatus")}</label>
              <select id="outbox-status" aria-label={t("admin.outbox.filterStatusAria")} value={displayDraft.status} onChange={(event) => updateDraft({ status: event.target.value as StatusFilter })} className="h-11 w-full min-w-0 rounded-lg border border-control-border bg-background px-3 text-sm text-foreground focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary">
                <option value="all">{t("admin.outbox.statusAll")}</option>
                {sourceStatusOptions.map((status) => (
                  <option key={status} value={status}>{t(outboxStatusKey(activeSource, status))}</option>
                ))}
              </select>
            </div>
            <div className="min-w-0">
              <label htmlFor="outbox-event-type" className="mb-1.5 block text-xs font-semibold text-muted">{t("admin.outbox.eventType")}</label>
              <select id="outbox-event-type" aria-label={t("admin.outbox.filterEventTypeAria")} value={displayDraft.eventType} onChange={(event) => updateDraft({ eventType: event.target.value as ApiAdminOutboxFilterEventType | "" })} className="h-11 w-full min-w-0 rounded-lg border border-control-border bg-background px-3 text-sm text-foreground focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary">
                <option value="">{t("admin.outbox.eventTypeAll")}</option>
                {sourceEventTypes.map((eventType) => <option key={eventType} value={eventType}>{eventType}</option>)}
              </select>
            </div>
            <div className="min-w-0">
              <label htmlFor="outbox-from" className="mb-1.5 block text-xs font-semibold text-muted">{t("admin.outbox.from")}</label>
              <input id="outbox-from" aria-label={t("admin.outbox.from")} type="datetime-local" required value={displayDraft.from} onChange={(event) => updateDraft({ from: event.target.value })} className="h-11 w-full min-w-0 rounded-lg border border-control-border bg-background px-2 text-sm text-foreground focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary" />
            </div>
            <div className="min-w-0">
              <label htmlFor="outbox-to" className="mb-1.5 block text-xs font-semibold text-muted">{t("admin.outbox.to")}</label>
              <input id="outbox-to" aria-label={t("admin.outbox.to")} type="datetime-local" required value={displayDraft.to} onChange={(event) => updateDraft({ to: event.target.value })} className="h-11 w-full min-w-0 rounded-lg border border-control-border bg-background px-2 text-sm text-foreground focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary" />
            </div>
            <div className="flex min-w-0 flex-wrap items-center gap-3 sm:col-span-2 xl:col-span-5">
              <button type="submit" disabled={!displayDraft.from || !displayDraft.to || loading || draft.source !== activeSource} className="min-h-11 rounded-full bg-primary px-5 text-sm font-semibold text-graphite transition-colors hover:bg-primary-hover focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary disabled:cursor-not-allowed disabled:opacity-50">
                {t("admin.outbox.applyFilters")}
              </button>
              {filterError && <p className="break-words text-sm text-danger-ink" role="alert">{t(filterError)}</p>}
            </div>
          </form>
        </section>

        <section className="min-w-0 space-y-3" aria-labelledby="outbox-results-heading">
          <div className="flex min-w-0 flex-wrap items-end justify-between gap-3">
            <div className="min-w-0">
              <p className="text-xs font-semibold uppercase tracking-wide text-muted">{sourceName}</p>
              <h2 id="outbox-results-heading" className="mt-1 break-words text-lg font-bold">{t("admin.outbox.records")}</h2>
              <p className="mt-1 break-words text-xs text-muted">
                {loading
                  ? t("admin.outbox.readingWindow")
                  : hasLoadError
                    ? loadErrorKind === "capacity" ? t("admin.overview.queryLimit") : t("admin.outbox.sourceUnavailable")
                    : recordCountLabel}
                {observedAt && !hasLoadError ? <> · {t("admin.outbox.observedAt")} <time dateTime={observedAt}>{formatDate(observedAt, locale)}</time></> : null}
              </p>
            </div>
            <p className="max-w-full break-words rounded-full border border-border bg-card px-3 py-1.5 text-xs text-muted">
              {activeSource === "notification" ? t("admin.outbox.sourceOutcomeNotification") : t("admin.outbox.sourceOutcomeBooking")}
            </p>
          </div>

          {refreshing && hasSuccessfulRead && (
            <p className="rounded-lg border border-border bg-card px-4 py-3 text-sm text-muted" role="status">
              {t("admin.outbox.refreshingStale", { source: sourceName })}
            </p>
          )}
          {refreshErrorKind && hasSuccessfulRead && (
            <div className="flex min-w-0 flex-wrap items-center justify-between gap-3 rounded-xl border border-danger/30 bg-danger/5 p-4" role="alert">
              <div className="min-w-0">
                <p className="break-words text-sm font-semibold text-danger-ink">
                  {t("admin.outbox.refreshFailedStale", { source: sourceName })}
                </p>
                <p className="mt-1 break-words text-xs text-muted">
                  {t(outboxErrorKey(refreshErrorKind), { source: sourceName })}
                </p>
                {attemptedAt && <p className="mt-1 break-words text-xs text-muted">{t("admin.outbox.lastAttempted")} <time dateTime={attemptedAt}>{formatDate(attemptedAt, locale)}</time></p>}
              </div>
              <button type="button" onClick={refresh} disabled={refreshing || loading} className="min-h-11 shrink-0 rounded-full border border-border px-4 text-sm font-semibold text-foreground hover:bg-tag-bg focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary disabled:cursor-wait disabled:opacity-50">
                {t("admin.outbox.retryRead")}
              </button>
            </div>
          )}
          {loading && (
            <div className="rounded-xl border border-border bg-card px-4 py-10 text-center text-sm text-muted" role="status">
              <Loader2 size={18} className="mx-auto mb-2 animate-spin" aria-hidden="true" />
              {t("admin.outbox.loadingRecords", { source: sourceName })}
            </div>
          )}
          {!loading && hasLoadError && (
            <div className="flex min-w-0 flex-wrap items-center justify-between gap-3 rounded-xl border border-danger/30 bg-danger/5 p-4" role="alert">
              <div className="flex min-w-0 items-start gap-2.5">
                <AlertCircle size={17} className="mt-0.5 shrink-0 text-danger-ink" aria-hidden="true" />
                <div className="min-w-0">
                  <p className="break-words text-sm text-danger-ink">{loadErrorText}</p>
                  {attemptedAt && <p className="mt-1 break-words text-xs text-muted">{t("admin.outbox.lastAttempted")} <time dateTime={attemptedAt}>{formatDate(attemptedAt, locale)}</time></p>}
                </div>
              </div>
              <button type="button" onClick={refresh} className="min-h-11 shrink-0 rounded-full border border-border px-4 text-sm font-semibold text-foreground hover:bg-tag-bg focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary">
                {t("admin.outbox.retryRead")}
              </button>
            </div>
          )}
          {!loading && !hasLoadError && records.length === 0 && (
            <div className="rounded-xl border border-border bg-card px-4 py-10 text-center">
              <p className="text-sm font-semibold">{t("admin.outbox.noRecords")}</p>
              <p className="mt-1 break-words text-xs text-muted">{t("admin.outbox.emptySource", { source: sourceName })}</p>
            </div>
          )}
          {!loading && !hasLoadError && records.length > 0 && (
            <>
              <div role="region" aria-label={t(activeSource === "notification" ? "admin.outbox.tableNotification" : "admin.outbox.tableBooking")} tabIndex={0} className="max-w-full overflow-x-auto rounded-xl border border-border bg-card focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary">
                <table className="w-full min-w-[860px] border-collapse text-left">
                  <thead>
                    <tr className="bg-background text-[11px] font-semibold uppercase tracking-wide text-muted">
                      <th scope="col" className="px-3 py-3 sm:px-4">{t("admin.outbox.column.eventType")}</th>
                      <th scope="col" className="px-3 py-3 sm:px-4">{t("admin.outbox.column.source")}</th>
                      <th scope="col" className="px-3 py-3 sm:px-4">{t("admin.outbox.column.recordedStatus")}</th>
                      <th scope="col" className="px-3 py-3 sm:px-4">{t("admin.outbox.column.createdAt")}</th>
                      <th scope="col" className="px-3 py-3 sm:px-4">{t("admin.outbox.column.consumerOutcome")}</th>
                      <th scope="col" className="px-3 py-3 sm:px-4">{t("admin.outbox.column.eventId")}</th>
                    </tr>
                  </thead>
                  <tbody>{records.map((record) => (
                    <OutboxRow
                      key={record.event_id}
                      record={record}
                      observedAt={observedAtByRecord[record.event_id] ?? observedAt}
                    />
                  ))}</tbody>
                </table>
              </div>
              {records.some((record) => record.recorded_status === "pending") && (
                <p className="break-words text-xs text-muted">{t("admin.outbox.ageOnly")}</p>
              )}
              {loadMoreErrorKind && (
                <div className="flex min-w-0 flex-wrap items-center justify-between gap-3 rounded-lg border border-danger/30 bg-danger/5 p-3" role="alert">
                  <p className="break-words text-sm text-danger-ink">{loadMoreErrorText}</p>
                  <button type="button" onClick={() => void loadMore()} disabled={loadingMore || refreshing} className="min-h-11 shrink-0 rounded-full border border-border px-4 text-sm font-semibold hover:bg-tag-bg disabled:opacity-50">
                    {t("admin.outbox.retryMore")}
                  </button>
                </div>
              )}
              {hasMore && !loadMoreErrorKind && (
                <div className="flex justify-center">
                  <button type="button" onClick={() => void loadMore()} disabled={loadingMore || refreshing} className="inline-flex min-h-11 items-center justify-center gap-2 rounded-full border border-border bg-card px-5 text-sm font-semibold hover:bg-tag-bg focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary disabled:cursor-wait disabled:opacity-60">
                    {loadingMore && <Loader2 size={15} className="animate-spin" aria-hidden="true" />}
                    {loadingMore ? t("admin.outbox.loadingMore") : t("admin.outbox.loadMore")}
                  </button>
                </div>
              )}
              {!hasMore && <p className="text-center text-xs text-muted">{t("admin.outbox.endOfRecords")}</p>}
            </>
          )}
        </section>
      </div>
    </main>
  );
}

export default function AdminOutboxPage() {
  return (
    <Suspense fallback={<OutboxLoadingFallback />}>
      <AdminOutboxContent />
    </Suspense>
  );
}

function OutboxLoadingFallback() {
  const { t } = useI18n();
  return <main className="min-w-0 flex-1 px-3 py-5 sm:px-6 sm:py-7 lg:px-9 lg:py-8">{t("admin.outbox.loadingSource")}</main>;
}
