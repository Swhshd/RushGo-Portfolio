"use client";

import { useEffect, useLayoutEffect, useRef, useState } from "react";
import Link from "next/link";
import {
  AlertCircle,
  ArrowUpRight,
  CalendarDays,
  Loader2,
  RefreshCw,
  Send,
  Timer,
} from "lucide-react";
import { apiClient } from "@/lib/api/client";
import {
  classifyAdminOutboxReadError,
  formatPendingAge,
  getOutboxOverviewFacts,
  isOutboxResponseForSource,
  type AdminOutboxReadErrorKind,
} from "@/lib/admin-outbox-view";
import { useAuth } from "@/lib/auth-context";
import { withRequestDeadline } from "@/lib/request-deadline";
import type {
  ApiAdminOutboxEventsResponse,
  ApiAdminOutboxRecord,
  ApiAdminOutboxRecordedStatus,
  ApiAdminOutboxSource,
  ApiListEventsResponse,
  ApiListSchedulersResponse,
} from "@/lib/api/types";
import { useI18n, type TranslationKey } from "@/lib/i18n";

type ReadState<T> = {
  status: "loading" | "ready" | "unavailable" | "capacity";
  value: T | null;
  timestamp: string | null;
  errorKind?: AdminOutboxReadErrorKind;
  refreshing?: boolean;
  refreshErrorKind?: AdminOutboxReadErrorKind;
};

const initialReadState = <T,>(): ReadState<T> => ({
  status: "loading",
  value: null,
  timestamp: null,
  refreshing: false,
});

function settleReadState<T>(previous: ReadState<T>, next: ReadState<T>): ReadState<T> {
  const failed = next.status === "unavailable" || next.status === "capacity";
  if (failed && previous.status === "ready" && previous.value !== null) {
    return {
      ...previous,
      refreshing: false,
      refreshErrorKind: next.errorKind ?? "unavailable",
    };
  }
  return { ...next, refreshing: false, refreshErrorKind: undefined };
}

type OverviewSnapshot = {
  ownerKey: string;
  pendingSources: number;
  events: ReadState<number>;
  jobs: ReadState<number>;
  bookingOutbox: ReadState<ApiAdminOutboxRecord[]>;
  notificationOutbox: ReadState<ApiAdminOutboxRecord[]>;
};

function loadingOverviewSnapshot(ownerKey: string): OverviewSnapshot {
  return {
    ownerKey,
    pendingSources: 4,
    events: initialReadState<number>(),
    jobs: initialReadState<number>(),
    bookingOutbox: initialReadState<ApiAdminOutboxRecord[]>(),
    notificationOutbox: initialReadState<ApiAdminOutboxRecord[]>(),
  };
}

function formatReadTime(value: string, locale: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(locale, {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "UTC",
  }).format(date) + " UTC";
}

function overviewSourceName(source: ApiAdminOutboxSource, t: (key: TranslationKey, params?: Record<string, string | number>) => string): string {
  return source === "notification"
    ? t("admin.overview.notificationIntents")
    : t("admin.overview.bookingOutbox");
}

function overviewStatusLabel(
  source: ApiAdminOutboxSource,
  status: ApiAdminOutboxRecordedStatus,
  t: (key: TranslationKey) => string,
): string {
  if (source === "notification" && status === "sent") return t("admin.outbox.status.adapterAccepted");
  switch (status) {
    case "pending": return t("admin.outbox.status.pending");
    case "published": return t("admin.outbox.status.published");
    case "delivering": return t("admin.outbox.status.delivering");
    case "retryable": return t("admin.outbox.status.retryable");
    case "unknown": return t("admin.outbox.status.unknown");
    case "attention": return t("admin.outbox.status.attention");
    case "sent": return t("admin.outbox.status.sent");
  }
}

function outboxReadErrorKey(kind: AdminOutboxReadErrorKind | undefined): TranslationKey {
  switch (kind) {
    case "capacity": return "admin.overview.sourceCapacityFailure";
    case "session": return "admin.overview.outboxSessionError";
    case "forbidden": return "admin.overview.outboxForbiddenError";
    case "filter": return "admin.overview.outboxFilterError";
    default: return "admin.overview.outboxUnavailableError";
  }
}

function hasValidEventTotal(result: ApiListEventsResponse): boolean {
  return typeof result.total === "number" &&
    Number.isFinite(result.total) &&
    result.total >= 0;
}

function hasValidSchedulers(result: ApiListSchedulersResponse): boolean {
  return Array.isArray(result.schedulers);
}

function hasValidOutbox(result: ApiAdminOutboxEventsResponse, source: ApiAdminOutboxSource): boolean {
  return isOutboxResponseForSource(result, source, 5);
}

function ReadTime({
  value,
  unavailable = false,
  label = "admin.overview.readAt",
}: {
  value: string | null;
  unavailable?: boolean;
  label?: TranslationKey;
}) {
  const { t, locale } = useI18n();
  if (!value) return <p className="mt-1 text-xs text-muted">{t("admin.overview.notReadYet")}</p>;
  return (
    <p className="mt-1 break-words text-xs text-muted">
      {unavailable ? t("admin.overview.lastChecked") : t(label)}{" "}
      <time dateTime={value}>{formatReadTime(value, locale)}</time>
    </p>
  );
}

function MetricCard({
  title,
  source,
  state,
  icon: Icon,
  valueLabel,
}: {
  title: string;
  source: string;
  state: ReadState<number>;
  icon: typeof CalendarDays;
  valueLabel: string;
}) {
  const { t, number } = useI18n();
  const value = state.status === "loading"
    ? t("admin.overview.loading")
    : state.status === "ready" && state.value !== null
      ? number(state.value)
      : t("admin.overview.unavailable");

  return (
    <section className="min-w-0 rounded-xl border border-border bg-card p-4 sm:p-5">
      <div className="flex min-w-0 items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-xs font-semibold uppercase tracking-wide text-muted">{title}</p>
          <p className="mt-3 break-words text-3xl font-bold tabular-nums text-foreground" aria-live="polite">
            {value}
          </p>
          <p className="mt-1 break-words text-xs text-muted">{valueLabel}</p>
        </div>
        <span className="grid h-10 w-10 shrink-0 place-items-center rounded-lg bg-tag-bg text-primary">
          <Icon size={18} aria-hidden="true" />
        </span>
      </div>
      {(state.status === "unavailable" || state.status === "capacity") && (
        <p className="mt-3 flex items-start gap-2 text-xs text-danger-ink" role="status">
          <AlertCircle size={14} className="mt-0.5 shrink-0" aria-hidden="true" />
          <span>{t("admin.overview.readFailed", { title })}</span>
        </p>
      )}
      {state.status === "loading" && (
        <p className="mt-3 flex items-center gap-2 text-xs text-muted" role="status">
          <Loader2 size={14} className="animate-spin" aria-hidden="true" />
          {t("admin.overview.readingSource", { source })}
        </p>
      )}
      {state.refreshing && state.value !== null && (
        <p className="mt-3 break-words text-xs text-muted" role="status">
          {t("admin.overview.refreshingStale", { source })}
        </p>
      )}
      {state.refreshErrorKind && state.value !== null && (
        <p className="mt-3 break-words text-xs text-danger-ink" role="alert">
          {t("admin.overview.refreshFailedStale", { source })}
        </p>
      )}
      <ReadTime value={state.timestamp} unavailable={state.status === "unavailable"} />
      <p className="mt-1 break-words text-[11px] text-muted">{t("admin.overview.source", { source })}</p>
    </section>
  );
}

function EntryLink({ href, title, description, icon: Icon }: {
  href: string;
  title: string;
  description: string;
  icon: typeof CalendarDays;
}) {
  return (
    <Link
      href={href}
      className="group flex min-h-16 min-w-0 items-center gap-3 rounded-xl border border-border bg-card px-4 py-3 transition-colors hover:border-control-border hover:bg-tag-bg focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
    >
      <span className="grid h-10 w-10 shrink-0 place-items-center rounded-lg bg-tag-bg text-primary">
        <Icon size={18} aria-hidden="true" />
      </span>
      <span className="min-w-0 flex-1">
        <span className="block break-words text-sm font-semibold text-foreground">{title}</span>
        <span className="mt-0.5 block break-words text-xs text-muted">{description}</span>
      </span>
      <ArrowUpRight size={16} className="shrink-0 text-muted transition-colors group-hover:text-foreground" aria-hidden="true" />
    </Link>
  );
}

function OutboxSnapshot({ state, source }: { state: ReadState<ApiAdminOutboxRecord[]>; source: ApiAdminOutboxSource }) {
  const { t, locale } = useI18n();
  const hasPending = state.status === "ready" && Boolean(state.value?.some((record) => record.recorded_status === "pending"));
  const sourceName = overviewSourceName(source, t);
  const headingId = source === "notification" ? "recent-notification-intents-heading" : "recent-booking-outbox-heading";
  return (
    <section className="min-w-0 rounded-xl border border-border bg-card p-4 sm:p-5" aria-labelledby={headingId}>
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-xs font-semibold uppercase tracking-wide text-muted">{source === "notification" ? t("admin.overview.notificationService") : t("admin.overview.bookingService")}</p>
          <h2 id={headingId} className="mt-1 break-words text-lg font-bold">{t("admin.overview.recentRecords", { source: sourceName })}</h2>
          <p className="mt-1 break-words text-xs text-muted">
            {source === "notification"
              ? t("admin.overview.notificationManagedIntent")
              : t("admin.overview.bookingRecordedState")}
          </p>
        </div>
        <Link
          href={source === "notification" ? "/admin/outbox?source=notification" : "/admin/outbox"}
          aria-label={t("admin.overview.viewAllSource", { source: sourceName })}
          className="inline-flex min-h-11 shrink-0 items-center gap-1.5 rounded-full border border-border px-3 text-xs font-semibold text-foreground transition-colors hover:bg-tag-bg focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
        >
          {t("admin.overview.viewAll")} <ArrowUpRight size={14} aria-hidden="true" />
        </Link>
      </div>

      {state.status === "loading" && (
        <p className="mt-5 flex items-center gap-2 text-sm text-muted" role="status">
          <Loader2 size={16} className="animate-spin" aria-hidden="true" />
          {t("admin.overview.readingRecords", { source: sourceName })}
        </p>
      )}
      {state.status === "unavailable" && (
        <div className="mt-5 rounded-lg border border-danger/30 bg-danger/5 p-3 text-sm" role="status">
          <p className="font-semibold text-danger-ink">{t("admin.overview.sourceUnavailable", { source: sourceName })}</p>
          <p className="mt-1 break-words text-xs text-muted">{t(outboxReadErrorKey(state.errorKind), { source: sourceName })}</p>
        </div>
      )}
      {state.status === "capacity" && (
        <div className="mt-5 rounded-lg border border-danger/30 bg-danger/5 p-3 text-sm" role="status">
          <p className="font-semibold text-danger-ink">{t("admin.overview.capacityHeading")}</p>
          <p className="mt-1 break-words text-xs text-muted">{t(outboxReadErrorKey(state.errorKind), { source: sourceName })}</p>
        </div>
      )}
      {state.status === "ready" && state.value?.length === 0 && (
        <p className="mt-5 rounded-lg border border-border bg-background px-3 py-5 text-center text-sm text-muted">
          {t("admin.overview.noRecentRecords", { source: sourceName })}
        </p>
      )}
      {state.status === "ready" && state.value && state.value.length > 0 && (
        <ul className="mt-4 divide-y divide-border rounded-lg border border-border bg-background">
          {state.value.map((record) => (
            <li key={record.event_id} className="min-w-0 px-3 py-3 sm:px-4">
              <div className="flex min-w-0 flex-wrap items-start justify-between gap-x-3 gap-y-2">
                <div className="min-w-0">
                  <p className="break-words text-sm font-semibold">{record.event_type}</p>
                  <p className="mt-1 break-words text-xs text-muted">
                    <time dateTime={record.created_at}>{formatReadTime(record.created_at, locale)}</time>
                  </p>
                </div>
                <span className="max-w-full break-words rounded-full border border-border px-2.5 py-1 text-[11px] font-semibold">
                  {overviewStatusLabel(source, record.recorded_status, t)}
                </span>
              </div>
              {record.recorded_status === "pending" && state.timestamp && (
                <p className="mt-2 break-words text-xs text-muted">
                  {(() => {
                    const age = formatPendingAge(record.created_at, state.timestamp, locale);
                    return age
                      ? t("admin.overview.pendingAge", { age })
                      : t("admin.overview.pendingAgeUnavailable");
                  })()}
                </p>
              )}
              <p className="mt-2 break-words text-xs text-muted">
                {source === "notification" ? t("admin.overview.notificationDeliveryUnverified") : t("admin.overview.consumerOutcomeUnverified")}
              </p>
            </li>
          ))}
        </ul>
      )}
      <ReadTime
        value={state.timestamp}
        unavailable={state.status === "unavailable" || state.status === "capacity"}
        label="admin.overview.observedAt"
      />
      {hasPending && <p className="mt-1 break-words text-[11px] text-muted">{t("admin.overview.ageOnly")}</p>}
      <p className="mt-1 break-words text-[11px] text-muted">{t("admin.overview.recentRecordsNotTotal", { source: sourceName })}</p>
    </section>
  );
}

function OutboxSourceCoverage({ state, source }: { state: ReadState<ApiAdminOutboxRecord[]>; source: ApiAdminOutboxSource }) {
  const { t, locale, number } = useI18n();
  const facts = state.status === "ready" && state.value && state.timestamp
    ? getOutboxOverviewFacts(state.value, state.timestamp)
    : null;
  const recordCount = facts?.recordCount ?? 0;
  const sourceName = overviewSourceName(source, t);
  const statusText = state.status === "loading"
    ? t("admin.overview.loading")
    : facts
      ? number(recordCount)
      : state.status === "capacity"
        ? t("admin.overview.queryLimit")
        : t("admin.overview.unavailable");

  return (
    <section className="min-w-0 rounded-xl border border-border bg-card p-4 sm:p-5" aria-label={t("admin.overview.outboxSourceRegion", { source: sourceName })}>
      <div className="flex min-w-0 items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-xs font-semibold uppercase tracking-wide text-muted">{t("admin.overview.outboxSource")}</p>
          <p className="mt-1 break-words text-sm font-semibold text-foreground">{source === "notification" ? t("admin.overview.notificationIntents") : t("admin.overview.booking")}</p>
          <p className="mt-3 break-words text-3xl font-bold tabular-nums text-foreground" aria-live="polite">{statusText}</p>
          <p className="mt-1 break-words text-xs text-muted">
            {facts
              ? recordCount === 1
                ? t("admin.overview.recordCount24hOne")
                : t("admin.overview.recordCount24hMany", { count: number(recordCount) })
              : state.status === "loading"
                ? t("admin.overview.readingRecent", { source: sourceName })
                : t("admin.overview.recentCountUnavailable", { source: sourceName })}
          </p>
        </div>
        <span className="grid h-10 w-10 shrink-0 place-items-center rounded-lg bg-tag-bg text-primary">
          <Send size={18} aria-hidden="true" />
        </span>
      </div>
      {state.refreshing && state.value !== null && (
        <p className="mt-3 break-words text-xs text-muted" role="status">
          {t("admin.overview.refreshingStale", { source: sourceName })}
        </p>
      )}
      {state.refreshErrorKind && state.value !== null && (
        <div className="mt-3 space-y-1 break-words text-xs text-danger-ink" role="alert">
          <p>{t("admin.overview.refreshFailedStale", { source: sourceName })}</p>
          <p>{t(outboxReadErrorKey(state.refreshErrorKind), { source: sourceName })}</p>
        </div>
      )}
      {facts && (
        <>
          <p className="mt-2 break-words text-[11px] text-muted">{t("admin.overview.sampleLimit", { count: number(facts.sampleLimit) })}</p>
        </>
      )}
      <p className="mt-2 break-words text-xs text-muted">{source === "notification" ? t("admin.overview.outboxStatesNotification") : t("admin.overview.outboxStatesBooking")}</p>
      <p className="mt-2 break-words text-xs text-muted">
        {t("admin.overview.newestRecord")} {facts
          ? facts.newestRecordAt
            ? <time dateTime={facts.newestRecordAt}>{formatReadTime(facts.newestRecordAt, locale)}</time>
            : t("admin.overview.noneInRange")
          : state.status === "loading"
            ? t("admin.overview.notReadYet")
            : t("admin.overview.unavailableInRead")}
      </p>
      {state.status === "capacity" && (
        <p className="mt-3 break-words text-xs text-danger-ink" role="status">
          {t(outboxReadErrorKey(state.errorKind), { source: sourceName })}
        </p>
      )}
      {state.status === "unavailable" && (
        <p className="mt-3 break-words text-xs text-danger-ink" role="status">
          {t(outboxReadErrorKey(state.errorKind), { source: sourceName })}
        </p>
      )}
      <ReadTime
        value={facts?.observedAt ?? state.timestamp}
        unavailable={state.status === "unavailable" || state.status === "capacity"}
        label="admin.overview.observedAt"
      />
      <p className="mt-1 break-words text-[11px] text-muted">
        {source === "notification"
          ? t("admin.overview.adapterAcceptanceUnverified")
          : t("admin.overview.publishedMarkerUnverified")}
      </p>
      <Link
        href={source === "notification" ? "/admin/outbox?source=notification" : "/admin/outbox"}
        aria-label={t("admin.overview.reviewSourceRecords", { source: sourceName })}
        className="mt-3 inline-flex min-h-11 items-center gap-1.5 text-sm font-semibold text-primary underline underline-offset-4 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
      >
        {t("admin.overview.reviewRecords")} <ArrowUpRight size={14} aria-hidden="true" />
      </Link>
    </section>
  );
}

export default function AdminHomePage() {
  const { t } = useI18n();
  const { user } = useAuth();
  const [snapshot, setSnapshot] = useState<OverviewSnapshot | null>(null);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const requestGeneration = useRef(0);
  const sessionVersion = apiClient.getSessionVersion();
  const userId = user?.id ?? null;
  const userRole = user?.role ?? null;
  const authorized = Boolean(userId && userRole === "admin");
  const ownerKey = JSON.stringify([sessionVersion, userId, userRole]);
  const currentOwnerKey = useRef(ownerKey);
  useLayoutEffect(() => {
    currentOwnerKey.current = ownerKey;
  }, [ownerKey]);
  const currentSnapshot: OverviewSnapshot = !authorized
    ? {
      ownerKey,
      pendingSources: 0,
      events: { status: "unavailable", value: null, timestamp: null },
      jobs: { status: "unavailable", value: null, timestamp: null },
      bookingOutbox: { status: "unavailable", value: null, timestamp: null },
      notificationOutbox: { status: "unavailable", value: null, timestamp: null },
    }
    : snapshot?.ownerKey === ownerKey
      ? snapshot
      : loadingOverviewSnapshot(ownerKey);
  const { events, jobs, bookingOutbox, notificationOutbox } = currentSnapshot;
  const refreshing = authorized && currentSnapshot.pendingSources > 0;
  const hasReadFailure = [events, jobs, bookingOutbox, notificationOutbox].some((state) =>
    state.status === "unavailable" || state.status === "capacity" || Boolean(state.refreshErrorKind),
  );

  function refreshSources() {
    if (!authorized || refreshing) return;
    requestGeneration.current += 1;
    const markRefreshing = <T,>(state: ReadState<T>): ReadState<T> =>
      state.status === "ready" && state.value !== null
        ? { ...state, refreshing: true, refreshErrorKind: undefined }
        : initialReadState<T>();

    setSnapshot((previous) => previous?.ownerKey === ownerKey
      ? {
        ...previous,
        pendingSources: 4,
        events: markRefreshing(previous.events),
        jobs: markRefreshing(previous.jobs),
        bookingOutbox: markRefreshing(previous.bookingOutbox),
        notificationOutbox: markRefreshing(previous.notificationOutbox),
      }
      : previous);
    setRefreshNonce((value) => value + 1);
  }

  useEffect(() => {
    if (!authorized) return;

    const expectedVersion = sessionVersion;
    const expectedGeneration = requestGeneration.current;
    const expectedOwnerKey = ownerKey;
    const eventsController = new AbortController();
    const jobsController = new AbortController();
    const bookingOutboxController = new AbortController();
    const notificationOutboxController = new AbortController();
    const controllers = [eventsController, jobsController, bookingOutboxController, notificationOutboxController];
    let current = true;
    const isCurrent = () =>
      current &&
      expectedVersion === apiClient.getSessionVersion() &&
      expectedGeneration === requestGeneration.current &&
      expectedOwnerKey === currentOwnerKey.current;
    const checkedAt = () => new Date().toISOString();
    const clearAfterAuthorizationFailure = (errorKind: AdminOutboxReadErrorKind) => {
      if (!isCurrent()) return;
      current = false;
      controllers.forEach((controller) => controller.abort());
      const deniedRead = <T,>(): ReadState<T> => ({
        status: "unavailable",
        value: null,
        timestamp: checkedAt(),
        errorKind,
        refreshing: false,
      });
      setSnapshot({
        ownerKey,
        pendingSources: 0,
        events: deniedRead<number>(),
        jobs: deniedRead<number>(),
        bookingOutbox: deniedRead<ApiAdminOutboxRecord[]>(),
        notificationOutbox: deniedRead<ApiAdminOutboxRecord[]>(),
      });
    };
    const completeSource = (
      source: "events" | "jobs" | "bookingOutbox" | "notificationOutbox",
      value: ReadState<number> | ReadState<ApiAdminOutboxRecord[]>,
    ) => {
      if (!isCurrent()) return;
      setSnapshot((previous) => {
        if (!isCurrent()) return previous;
        const base = previous?.ownerKey === ownerKey
          ? previous
          : loadingOverviewSnapshot(ownerKey);
        const pendingSources = Math.max(0, base.pendingSources - 1);
        if (source === "events") {
          return { ...base, events: settleReadState(base.events, value as ReadState<number>), pendingSources };
        }
        if (source === "jobs") {
          return { ...base, jobs: settleReadState(base.jobs, value as ReadState<number>), pendingSources };
        }
        if (source === "bookingOutbox") {
          return { ...base, bookingOutbox: settleReadState(base.bookingOutbox, value as ReadState<ApiAdminOutboxRecord[]>), pendingSources };
        }
        return { ...base, notificationOutbox: settleReadState(base.notificationOutbox, value as ReadState<ApiAdminOutboxRecord[]>), pendingSources };
      });
    };
    const failSource = (
      source: "events" | "jobs" | "bookingOutbox" | "notificationOutbox",
      error: unknown,
    ) => {
      if (!isCurrent()) return;
      const errorKind = classifyAdminOutboxReadError(error);
      if (errorKind === "session" || errorKind === "forbidden") {
        clearAfterAuthorizationFailure(errorKind);
        return;
      }
      completeSource(source, {
        status: errorKind === "capacity" ? "capacity" : "unavailable",
        value: null,
        timestamp: checkedAt(),
        errorKind,
      });
    };

    const now = new Date();
    const range = {
      from: new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString(),
      to: now.toISOString(),
    };

    void withRequestDeadline(
      apiClient.listEventsPage({ page: 1, page_size: 1 }, eventsController.signal),
      eventsController,
    )
      .then((result) => {
        if (!isCurrent()) return;
        if (!hasValidEventTotal(result)) throw new Error("invalid events response");
        completeSource("events", { status: "ready", value: result.total, timestamp: checkedAt() });
      })
      .catch((error: unknown) => {
        failSource("events", error);
      });

    void withRequestDeadline(
      apiClient.listSchedulerJobs(jobsController.signal),
      jobsController,
    )
      .then((result) => {
        if (!isCurrent()) return;
        if (!hasValidSchedulers(result)) throw new Error("invalid scheduler response");
        completeSource("jobs", { status: "ready", value: result.schedulers.length, timestamp: checkedAt() });
      })
      .catch((error: unknown) => {
        failSource("jobs", error);
      });

    const requestOutboxSource = (
      source: ApiAdminOutboxSource,
      controller: AbortController,
    ) => {
      const snapshotSource = source === "booking" ? "bookingOutbox" : "notificationOutbox";
      void withRequestDeadline(
        apiClient.listAdminOutboxEvents({
          source,
          status: "all",
          from: range.from,
          to: range.to,
          limit: 5,
        }, controller.signal),
        controller,
      )
        .then((result) => {
          if (!isCurrent()) return;
          if (!hasValidOutbox(result, source)) {
            throw new Error(`invalid ${source} response`);
          }
          completeSource(snapshotSource, { status: "ready", value: result.records, timestamp: result.observed_at });
        })
        .catch((error: unknown) => {
          failSource(snapshotSource, error);
        });
    };

    requestOutboxSource("booking", bookingOutboxController);
    requestOutboxSource("notification", notificationOutboxController);

    return () => {
      current = false;
      controllers.forEach((controller) => controller.abort());
    };
  }, [authorized, ownerKey, refreshNonce, sessionVersion]);

  return (
    <main className="min-w-0 flex-1 px-3 py-5 sm:px-6 sm:py-7 lg:px-9 lg:py-8">
      <div className="mx-auto w-full max-w-7xl space-y-7">
        <header className="flex min-w-0 flex-wrap items-start justify-between gap-4">
          <div className="min-w-0">
            <p className="text-xs font-semibold uppercase tracking-[0.16em] text-primary">{t("admin.overview.adminConsole")}</p>
            <h1 className="mt-1 break-words text-2xl font-bold tracking-tight sm:text-3xl">{t("admin.overview.title")}</h1>
            <p className="mt-2 max-w-2xl break-words text-sm leading-relaxed text-muted">
              {t("admin.overview.description")}
            </p>
          </div>
          <button
            type="button"
            disabled={!authorized || refreshing}
            onClick={refreshSources}
            className="inline-flex min-h-11 shrink-0 items-center justify-center gap-2 rounded-full border border-border bg-card px-4 text-sm font-semibold text-foreground transition-colors hover:bg-tag-bg focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary disabled:cursor-wait disabled:opacity-60"
          >
            <RefreshCw size={15} className={refreshing ? "animate-spin" : ""} aria-hidden="true" />
            {refreshing
              ? t("admin.overview.refreshing")
              : hasReadFailure
                ? t("admin.common.retry")
                : t("admin.overview.refresh")}
          </button>
        </header>

        <section aria-label={t("admin.overview.systemOverview")} className="grid min-w-0 grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-3">
          <MetricCard
            title={t("admin.events.title")}
            source={t("admin.overview.eventsSource")}
            state={events}
            icon={CalendarDays}
            valueLabel={t("admin.overview.eventsValueLabel")}
          />
          <MetricCard
            title={t("admin.nav.cronJobs")}
            source={t("admin.overview.jobsSource")}
            state={jobs}
            icon={Timer}
            valueLabel={t("admin.overview.jobsValueLabel")}
          />
          <OutboxSourceCoverage state={bookingOutbox} source="booking" />
          <OutboxSourceCoverage state={notificationOutbox} source="notification" />
        </section>

        <section aria-labelledby="admin-entry-heading" className="space-y-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-muted">{t("admin.overview.manage")}</p>
            <h2 id="admin-entry-heading" className="mt-1 text-lg font-bold">{t("admin.overview.sections")}</h2>
          </div>
          <div className="grid min-w-0 grid-cols-1 gap-3 md:grid-cols-3">
            <EntryLink
              href="/admin/events"
              title={t("admin.events.title")}
              description={t("admin.overview.eventsEntryDescription")}
              icon={CalendarDays}
            />
            <EntryLink
              href="/admin/ops"
              title={t("admin.overview.operationsEntry")}
              description={t("admin.overview.jobsEntryDescription")}
              icon={Timer}
            />
            <EntryLink
              href="/admin/outbox"
              title={t("admin.nav.outboxEvents")}
              description={t("admin.overview.outboxEntryDescription")}
              icon={Send}
            />
          </div>
        </section>

        <section aria-labelledby="outbox-source-coverage-heading" className="space-y-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-muted">{t("admin.overview.outboxSources")}</p>
            <h2 id="outbox-source-coverage-heading" className="mt-1 text-lg font-bold">{t("admin.overview.coverageRecentRecords")}</h2>
          </div>
          <div className="grid min-w-0 grid-cols-1 gap-3 xl:grid-cols-2">
            <OutboxSnapshot state={bookingOutbox} source="booking" />
            <OutboxSnapshot state={notificationOutbox} source="notification" />
          </div>
        </section>
      </div>
    </main>
  );
}
