"use client";

/**
 * Admin > Operations > Cron Jobs
 *
 * Manages scheduler_configs rows in scheduler-service. Creating a config
 * calls CreateSchedulerConfig (gateway → scheduler gRPC); toggling or
 * editing a job calls UpdateSchedulerById (gateway → scheduler gRPC)
 * with enable state, cron expression, and timeout; the update bumps
 * the config version and propagates via the scheduler-config Kafka
 * topic so the worker re-registers the job with the new schedule.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { FormEvent } from "react";
import {
  Check,
  ChevronRight,
  Search,
  X,
} from "lucide-react";
import { describeCron, nextTick, parseCron } from "@/lib/cron";
import { apiClient } from "@/lib/api/client";
import { ApiSchedulerJob } from "@/lib/api/types";
import type { ApiCreateSchedulerJobParams } from "@/lib/api/types";
import { useI18n, type TranslationKey } from "@/lib/i18n";

interface CronJobConfig {
  id: string;
  name: string;
  cronExpression: string;
  timeout: number; // seconds
  version: number;
  isEnabled: boolean;
  valid: boolean;
  validationError: string;
  updatedAt: string;
}

/** ApiSchedulerJob (snake_case, RFC3339) → row model */
function toCronJobConfig(j: ApiSchedulerJob): CronJobConfig {
  return {
    id: j.id,
    name: j.name,
    cronExpression: j.cron_expression,
    timeout: j.timeout,
    version: j.version,
    isEnabled: j.is_enabled,
    valid: j.valid,
    validationError: j.validation_error,
    updatedAt: j.updated_at,
  };
}

const CRON_PRESETS: Array<{ label: string; value: string; labelKey?: TranslationKey }> = [
  { label: "30s", value: "*/30 * * * * *" },
  { label: "1m", value: "0 * * * * *" },
  { label: "5m", value: "0 */5 * * * *" },
  { label: "15m", value: "0 */15 * * * *" },
  { label: "hourly", labelKey: "admin.cron.drawer.preset.hourly", value: "0 0 * * * *" },
  { label: "daily 3am", labelKey: "admin.cron.drawer.preset.daily3am", value: "0 0 3 * * *" },
];

type Filter = "all" | "enabled" | "disabled";
type CronNotice = { key: TranslationKey; params?: Record<string, string | number> };

function schedulerErrorKey(error: unknown): TranslationKey {
  const status = typeof error === "object" && error !== null && "status" in error
    ? (error as { status?: number }).status
    : undefined;
  if (status === 401) return "admin.cron.error.session";
  if (status === 403) return "admin.cron.error.forbidden";
  if (status === 409) return "admin.cron.error.conflict";
  if (status === 400) return "admin.cron.error.invalid";
  return "admin.cron.error.unavailable";
}

function cronFieldKey(field: string): TranslationKey | null {
  switch (field) {
    case "sec": return "admin.cron.field.seconds";
    case "min": return "admin.cron.field.minutes";
    case "hour": return "admin.cron.field.hours";
    case "dom": return "admin.cron.field.dayOfMonth";
    case "month": return "admin.cron.field.month";
    case "dow": return "admin.cron.field.dayOfWeek";
    default: return null;
  }
}

function localizedCronError(error: string, t: (key: TranslationKey, params?: Record<string, string | number>) => string, number: (value: number) => string): string {
  const count = error.match(/^Expected 6 fields, got (\d+)$/);
  if (count) return t("admin.cron.validation.fieldCount", { count: number(Number(count[1])) });
  const inField = error.match(/ in (sec|min|hour|dom|month|dow) field$/);
  const fieldKey = inField ? cronFieldKey(inField[1]) : null;
  const field = fieldKey ? t(fieldKey) : "";
  const detail = inField ? error.slice(0, inField.index) : error;
  if (fieldKey && detail === "empty field") return t("admin.cron.validation.emptyField", { field });
  const invalidValue = detail.match(/^“([^”]+)” is not a valid cron value$/);
  if (fieldKey && invalidValue) return t("admin.cron.validation.invalidValue", { value: invalidValue[1], field });
  const range = detail.match(/^“([^”]+)” is out of range \((\d+)–(\d+)\)$/);
  if (fieldKey && range) return t("admin.cron.validation.range", { value: range[1], min: number(Number(range[2])), max: number(Number(range[3])), field });
  const step = detail.match(/^step “\/(\d+)” must be ≥ 1$/);
  if (fieldKey && step) return t("admin.cron.validation.step", { step: number(Number(step[1])), field });
  return t("admin.cron.validation.invalid");
}

function localizedCronDescription(expression: string, t: (key: TranslationKey, params?: Record<string, string | number>) => string, number: (value: number) => string): string | null {
  const description = describeCron(expression);
  if (!description) return null;
  if (description === "Every minute") return t("admin.cron.description.everyMinute");
  if (description === "Every second") return t("admin.cron.description.everySecond");
  if (description === "Custom schedule") return t("admin.cron.description.custom");
  let match = description.match(/^Every minute at second :(\d{2})$/);
  if (match) return t("admin.cron.description.everyMinuteAtSecond", { second: number(Number(match[1])) });
  match = description.match(/^Every (\d+) seconds$/);
  if (match) return t("admin.cron.description.everySeconds", { count: number(Number(match[1])) });
  match = description.match(/^At minute :(\d{2}) of every hour$/);
  if (match) return t("admin.cron.description.atMinuteEveryHour", { minute: number(Number(match[1])) });
  match = description.match(/^Daily at (\d{2}):(\d{2})$/);
  if (match) return t("admin.cron.description.dailyAt", { hour: number(Number(match[1])).padStart(2, "0"), minute: number(Number(match[2])).padStart(2, "0") });
  match = description.match(/^Every (\d+) minutes$/);
  if (match) return t("admin.cron.description.everyMinutes", { count: number(Number(match[1])) });
  return t("admin.cron.description.custom");
}

export default function AdminOpsCronPage() {
  const { t, number, date } = useI18n();
  const [jobs, setJobs] = useState<CronJobConfig[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<TranslationKey | null>(null);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [filter, setFilter] = useState<Filter>("all");
  const [query, setQuery] = useState("");
  const [editing, setEditing] = useState<CronJobConfig | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [toast, setToast] = useState<CronNotice | null>(null);
  const requestSeq = useRef(0);
  const mounted = useRef(false);
  const toastTimer = useRef<number | null>(null);
  const toastGeneration = useRef(0);

  const fetchJobs = useCallback(() => apiClient.listSchedulerJobs(), []);

  useEffect(() => {
    mounted.current = true;
    const requestId = ++requestSeq.current;
    let active = true;

    void fetchJobs()
      .then((res) => {
        if (!active || requestId !== requestSeq.current) return;
        setJobs(res.schedulers.map(toCronJobConfig));
        setLoadError(null);
        setLoading(false);
      })
      .catch((err) => {
        if (!active || requestId !== requestSeq.current) return;
        setLoadError(schedulerErrorKey(err));
        setLoading(false);
      });

    return () => {
      active = false;
      mounted.current = false;
      requestSeq.current += 1;
      toastGeneration.current += 1;
      if (toastTimer.current !== null) {
        window.clearTimeout(toastTimer.current);
        toastTimer.current = null;
      }
    };
  }, [fetchJobs]);

  async function reloadJobs() {
    const requestId = ++requestSeq.current;
    try {
      const res = await fetchJobs();
      if (!mounted.current || requestId !== requestSeq.current) return;
      setJobs(res.schedulers.map(toCronJobConfig));
      setLoadError(null);
    } catch (err) {
      if (!mounted.current || requestId !== requestSeq.current) return;
      setLoadError(schedulerErrorKey(err));
    } finally {
      if (mounted.current && requestId === requestSeq.current) setLoading(false);
    }
  }

  const visible = useMemo(
    () =>
      jobs.filter(
        (j) =>
          (filter === "all" ||
            (filter === "enabled" ? j.isEnabled : !j.isEnabled)) &&
          j.name.toLowerCase().includes(query.toLowerCase())
      ),
    [jobs, filter, query]
  );

  function notify(msg: CronNotice) {
    const generation = ++toastGeneration.current;
    if (toastTimer.current !== null) {
      window.clearTimeout(toastTimer.current);
    }
    setToast(msg);
    toastTimer.current = window.setTimeout(() => {
      if (!mounted.current || generation !== toastGeneration.current) return;
      toastTimer.current = null;
      setToast(null);
    }, 3200);
  }

  // PATCH /scheduler/jobs/:id { is_enable, cron_expression, timeout }
  async function toggleJob(job: CronJobConfig) {
    setBusyId(job.id);
    try {
      await apiClient.updateSchedulerJob({
        id: job.id,
        is_enable: !job.isEnabled,
        cron_expression: job.cronExpression,
        timeout: job.timeout,
        expected_version: job.version,
      });
      if (!mounted.current) return;
      notify({
        key: !job.isEnabled ? "admin.cron.notice.scheduled" : "admin.cron.notice.paused",
        params: { name: job.name },
      });
      await reloadJobs(); // version/updated_at are authoritative server-side
    } catch {
      if (mounted.current) {
        notify({ key: "admin.cron.notice.error" });
      }
    } finally {
      if (mounted.current) setBusyId(null);
    }
  }

  // PATCH /scheduler/jobs/:id { is_enable, cron_expression, timeout }
  async function saveJob(updated: CronJobConfig) {
    setBusyId(updated.id);
    try {
      await apiClient.updateSchedulerJob({
        id: updated.id,
        is_enable: updated.isEnabled,
        cron_expression: updated.cronExpression,
        timeout: updated.timeout,
        expected_version: updated.version,
      });
      if (!mounted.current) return;
      notify({ key: "admin.cron.notice.saved", params: { name: updated.name } });
      setEditing(null);
      await reloadJobs();
    } catch {
      if (mounted.current) {
        notify({ key: "admin.cron.notice.error" });
      }
    } finally {
      if (mounted.current) setBusyId(null);
    }
  }

  async function createJob(input: ApiCreateSchedulerJobParams) {
    setBusyId("create");
    try {
      const created = await apiClient.createSchedulerJob(input);
      if (!mounted.current) return;
      setCreateOpen(false);
      notify({ key: "admin.cron.notice.created", params: { name: created.name } });
      await reloadJobs();
    } catch (err) {
      if (mounted.current) notify({ key: schedulerErrorKey(err) });
    } finally {
      if (mounted.current) setBusyId(null);
    }
  }

  return (
    <div className="flex min-h-[calc(100dvh_-_3.5rem)] min-w-0 flex-col lg:min-h-screen">
      {/* Topbar */}
      <header className="sticky top-14 z-10 flex h-14 items-center justify-between border-b border-border bg-card px-3 sm:px-6 lg:top-0">
        <div className="flex items-center gap-1.5 text-sm text-muted">
          <span>{t("admin.nav.operations")}</span>
          <ChevronRight size={12} />
          <strong className="text-foreground font-semibold">{t("admin.cron.title")}</strong>
        </div>
        <div className="flex items-center gap-3">
          <div
            className="w-[30px] h-[30px] rounded-full bg-tag-bg border border-border grid place-items-center text-xs font-semibold"
            title={t("admin.nav.console")}
          >
            OA
          </div>
        </div>
      </header>

      <main className="flex min-w-0 w-full max-w-[1280px] flex-col gap-4 p-3 sm:p-6">
        {/* Page head */}
        <div className="flex items-start justify-between gap-4 flex-wrap">
          <div>
            <h1 className="text-[22px] font-bold tracking-tight">{t("admin.cron.title")}</h1>
            <p className="text-muted text-[13px] mt-0.5 max-w-[560px]">
              {t("admin.cron.pageDescription")}
            </p>
          </div>
          <button
            onClick={() => setCreateOpen(true)}
            disabled={busyId !== null}
            className="flex min-h-11 items-center gap-2 rounded-full bg-primary px-[18px] text-[13px] font-semibold text-graphite transition-colors hover:bg-primary-hover disabled:opacity-50"
          >
            {t("admin.cron.registerJob")}
          </button>
        </div>

        {/* Toolbar */}
        <div className="flex items-center gap-2.5 flex-wrap">
          <div className="flex h-11 min-w-[200px] max-w-[320px] flex-1 items-center gap-2 rounded-lg border border-control-border bg-card px-3">
            <Search size={14} className="text-muted shrink-0" />
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder={t("admin.cron.searchPlaceholder")}
              aria-label={t("admin.cron.searchLabel")}
              className="w-full min-w-0 border-none bg-transparent text-[13px] text-foreground"
            />
          </div>
          <div className="flex bg-tag-bg rounded-full p-[3px]">
            {(["all", "enabled", "disabled"] as Filter[]).map((f) => (
              <button
                key={f}
                onClick={() => setFilter(f)}
                className={`min-h-11 rounded-full px-3.5 text-xs font-medium capitalize transition-colors ${
                  filter === f
                    ? "bg-card text-foreground font-semibold shadow-sm"
                    : "text-muted hover:text-foreground"
                }`}
              >
                {t(f === "all" ? "admin.cron.filter.all" : f === "enabled" ? "admin.cron.filter.enabled" : "admin.cron.filter.disabled")}
              </button>
            ))}
          </div>
        </div>

        {/* Table */}
        <div className="min-w-0 overflow-hidden rounded-xl border border-border bg-card">
          {loadError && (
            <div className="flex flex-wrap items-start justify-between gap-2 border-b border-danger/30 bg-danger-bg px-4 py-3 text-[13px] text-danger-ink">
              <span className="min-w-0 flex-1 break-words">{t(loadError)}</span>
              <button
                onClick={() => {
                  requestSeq.current += 1;
                  setLoading(true);
                  void reloadJobs();
                }}
                className="min-h-11 shrink-0 px-3 font-semibold underline underline-offset-2"
              >
                {t("admin.common.retry")}
              </button>
            </div>
          )}
          <div
            role="region"
            tabIndex={0}
            aria-label={t("admin.cron.tableLabel")}
            className="max-w-full overflow-x-auto"
          >
          <table className="w-full min-w-[720px] border-collapse">
            <thead>
              <tr className="bg-background">
                {[
                  t("admin.cron.column.job"),
                  t("admin.cron.column.schedule"),
                  t("admin.cron.column.timeout"),
                  t("admin.cron.column.version"),
                  t("admin.cron.column.lastUpdated"),
                  t("admin.cron.column.status"),
                ].map(
                  (h) => (
                    <th
                      key={h}
                      className="text-left text-[11px] font-semibold uppercase tracking-wider text-muted px-4 py-3 border-b border-border whitespace-nowrap"
                    >
                      {h}
                    </th>
                  )
                )}
                <th className="text-right text-[11px] font-semibold uppercase tracking-wider text-muted px-4 py-3 border-b border-border">
                  {t("admin.cron.column.enabled")}
                </th>
              </tr>
            </thead>
            <tbody>
              {loading && (
                <tr>
                  <td
                    colSpan={7}
                    className="px-4 py-12 text-center text-muted text-[13px]"
                  >
                    {t("admin.cron.loadingJobs")}
                  </td>
                </tr>
              )}
              {!loading && visible.map((j) => (
                <tr key={j.id} className="hover:bg-tag-bg/60 transition-colors">
                  <td className="px-4 py-3.5 border-b border-border">
                    <div className="font-semibold text-[13px]">{j.name}</div>
                    <div className="font-mono text-[11px] text-muted mt-0.5">
                      {j.id.slice(0, 8)}
                    </div>
                    {!j.valid && (
                      <div className="mt-1 max-w-[220px] text-[11px] leading-relaxed text-danger-ink">
                        {t("admin.cron.invalidLegacyExplanation")}
                      </div>
                    )}
                  </td>
                  <td className="px-4 py-3.5 border-b border-border">
                    <span className="font-mono text-xs bg-tag-bg rounded-md px-2 py-[3px]">
                      {j.cronExpression}
                    </span>
                    <div className="text-xs text-muted mt-1">
                      {localizedCronDescription(j.cronExpression, t, number) ?? "—"}
                    </div>
                  </td>
                  <td className="px-4 py-3.5 border-b border-border tabular-nums">
                    {number(j.timeout)}{t("admin.cron.secondsShort")}
                  </td>
                  <td className="px-4 py-3.5 border-b border-border">
                    <span className="font-mono text-[11px] text-muted bg-tag-bg rounded-md px-1.5 py-0.5">
                      v{j.version}
                    </span>
                  </td>
                  <td className="px-4 py-3.5 border-b border-border text-xs text-muted whitespace-nowrap">
                    {Number.isNaN(new Date(j.updatedAt).getTime())
                      ? j.updatedAt
                      : date(j.updatedAt, { year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" })}
                  </td>
                  <td className="px-4 py-3.5 border-b border-border">
                    <span
                      className={`inline-flex items-center gap-1.5 text-xs font-semibold rounded-full px-2.5 py-[3px] ${
                        !j.valid
                          ? "bg-danger-bg text-danger-ink"
                          : j.isEnabled
                          ? "bg-success-bg text-success-ink"
                          : "bg-tag-bg text-muted"
                      }`}
                    >
                      <span className="w-1.5 h-1.5 rounded-full bg-current" />
                      {!j.valid
                        ? t("admin.cron.invalid")
                        : j.isEnabled
                          ? t("admin.cron.scheduled")
                          : t("admin.cron.paused")}
                    </span>
                  </td>
                  <td className="px-4 py-3.5 border-b border-border text-right whitespace-nowrap">
                    <button
                      onClick={() => setEditing(j)}
                      disabled={busyId === j.id || !j.valid}
                      title={!j.valid ? t("admin.cron.invalidLegacyExplanation") : undefined}
                      className="min-h-11 rounded-lg px-3 text-[13px] text-muted transition-colors hover:bg-tag-bg hover:text-foreground disabled:opacity-40"
                    >
                      {t("admin.cron.edit")}
                    </button>
                    <Toggle
                      checked={j.isEnabled}
                      disabled={busyId === j.id || !j.valid}
                      onChange={() => toggleJob(j)}
                      label={!j.valid
                        ? t("admin.cron.invalidLegacyLabel", { name: j.name })
                        : t("admin.cron.toggleJob", { name: j.name })}
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          </div>
          {!loading && !loadError && visible.length === 0 && (
            <div className="px-4 py-12 text-center text-[13px] text-muted">
              {jobs.length === 0
                ? t("admin.cron.empty")
                : t("admin.cron.noFilterMatches")}
            </div>
          )}
        </div>
      </main>

      {/* Edit drawer */}
      {editing && (
        <EditDrawer
          job={editing}
          onClose={() => setEditing(null)}
          onSave={saveJob}
        />
      )}

      {createOpen && (
        <CreateDrawer
          saving={busyId === "create"}
          onClose={() => setCreateOpen(false)}
          onCreate={createJob}
        />
      )}

      {/* Toast */}
      {toast && (
        <div className="fixed bottom-6 left-1/2 -translate-x-1/2 bg-tag-bg border border-border text-foreground text-[13px] rounded-lg px-5 py-2.5 flex items-center gap-2.5 shadow-lg z-50">
          <span className="w-[7px] h-[7px] rounded-full bg-primary" />
          {t(toast.key, toast.params)}
        </div>
      )}
    </div>
  );
}

function Toggle({
  checked,
  onChange,
  label,
  disabled = false,
}: {
  checked: boolean;
  onChange: () => void;
  label: string;
  disabled?: boolean;
}) {
  return (
    <button
      role="switch"
      aria-checked={checked}
      aria-label={label}
      onClick={onChange}
      disabled={disabled}
      className={`relative ml-2 grid h-11 w-11 shrink-0 place-items-center transition-colors ${
        disabled
          ? "opacity-40 cursor-not-allowed"
          : "focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
      }`}
    >
      <span
        className={`relative h-[22px] w-10 rounded-full border transition-colors ${
          checked
            ? "border-primary bg-primary"
            : "border-border bg-tag-bg"
        }`}
      >
        <span
          className={`absolute left-[2px] top-[2px] h-4 w-4 rounded-full shadow-sm transition-transform ${
            checked ? "translate-x-[18px] bg-graphite" : "bg-white"
          }`}
        />
      </span>
    </button>
  );
}

function CreateDrawer({
  saving,
  onClose,
  onCreate,
}: {
  saving: boolean;
  onClose: () => void;
  onCreate: (job: ApiCreateSchedulerJobParams) => Promise<void>;
}) {
  const { t, number, date } = useI18n();
  const [name, setName] = useState("");
  const [cron, setCron] = useState("0 */5 * * * *");
  const [timeout, setTimeoutSecs] = useState(30);

  const parsed = parseCron(cron);
  const next = parsed.error ? null : nextTick(cron);
  const validName = /^[a-z][a-z0-9._-]{0,99}$/.test(name);
  const validTimeout = Number.isInteger(timeout) && timeout >= 1 && timeout <= 600;
  const canCreate = validName && parsed.error === null && validTimeout && !saving;

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canCreate) return;
    await onCreate({
      name,
      job_type: "reservation-cleaner-job",
      cron_expression: cron,
      timeout,
    });
  }

  return (
    <>
      <div
        className="fixed inset-0 z-40 bg-black/50"
        onClick={() => { if (!saving) onClose(); }}
      />
      <aside
        role="dialog"
        aria-modal="true"
        aria-labelledby="scheduler-create-title"
        data-admin-ops-create-drawer
        className="fixed right-0 top-0 z-50 flex h-[100dvh] w-[min(480px,100vw)] flex-col border-l border-border bg-card"
      >
        <div className="flex items-center justify-between border-b border-border px-3 py-3 sm:px-[22px] sm:py-4">
          <div className="min-w-0 flex-1">
            <h2 id="scheduler-create-title" className="text-base font-bold">
              {t("admin.cron.create.title")}
            </h2>
            <p className="mt-1 text-xs leading-relaxed text-muted">
              {t("admin.cron.create.description")}
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            disabled={saving}
            aria-label={t("admin.cron.drawer.close")}
            className="grid h-11 w-11 shrink-0 place-items-center rounded-lg text-muted hover:bg-tag-bg hover:text-foreground disabled:opacity-40"
          >
            <X size={18} />
          </button>
        </div>

        <form id="scheduler-create-form" onSubmit={submit} className="flex min-h-0 flex-1 flex-col">
          <div className="flex min-h-0 min-w-0 flex-1 flex-col gap-[22px] overflow-y-auto px-3 py-4 sm:px-[22px] sm:py-[22px]">
            <div className="flex flex-col gap-1.5">
              <label htmlFor="scheduler-create-name" className="text-xs font-semibold">
                {t("admin.cron.create.name")}
              </label>
              <input
                id="scheduler-create-name"
                required
                maxLength={100}
                disabled={saving}
                autoComplete="off"
                value={name}
                onChange={(event) => setName(event.target.value)}
                aria-describedby="scheduler-create-name-hint"
                pattern="[a-z][a-z0-9._-]{0,99}"
                spellCheck={false}
                className="h-11 min-w-0 rounded-lg border border-control-border bg-card px-3 text-[13px] font-mono text-foreground focus:border-primary"
              />
              <p id="scheduler-create-name-hint" className="text-xs text-muted">
                {t("admin.cron.create.nameHint")}
              </p>
            </div>

            <div className="flex flex-col gap-1.5">
              <label htmlFor="scheduler-create-type" className="text-xs font-semibold">
                {t("admin.cron.create.jobType")}
              </label>
              <select
                id="scheduler-create-type"
                value="reservation-cleaner-job"
                disabled
                className="h-11 rounded-lg border border-control-border bg-tag-bg px-3 text-[13px] text-foreground opacity-80"
              >
                <option value="reservation-cleaner-job">{t("admin.cron.create.reservationCleaner")}</option>
              </select>
              <p className="text-xs leading-relaxed text-muted">
                {t("admin.cron.create.jobTypeDescription")}
              </p>
            </div>

            <div className="flex flex-col gap-1.5">
              <div className="flex items-center justify-between">
                <label htmlFor="scheduler-create-cron" className="text-xs font-semibold">
                  {t("admin.cron.drawer.expression")}
                </label>
                <span className="text-[11px] text-muted">{t("admin.cron.drawer.sixFields")}</span>
              </div>
              <input
                id="scheduler-create-cron"
                required
                maxLength={50}
                disabled={saving}
                value={cron}
                onChange={(event) => setCron(event.target.value)}
                spellCheck={false}
                aria-invalid={parsed.error !== null}
                className="h-11 min-w-0 rounded-lg border border-control-border bg-card px-3 text-[13px] font-mono text-foreground focus:border-primary"
              />
              <div className="flex flex-wrap gap-1.5">
                {CRON_PRESETS.map((preset) => (
                  <button
                    key={preset.value}
                    type="button"
                    disabled={saving}
                    onClick={() => setCron(preset.value)}
                    className={`min-h-11 rounded-full border px-3 text-xs font-mono transition-colors disabled:opacity-40 ${
                      cron === preset.value
                        ? "border-primary bg-primary text-graphite"
                        : "border-control-border bg-card text-muted hover:border-primary hover:text-foreground"
                    }`}
                  >
                    {preset.labelKey ? t(preset.labelKey) : preset.label}
                  </button>
                ))}
              </div>
              {parsed.error ? (
                <p className="text-xs text-danger-ink">{localizedCronError(parsed.error, t, number)}</p>
              ) : (
                <div className="rounded-lg border border-border bg-background px-3 py-2.5">
                  <div className="mb-1 text-[11px] font-semibold uppercase tracking-wider text-muted">
                    {t("admin.cron.drawer.interpretedAs")}
                  </div>
                  <div className="text-[13px]">{localizedCronDescription(cron, t, number)}</div>
                  {next && (
                    <div className="mt-1.5 text-xs text-muted">
                      {t("admin.cron.drawer.nextRun", {
                        time: date(next, { hour: "numeric", minute: "2-digit" }),
                        date: date(next, { year: "numeric", month: "2-digit", day: "2-digit" }),
                      })}
                    </div>
                  )}
                </div>
              )}
            </div>

            <div className="flex flex-col gap-1.5">
              <label htmlFor="scheduler-create-timeout" className="text-xs font-semibold">
                {t("admin.cron.drawer.timeout")}
              </label>
              <input
                id="scheduler-create-timeout"
                required
                type="number"
                disabled={saving}
                min={1}
                max={600}
                step={1}
                value={timeout}
                onChange={(event) => setTimeoutSecs(event.target.valueAsNumber)}
                aria-describedby="scheduler-create-timeout-hint"
                className="h-11 w-36 rounded-lg border border-control-border bg-card px-3 text-[13px] tabular-nums text-foreground focus:border-primary"
              />
              <p id="scheduler-create-timeout-hint" className="text-xs text-muted">
                {t("admin.cron.drawer.secondsRange")}
              </p>
            </div>

            <div className="rounded-[10px] border border-border bg-tag-bg px-3.5 py-3 text-xs leading-relaxed text-foreground">
              {t("admin.cron.create.disabledUntilEnabled")}
            </div>
          </div>

          <div className="flex justify-end gap-2.5 border-t border-border px-3 py-3 sm:px-[22px] sm:py-4">
            <button
              type="button"
              onClick={onClose}
              disabled={saving}
              className="h-11 rounded-full border border-border bg-card px-4 text-[13px] font-semibold text-foreground hover:bg-tag-bg disabled:opacity-40"
            >
              {t("admin.cron.drawer.cancel")}
            </button>
            <button
              type="submit"
              disabled={!canCreate}
              className="flex h-11 items-center gap-2 rounded-full bg-primary px-[18px] text-[13px] font-semibold text-graphite hover:bg-primary-hover disabled:cursor-not-allowed disabled:opacity-40"
            >
              <Check size={14} />
              {saving ? t("admin.cron.create.creating") : t("admin.cron.create.submit")}
            </button>
          </div>
        </form>
      </aside>
    </>
  );
}

function EditDrawer({
  job,
  onClose,
  onSave,
}: {
  job: CronJobConfig;
  onClose: () => void;
  onSave: (job: CronJobConfig) => Promise<void>;
}) {
  const { t, number, date } = useI18n();
  const [enabled, setEnabled] = useState(job.isEnabled);
  const [cron, setCron] = useState(job.cronExpression);
  const [timeout, setTimeoutSecs] = useState(job.timeout);
  const [saving, setSaving] = useState(false);

  const parsed = parseCron(cron);
  const next = parsed.error ? null : nextTick(cron);
  const canSave = parsed.error === null;

  return (
    <>
      <div
        className="fixed inset-0 bg-black/50 z-40"
        onClick={onClose}
      />
      <aside data-admin-ops-edit-drawer className="fixed right-0 top-0 z-50 flex h-[100dvh] w-[min(440px,100vw)] flex-col border-l border-border bg-card">
        <div className="flex items-center justify-between border-b border-border px-3 py-3 sm:px-[22px] sm:py-4">
          <div className="min-w-0 flex-1 [overflow-wrap:anywhere]">
            <h2 className="min-w-0 break-words text-base font-bold">{t("admin.cron.drawer.title")}</h2>
            <p className="mt-0.5 text-xs text-muted [overflow-wrap:anywhere]">
              {t("admin.cron.drawer.jobVersion", { name: job.name, version: number(job.version) })}
            </p>
          </div>
          <button
            onClick={onClose}
            aria-label={t("admin.cron.drawer.close")}
            className="grid h-11 w-11 shrink-0 place-items-center rounded-lg text-muted hover:bg-tag-bg hover:text-foreground"
          >
            <X size={18} />
          </button>
        </div>

        <div className="flex min-h-0 min-w-0 flex-1 flex-col gap-[22px] overflow-y-auto px-3 py-4 sm:px-[22px] sm:py-[22px]">
          {/* Enabled switch */}
          <div className="flex items-center justify-between bg-background border border-border rounded-[10px] px-3.5 py-3">
            <div>
              <div className="text-[13px] font-semibold">{t("admin.cron.drawer.jobEnabled")}</div>
              <div className="text-xs text-muted">{t("admin.cron.drawer.disabledExplanation")}</div>
            </div>
            <Toggle
              checked={enabled}
              onChange={() => setEnabled(!enabled)}
              label={t("admin.cron.drawer.jobEnabled")}
            />
          </div>

          {/* Cron expression */}
          <div className="flex flex-col gap-1.5">
            <div className="flex items-center justify-between">
              <label htmlFor="cron-input" className="text-xs font-semibold">
                {t("admin.cron.drawer.expression")}
              </label>
              <span className="text-[11px] text-muted font-normal">
                {t("admin.cron.drawer.sixFields")}
              </span>
            </div>
            <input
              id="cron-input"
              value={cron}
              onChange={(e) => setCron(e.target.value)}
              spellCheck={false}
              className="h-11 min-w-0 rounded-lg border border-control-border bg-card px-3 text-[13px] font-mono text-foreground transition-colors focus:border-primary"
            />
            <div className="flex gap-1.5 flex-wrap">
              {CRON_PRESETS.map((p) => (
                <button
                  key={p.value}
                  onClick={() => setCron(p.value)}
                  className={`min-h-11 rounded-full border px-3 text-xs font-mono transition-colors ${
                    cron === p.value
                      ? "bg-primary border-primary text-graphite"
                      : "border-control-border bg-card text-muted hover:border-primary hover:text-foreground"
                  }`}
                >
                  {p.labelKey ? t(p.labelKey) : p.label}
                </button>
              ))}
            </div>
            {parsed.error ? (
              <p className="text-xs text-danger-ink">{localizedCronError(parsed.error, t, number)}</p>
            ) : (
              <div className="bg-background border border-border rounded-lg px-3 py-2.5">
                <div className="text-[11px] uppercase tracking-wider text-muted font-semibold mb-1">
                  {t("admin.cron.drawer.interpretedAs")}
                </div>
                <div className="text-[13px]">{localizedCronDescription(cron, t, number)}</div>
                {next && (
                  <div className="text-xs text-muted mt-1.5">
                    {t("admin.cron.drawer.nextRun", {
                      time: date(next, { hour: "numeric", minute: "2-digit" }),
                      date: date(next, { year: "numeric", month: "2-digit", day: "2-digit" }),
                    })}
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Timeout */}
          <div className="flex flex-col gap-1.5">
            <div className="flex items-center justify-between">
              <label htmlFor="timeout-input" className="text-xs font-semibold">
                {t("admin.cron.drawer.timeout")}
              </label>
              <span className="text-[11px] text-muted font-normal">
                {t("admin.cron.drawer.jobContextDeadline")}
              </span>
            </div>
            <div className="flex h-11 w-[200px] overflow-hidden rounded-lg border border-control-border focus-within:border-primary">
              <button
                type="button"
                aria-label={t("admin.cron.drawer.decreaseTimeout")}
                onClick={() =>
                  setTimeoutSecs((t) => Math.max(1, t - 5))
                }
                className="h-11 w-11 shrink-0 bg-tag-bg text-base hover:bg-border transition-colors"
              >
                −
              </button>
              <input
                id="timeout-input"
                type="number"
                min={1}
                max={600}
                value={timeout}
                onChange={(e) =>
                  setTimeoutSecs(
                    Math.max(1, Math.min(600, Number(e.target.value) || 30))
                  )
                }
                className="h-11 w-full min-w-0 border-x border-control-border bg-card text-center text-[13px] tabular-nums text-foreground"
              />
              <button
                type="button"
                aria-label={t("admin.cron.drawer.increaseTimeout")}
                onClick={() =>
                  setTimeoutSecs((t) => Math.min(600, t + 5))
                }
                className="h-11 w-11 shrink-0 bg-tag-bg text-base hover:bg-border transition-colors"
              >
                +
              </button>
            </div>
            <p className="text-xs text-muted">
              {t("admin.cron.drawer.secondsRange")}
            </p>
          </div>

          {/* Propagation note */}
          <div className="bg-tag-bg border border-border rounded-[10px] px-3.5 py-3 text-xs text-foreground leading-relaxed">
            {t("admin.cron.drawer.propagation")}
          </div>
        </div>

        <div className="flex justify-end gap-2.5 border-t border-border px-3 py-3 sm:px-[22px] sm:py-4">
          <button
            onClick={onClose}
            className="h-11 rounded-full border border-border bg-card px-4 text-[13px] font-semibold text-foreground transition-colors hover:bg-tag-bg"
          >
            {t("admin.cron.drawer.cancel")}
          </button>
          <button
            disabled={!canSave || saving}
            onClick={async () => {
              setSaving(true);
              try {
                await onSave({
                  ...job,
                  isEnabled: enabled,
                  cronExpression: cron,
                  timeout,
                });
              } finally {
                setSaving(false);
              }
            }}
            className="flex h-11 items-center gap-2 rounded-full bg-primary px-[18px] text-[13px] font-semibold text-graphite transition-colors hover:bg-primary-hover disabled:cursor-not-allowed disabled:opacity-40"
          >
            <Check size={14} />
            {saving ? t("admin.cron.drawer.saving") : t("admin.cron.drawer.saveChanges")}
          </button>
        </div>
      </aside>
    </>
  );
}
