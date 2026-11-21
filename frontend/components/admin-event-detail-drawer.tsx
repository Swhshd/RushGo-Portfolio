import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import {
  Activity,
  AlertCircle,
  CalendarDays,
  Check,
  Copy,
  Loader2,
  MapPin,
  Pencil,
  Tag,
  X,
} from "lucide-react";
import { EventArtwork } from "@/components/event-artwork";
import { apiClient } from "@/lib/api/client";
import type { ApiEvent, ApiScannerAssignment, ApiTicketTier } from "@/lib/api/types";
import { useDialogFocus } from "@/lib/use-dialog-focus";
import { isAdminEventOperationsId } from "@/lib/admin-event-operations";
import { useI18n, type TranslationKey } from "@/lib/i18n";

type DetailState =
  | { status: "loading" }
  | { status: "ready"; event: ApiEvent }
  | { status: "missing" }
  | { status: "error"; message: TranslationKey };

function statusOf(err: unknown): number | undefined {
  return typeof err === "object" && err !== null && "status" in err
    ? (err as { status?: number }).status
    : undefined;
}

function messageForError(err: unknown, fallback: TranslationKey): TranslationKey {
  if (statusOf(err) === 401) {
    return "admin.eventDetail.sessionExpired";
  }
  if (statusOf(err) === 403) {
    return "admin.eventDetail.accessRequired";
  }
  return fallback;
}

export function EventDetailDrawer({
  eventId,
  onClose,
  onEdit,
}: {
  eventId: string;
  onClose: () => void;
  onEdit: (event: ApiEvent) => void;
}) {
  const { t } = useI18n();
  const [state, setState] = useState<DetailState>({ status: "loading" });
  const [reloadKey, setReloadKey] = useState(0);
  const dialogRef = useRef<HTMLDivElement>(null);
  useDialogFocus(dialogRef, { onClose });

  useEffect(() => {
    let cancelled = false;
    apiClient
      .getAdminEvent(eventId)
      .then((event) => {
        if (!cancelled) setState({ status: "ready", event });
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        if (statusOf(err) === 404) {
          setState({ status: "missing" });
        } else {
          setState({ status: "error", message: messageForError(err, "admin.eventDetail.requestFailed") });
        }
      });
    return () => {
      cancelled = true;
    };
  }, [eventId, reloadKey]);

  return (
    <div className="fixed inset-0 z-50 flex">
      <div
        className="flex-1 bg-black/40"
        onClick={onClose}
        aria-hidden="true"
      />
      <aside
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby="admin-event-detail-heading admin-event-detail-title"
        tabIndex={-1}
        className="flex h-full w-full max-w-full flex-col overflow-y-auto border-l border-border bg-card sm:w-[560px]"
      >
        <div className="sticky top-0 z-10 flex h-14 items-center justify-between border-b border-border bg-card px-5">
          <div className="flex items-center gap-2">
            <CalendarDays size={16} className="text-muted" />
            <strong
              id="admin-event-detail-heading"
              className="text-sm font-semibold"
            >
              {t("admin.eventDetail.heading")}
            </strong>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="grid h-11 w-11 shrink-0 place-items-center rounded-lg text-muted transition-colors hover:bg-tag-bg hover:text-foreground"
            aria-label={t("admin.eventDetail.close")}
            data-dialog-autofocus
          >
            <X size={16} />
          </button>
        </div>

        <div className="flex min-w-0 flex-1 flex-col gap-5 p-5">
          {state.status === "loading" && (
            <div
              role="status"
              aria-live="polite"
              className="flex flex-col items-center gap-2 py-16 text-muted text-[13px]"
            >
              <Loader2 size={20} className="animate-spin" />
              {t("admin.eventDetail.loading")}
            </div>
          )}

          {state.status === "missing" && (
            <div className="flex flex-col items-center gap-3 py-16 text-center">
              <AlertCircle size={22} className="text-danger" />
              <div role="alert">
                <p className="text-sm font-semibold">{t("admin.eventDetail.notFound")}</p>
                <p className="mt-1 max-w-[360px] text-[13px] text-muted">
                  {t("admin.eventDetail.notFoundExplanation")}
                </p>
              </div>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => {
                    setState({ status: "loading" });
                    setReloadKey((key) => key + 1);
                  }}
                  className="h-11 rounded-full border border-border px-4 text-[13px] font-medium hover:bg-tag-bg"
                >
                  {t("admin.eventDetail.retry")}
                </button>
                <button
                  type="button"
                  onClick={onClose}
                  className="h-11 rounded-full px-4 text-[13px] font-medium text-muted hover:bg-tag-bg"
                >
                  {t("admin.eventDetail.backToEvents")}
                </button>
              </div>
            </div>
          )}

          {state.status === "error" && (
            <div className="flex flex-col items-center gap-3 py-16 text-center">
              <AlertCircle size={22} className="text-danger" />
              <div role="alert">
                <p className="text-sm font-semibold">
                  {t("admin.eventDetail.unavailable")}
                </p>
                <p className="mt-1 max-w-[340px] text-[13px] text-muted">
                  {t(state.message)}
                </p>
              </div>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => {
                    setState({ status: "loading" });
                    setReloadKey((key) => key + 1);
                  }}
                  className="h-11 rounded-full border border-border px-4 text-[13px] font-medium hover:bg-tag-bg"
                >
                  {t("admin.eventDetail.retry")}
                </button>
                <button
                  type="button"
                  onClick={onClose}
                  className="h-11 rounded-full px-4 text-[13px] font-medium text-muted hover:bg-tag-bg"
                >
                  {t("admin.eventDetail.backToEvents")}
                </button>
              </div>
            </div>
          )}

          {state.status === "ready" && (
            <EventDetailContent event={state.event} />
          )}
        </div>

        <div className="mt-auto flex items-center justify-end gap-2 border-t border-border bg-card p-5">
          <button
            type="button"
            onClick={onClose}
            className="h-11 rounded-full px-4 text-[13px] font-medium text-muted transition-colors hover:bg-tag-bg"
          >
            {t("admin.eventDetail.backToList")}
          </button>
          {state.status === "ready" && (
            <button
              type="button"
              onClick={() => onEdit(state.event)}
              className="inline-flex h-11 items-center gap-2 rounded-full bg-primary px-5 text-[13px] font-semibold text-graphite transition-colors hover:bg-primary-hover"
            >
              <Pencil size={14} />
              {t("admin.eventDetail.editEvent")}
            </button>
          )}
        </div>
      </aside>
    </div>
  );
}

/** Read-only detail body. Exported separately so markup can be tested. */
export function EventDetailContent({ event }: { event: ApiEvent }) {
  const { t, date } = useI18n();
  const [copied, setCopied] = useState(false);

  async function copyId() {
    try {
      await navigator.clipboard.writeText(event.id);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1800);
    } catch {
      // Clipboard access can be denied; the ID stays visible for manual copy.
    }
  }

  return (
    <div className="flex min-w-0 flex-col gap-5">
      {event.image_url ? (
        <div className="aspect-[16/9] overflow-hidden rounded-xl border border-border bg-tag-bg">
          <EventArtwork
            src={event.image_url}
            title={event.title}
            category={event.category}
          />
        </div>
      ) : null}

      <div>
        <div className="flex flex-wrap items-start justify-between gap-2">
          <h2
            id="admin-event-detail-title"
            className="min-w-0 break-words text-[20px] font-bold leading-tight tracking-tight"
          >
            {event.title}
          </h2>
          <span className="inline-flex items-center gap-1.5 rounded-full bg-tag-bg px-2.5 py-[3px] text-xs font-semibold capitalize text-muted">
            <span className="h-1.5 w-1.5 rounded-full bg-current" />
            {event.status || "—"}
          </span>
        </div>
        <div className="mt-1.5 flex items-center gap-1.5 text-[13px] capitalize text-muted">
          <Tag size={13} />
          {event.category || "—"}
        </div>
        <p className="mt-1.5 text-[11px] text-muted">
          {t("admin.eventDetail.statusAuthority")}
        </p>
      </div>

      <div className="flex items-center gap-2 rounded-lg border border-border bg-background px-3 py-2">
        <span className="text-[11px] font-semibold uppercase tracking-wider text-muted">
          {t("admin.eventDetail.eventId")}
        </span>
        <code className="min-w-0 flex-1 break-all font-mono text-[11px] text-foreground">
          {event.id}
        </code>
        <button
          type="button"
          onClick={copyId}
          className="inline-flex h-11 shrink-0 items-center gap-1 rounded-md px-2 text-[11px] font-medium text-muted hover:bg-tag-bg hover:text-foreground"
          aria-label={t("admin.eventDetail.copyId")}
        >
          {copied ? <Check size={12} /> : <Copy size={12} />}
          {copied ? t("admin.eventDetail.copied") : t("admin.eventDetail.copy")}
        </button>
      </div>

      {isAdminEventOperationsId(event.id) ? (
        <Link
          href={`/admin/events/${encodeURIComponent(event.id)}/operations`}
          className="inline-flex min-h-11 items-center justify-center gap-2 rounded-full border border-border px-4 text-[12px] font-semibold text-foreground transition-colors hover:bg-tag-bg focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
        >
          <Activity size={14} aria-hidden="true" />
          {t("admin.eventDetail.eventOperations")}
        </Link>
      ) : null}

      <dl className="grid grid-cols-1 gap-3 sm:grid-cols-2">
        <DetailItem label={t("admin.eventDetail.dateTime")} icon={<CalendarDays size={13} />}>
          {date(event.date, { year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" })}
        </DetailItem>
        <DetailItem label={t("admin.eventDetail.venue")} icon={<MapPin size={13} />}>
          {event.venue || "—"}
        </DetailItem>
        <DetailItem label={t("admin.eventDetail.location")} icon={<MapPin size={13} />}>
          {event.location || "—"}
        </DetailItem>
        <DetailItem label={t("admin.eventDetail.created")} icon={<CalendarDays size={13} />}>
          {event.created_at ? date(event.created_at, { year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }) : "—"}
        </DetailItem>
      </dl>
      <p className="-mt-2 text-[11px] text-muted">
        {t("admin.eventDetail.localTimezone")}
      </p>

      {event.description?.trim() ? (
        <section aria-labelledby="admin-event-description-heading">
          <h3
            id="admin-event-description-heading"
            className="mb-1.5 text-[11px] font-semibold uppercase tracking-wider text-muted"
          >
            {t("admin.eventDetail.description")}
          </h3>
          <p className="whitespace-pre-line text-[13px] leading-relaxed text-foreground/90">
            {event.description}
          </p>
        </section>
      ) : null}

      <section aria-labelledby="admin-event-tiers-heading">
        <h3
          id="admin-event-tiers-heading"
          className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-muted"
        >
          {t("admin.eventDetail.ticketTiers")}
        </h3>
        {(event.tiers ?? []).length === 0 ? (
          <p className="rounded-lg border border-border px-3 py-4 text-[13px] text-muted">
            {t("admin.eventDetail.noTicketTiers")}
          </p>
        ) : (
          <ul className="flex flex-col gap-2">
            {event.tiers.map((tier) => (
              <TierRow key={tier.id} tier={tier} />
            ))}
          </ul>
        )}
        <p className="mt-2 text-[11px] text-muted">
          {t("admin.eventDetail.exactApiValues")}
        </p>
      </section>

      <ScannerAssignmentsPanel key={event.id} eventId={event.id} />
    </div>
  );
}

type ScannerAssignmentsState =
  | { status: "loading"; eventId: string; reloadKey: number }
  | { status: "ready"; eventId: string; reloadKey: number; assignments: ApiScannerAssignment[] }
  | { status: "error"; eventId: string; reloadKey: number; message: TranslationKey };

const userIdPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function ScannerAssignmentsPanel({ eventId }: { eventId: string }) {
  const { t } = useI18n();
  const [state, setState] = useState<ScannerAssignmentsState>({ status: "loading", eventId, reloadKey: 0 });
  const [staffUserId, setStaffUserId] = useState("");
  const [busyUserId, setBusyUserId] = useState<string | null>(null);
  const [formError, setFormError] = useState<TranslationKey | null>(null);
  const [notice, setNotice] = useState<TranslationKey | null>(null);
  const [reloadKey, setReloadKey] = useState(0);
  const visibleState: ScannerAssignmentsState = state.eventId === eventId && state.reloadKey === reloadKey
    ? state
    : { status: "loading", eventId, reloadKey };

  useEffect(() => {
    let cancelled = false;
    apiClient
      .listEventScannerAssignments(eventId)
      .then(({ assignments }) => {
        if (!cancelled) setState({ status: "ready", eventId, reloadKey, assignments });
      })
      .catch((err: unknown) => {
        if (!cancelled) setState({ status: "error", eventId, reloadKey, message: messageForError(err, "admin.scannerAssignments.unavailable") });
      });
    return () => {
      cancelled = true;
    };
  }, [eventId, reloadKey]);

  async function setAssignment(staffId: string, active: boolean) {
    setBusyUserId(staffId);
    setFormError(null);
    setNotice(null);
    try {
      if (active) {
        await apiClient.grantEventScanner(eventId, staffId);
      } else {
        await apiClient.revokeEventScanner(eventId, staffId);
      }
      setNotice(active ? "admin.scannerAssignments.grantedNotice" : "admin.scannerAssignments.revokedNotice");
      setReloadKey((key) => key + 1);
    } catch (err) {
      setFormError(messageForError(err, "admin.scannerAssignments.unavailable"));
    } finally {
      setBusyUserId(null);
    }
  }

  function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const staffId = staffUserId.trim().toLowerCase();
    if (!userIdPattern.test(staffId)) {
      setFormError("admin.scannerAssignments.invalidUuid");
      return;
    }
    setStaffUserId(staffId);
    void setAssignment(staffId, true);
  }

  return (
    <section aria-labelledby="admin-event-scanners-heading" className="border-t border-border pt-4">
      <div className="mb-2">
        <h3 id="admin-event-scanners-heading" className="text-[11px] font-semibold uppercase tracking-wider text-muted">
          {t("admin.scannerAssignments.title")}
        </h3>
        <p className="mt-1 text-[11px] leading-relaxed text-muted">
          {t("admin.scannerAssignments.explanation")}
        </p>
      </div>

      <form onSubmit={submit} className="flex flex-col gap-2 rounded-lg border border-border bg-background p-3 sm:flex-row sm:items-end">
        <div className="min-w-0 flex-1">
          <label htmlFor="scanner-staff-user-id" className="mb-1 block text-[11px] font-medium text-muted">
            {t("admin.scannerAssignments.staffUserId")}
          </label>
          <input
            id="scanner-staff-user-id"
            type="text"
            inputMode="text"
            autoComplete="off"
            spellCheck={false}
            pattern="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
            value={staffUserId}
            onChange={(event) => setStaffUserId(event.target.value)}
            disabled={busyUserId !== null}
            aria-describedby="scanner-staff-user-id-hint"
            className="h-11 w-full min-w-0 rounded-lg border border-control-border bg-card px-3 font-mono text-[12px] text-foreground focus:border-primary"
            placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          />
          <span id="scanner-staff-user-id-hint" className="sr-only">{t("admin.scannerAssignments.accountUuidHint")}</span>
        </div>
        <button
          type="submit"
          disabled={busyUserId !== null}
          className="h-11 shrink-0 rounded-full bg-primary px-4 text-[12px] font-semibold text-graphite transition-colors hover:bg-primary-hover disabled:cursor-not-allowed disabled:opacity-50"
        >
          {busyUserId === staffUserId ? t("admin.scannerAssignments.saving") : t("admin.scannerAssignments.grant")}
        </button>
      </form>

      {formError ? <p role="alert" className="mt-2 text-[12px] text-danger-ink">{t(formError)}</p> : null}
      {notice ? <p role="status" aria-live="polite" className="mt-2 text-[12px] text-success">{t(notice)}</p> : null}

      {visibleState.status === "loading" ? (
        <p role="status" aria-live="polite" className="mt-3 text-[12px] text-muted">{t("admin.scannerAssignments.loading")}</p>
      ) : null}
      {visibleState.status === "error" ? (
        <div className="mt-3 flex flex-wrap items-start justify-between gap-3 rounded-lg border border-danger/30 bg-danger-bg px-3 py-2 text-[12px] text-danger-ink">
          <span role="alert" className="min-w-0 flex-1 break-words">{t(visibleState.message)}</span>
          <button type="button" onClick={() => setReloadKey((key) => key + 1)} className="min-h-11 shrink-0 px-3 font-semibold underline underline-offset-2">{t("admin.scannerAssignments.retry")}</button>
        </div>
      ) : null}
      {visibleState.status === "ready" ? (
        visibleState.assignments.length === 0 ? (
          <p className="mt-3 rounded-lg border border-border px-3 py-3 text-[12px] text-muted">{t("admin.scannerAssignments.noAssignments")}</p>
        ) : (
          <ul aria-label={t("admin.scannerAssignments.listLabel")} className="mt-3 flex flex-col gap-2">
            {visibleState.assignments.map((assignment) => (
              <li key={assignment.staff_user_id} className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border px-3 py-2.5">
                <div className="min-w-0">
                  <code className="block break-all font-mono text-[11px] text-foreground">{assignment.staff_user_id}</code>
                  <span className={`mt-1 inline-flex items-center gap-1.5 text-[11px] ${assignment.active ? "text-success" : "text-muted"}`}>
                    <span aria-hidden="true" className="h-1.5 w-1.5 rounded-full bg-current" />
                    {assignment.active ? t("admin.scannerAssignments.active") : t("admin.scannerAssignments.revoked")}
                  </span>
                </div>
                <button
                  type="button"
                  disabled={busyUserId !== null}
                  onClick={() => void setAssignment(assignment.staff_user_id, !assignment.active)}
                  className="h-11 shrink-0 rounded-full border border-border px-3 text-[11px] font-medium text-muted hover:bg-tag-bg hover:text-foreground disabled:cursor-not-allowed disabled:opacity-50"
                >
                  {busyUserId === assignment.staff_user_id ? t("admin.scannerAssignments.saving") : assignment.active ? t("admin.scannerAssignments.revoke") : t("admin.scannerAssignments.restore")}
                </button>
              </li>
            ))}
          </ul>
        )
      ) : null}
    </section>
  );
}

function DetailItem({
  label,
  icon,
  children,
}: {
  label: string;
  icon: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <div className="min-w-0 rounded-lg border border-border px-3 py-2.5">
      <dt className="flex items-center gap-1.5 text-[11px] font-semibold uppercase tracking-wider text-muted">
        {icon}
        {label}
      </dt>
      <dd className="mt-1 min-w-0 break-words text-[13px]">{children}</dd>
    </div>
  );
}

function TierRow({ tier }: { tier: ApiTicketTier }) {
  const { t, number, usdCents } = useI18n();
  return (
    <li className="rounded-lg border border-border px-3 py-2.5">
      <div className="flex items-center justify-between gap-3">
        <span className="text-[13px] font-semibold">{tier.name}</span>
        <span className="font-mono text-[13px] tabular-nums">
          {usdCents(tier.price_cents)}
        </span>
      </div>
      <div className="mt-1 flex items-center justify-between gap-3 text-[11px] text-muted">
        <span>
          {t("admin.eventDetail.availableOfTotal", { available: number(tier.available_quantity), total: number(tier.total_quantity) })}
        </span>
          <span className="font-mono">{t("admin.eventDetail.tierId", { id: tier.id.slice(0, 8) })}</span>
      </div>
    </li>
  );
}
