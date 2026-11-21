import { useRef } from "react";
import { AlertCircle, CalendarDays, Loader2, X } from "lucide-react";
import type { ApiEvent } from "@/lib/api/types";
import {
  type EventFormErrors,
  type EventFormState,
  type EventTierDraft,
} from "@/lib/admin-events";
import { discoveryCategories } from "@/lib/discovery";
import { useDialogFocus } from "@/lib/use-dialog-focus";
import { useI18n, type TranslationKey } from "@/lib/i18n";

const CATEGORY_OPTIONS = discoveryCategories.filter((item) => item.category);

const inputClass =
  "w-full min-w-0 bg-card border border-control-border rounded-lg px-3 h-11 text-[13px] text-foreground focus:border-primary";

function fieldAria(id: string, error?: string) {
  return error
    ? { "aria-invalid": true as const, "aria-describedby": `${id}-error` }
    : {};
}

function Field({
  label,
  htmlFor,
  error,
  children,
}: {
  label: string;
  htmlFor: string;
  error?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col gap-1.5">
      <label
        htmlFor={htmlFor}
        className="text-[11px] font-semibold uppercase tracking-wider text-muted"
      >
        {label}
      </label>
      {children}
      {error && (
        <p
          id={`${htmlFor}-error`}
          className="text-[11px] text-danger-ink"
          role="alert"
        >
          {error}
        </p>
      )}
    </div>
  );
}

export function EventFormDrawer({
  editing,
  form,
  errors,
  submitting,
  createRetryLocked,
  submitError,
  onField,
  onFieldBlur,
  onTier,
  onTierBlur,
  onAddTier,
  onRemoveTier,
  onClose,
  onSubmit,
}: {
  editing: ApiEvent | null;
  form: EventFormState;
  errors: EventFormErrors;
  submitting: boolean;
  createRetryLocked: boolean;
  submitError: TranslationKey | null;
  onField: <K extends keyof EventFormState>(
    field: K,
    value: EventFormState[K],
  ) => void;
  onFieldBlur: (field: keyof EventFormState) => void;
  onTier: (index: number, field: keyof EventTierDraft, value: string) => void;
  onTierBlur: (index: number, field: keyof EventTierDraft) => void;
  onAddTier: () => void;
  onRemoveTier: (index: number) => void;
  onClose: () => void;
  onSubmit: () => void;
}) {
  const { t, number, usdCents } = useI18n();
  const dialogRef = useRef<HTMLDivElement>(null);
  useDialogFocus(dialogRef, { onClose, closeDisabled: submitting });

  // An event may carry a category outside the discovery list (the list filter
  // already accepts such values); keep the stored value selectable on edit.
  const categoryOptions =
    editing?.category &&
    !CATEGORY_OPTIONS.some((item) => item.category === editing.category)
      ? [
          { category: editing.category, label: editing.category },
          ...CATEGORY_OPTIONS,
        ]
      : CATEGORY_OPTIONS;

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
        aria-labelledby="admin-event-form-heading"
        tabIndex={-1}
        className="flex h-full w-full max-w-full flex-col overflow-y-auto border-l border-border bg-card sm:w-[560px]"
      >
        <div className="sticky top-0 z-10 flex h-14 items-center justify-between border-b border-border bg-card px-5">
          <div className="flex items-center gap-2">
            <CalendarDays size={16} className="text-muted" />
            <strong
              id="admin-event-form-heading"
              className="text-sm font-semibold"
            >
              {editing ? t("admin.events.form.edit") : t("admin.events.form.new")}
            </strong>
          </div>
          <button
            type="button"
            onClick={onClose}
            disabled={submitting}
            className="grid h-11 w-11 shrink-0 place-items-center rounded-lg text-muted transition-colors hover:bg-tag-bg hover:text-foreground disabled:opacity-40"
            aria-label={t("admin.events.form.close")}
          >
            <X size={16} />
          </button>
        </div>

        <fieldset
          disabled={submitting || createRetryLocked}
          className="m-0 flex min-w-0 flex-col gap-4 border-0 p-5"
        >
          <Field label={t("admin.events.form.title")} htmlFor="event-title" error={errors.title ? t(errors.title) : undefined}>
            <input
              id="event-title"
              value={form.title}
              onChange={(event) => onField("title", event.target.value)}
              onBlur={() => onFieldBlur("title")}
              className={inputClass}
              data-dialog-autofocus
              {...fieldAria("event-title", errors.title)}
            />
          </Field>

          <Field
            label={t("admin.events.form.description")}
            htmlFor="event-description"
            error={errors.description ? t(errors.description) : undefined}
          >
            <textarea
              id="event-description"
              value={form.description}
              onChange={(event) => onField("description", event.target.value)}
              onBlur={() => onFieldBlur("description")}
              rows={3}
              className="w-full min-w-0 resize-y rounded-lg border border-control-border bg-card px-3 py-2 text-[13px] text-foreground focus:border-primary"
              {...fieldAria("event-description", errors.description)}
            />
          </Field>

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <Field
              label={t("admin.events.form.category")}
              htmlFor="event-category"
              error={errors.category ? t(errors.category) : undefined}
            >
              <select
                id="event-category"
                value={form.category}
                onChange={(event) => onField("category", event.target.value)}
                onBlur={() => onFieldBlur("category")}
                className={inputClass}
                {...fieldAria("event-category", errors.category)}
              >
                <option value="">{t("admin.events.form.selectCategory")}</option>
                {categoryOptions.map((item) => (
                  <option key={item.category} value={item.category}>
                    {item.label}
                  </option>
                ))}
              </select>
            </Field>

            <Field label={t("admin.events.form.dateTime")} htmlFor="event-date" error={errors.date ? t(errors.date) : undefined}>
              <input
                id="event-date"
                type="datetime-local"
                step="0.001"
                value={form.date}
                onChange={(event) => onField("date", event.target.value)}
                onBlur={() => onFieldBlur("date")}
                className={inputClass}
                {...fieldAria("event-date", errors.date)}
              />
            </Field>
          </div>

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <Field label={t("admin.events.form.venue")} htmlFor="event-venue" error={errors.venue ? t(errors.venue) : undefined}>
              <input
                id="event-venue"
                value={form.venue}
                onChange={(event) => onField("venue", event.target.value)}
                onBlur={() => onFieldBlur("venue")}
                className={inputClass}
                {...fieldAria("event-venue", errors.venue)}
              />
            </Field>
            <Field
              label={t("admin.events.form.location")}
              htmlFor="event-location"
              error={errors.location ? t(errors.location) : undefined}
            >
              <input
                id="event-location"
                value={form.location}
                onChange={(event) => onField("location", event.target.value)}
                onBlur={() => onFieldBlur("location")}
                className={inputClass}
                {...fieldAria("event-location", errors.location)}
              />
            </Field>
          </div>

          <Field label={t("admin.events.form.imageUrl")} htmlFor="event-image" error={errors.imageUrl ? t(errors.imageUrl) : undefined}>
            <input
              id="event-image"
              value={form.imageUrl}
              onChange={(event) => onField("imageUrl", event.target.value)}
              onBlur={() => onFieldBlur("imageUrl")}
              placeholder={t("admin.events.form.imagePlaceholder")}
              className={inputClass}
              {...fieldAria("event-image", errors.imageUrl)}
            />
          </Field>

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <Field label={t("admin.events.form.publicationStatus")} htmlFor="event-publication-status">
              <select id="event-publication-status" value={form.publicationStatus} onChange={(event) => onField("publicationStatus", event.target.value as EventFormState["publicationStatus"])} className={inputClass}>
                <option value="draft">{t("admin.events.form.publicationDraft")}</option>
                <option value="published">{t("admin.events.form.publicationPublished")}</option>
              </select>
            </Field>
            <Field label={t("admin.events.form.salesStatus")} htmlFor="event-sales-status">
              <select id="event-sales-status" value={form.salesStatus} onChange={(event) => onField("salesStatus", event.target.value as EventFormState["salesStatus"])} className={inputClass}>
                <option value="scheduled">{t("admin.events.form.salesScheduled")}</option>
                <option value="selling">{t("admin.events.form.salesSelling")}</option>
                <option value="paused">{t("admin.events.form.salesPaused")}</option>
                <option value="ended">{t("admin.events.form.salesEnded")}</option>
                <option value="cancelled">{t("admin.events.form.salesCancelled")}</option>
              </select>
            </Field>
          </div>
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <Field label={t("admin.events.form.salesStartAt")} htmlFor="event-sales-start" error={errors.salesStartAt ? t(errors.salesStartAt) : undefined}>
              <input id="event-sales-start" type="datetime-local" step="0.001" value={form.salesStartAt} onChange={(event) => onField("salesStartAt", event.target.value)} onBlur={() => onFieldBlur("salesStartAt")} className={inputClass} {...fieldAria("event-sales-start", errors.salesStartAt)} />
            </Field>
            <Field label={t("admin.events.form.salesEndAt")} htmlFor="event-sales-end" error={errors.salesEndAt ? t(errors.salesEndAt) : undefined}>
              <input id="event-sales-end" type="datetime-local" step="0.001" value={form.salesEndAt} onChange={(event) => onField("salesEndAt", event.target.value)} onBlur={() => onFieldBlur("salesEndAt")} className={inputClass} {...fieldAria("event-sales-end", errors.salesEndAt)} />
            </Field>
          </div>
          {errors.salesWindow && <p role="alert" className="text-[11px] text-danger-ink">{t(errors.salesWindow)}</p>}
          <p className="rounded-lg border border-border bg-tag-bg px-3 py-2 text-[11px] leading-5 text-muted">{t("admin.events.form.salesLifecycleNotice")}</p>
          {createRetryLocked && <p role="status" className="rounded-lg border border-border bg-tag-bg px-3 py-2 text-[11px] leading-5 text-muted">{t("admin.events.form.createRetryLocked")}</p>}

          {editing ? (
            <div className="border border-border rounded-lg p-3">
              <div className="text-[11px] font-semibold uppercase tracking-wider text-muted mb-2">
                {t("admin.events.form.ticketTiersReadOnly")}
              </div>
              {editing.tiers.length === 0 ? (
                <p className="text-xs text-muted">{t("admin.events.form.noTiers")}</p>
              ) : (
                <ul className="flex flex-col gap-1.5">
                  {editing.tiers.map((tier) => (
                    <li
                      key={tier.id}
                      className="flex items-center justify-between gap-3 text-[13px]"
                    >
                      <span>{tier.name}</span>
                      <span className="text-muted tabular-nums">
                        {usdCents(tier.price_cents)} ·{" "}
                        {t("admin.events.form.availableOf", { available: number(tier.available_quantity), total: number(tier.total_quantity) })}
                      </span>
                    </li>
                  ))}
                </ul>
              )}
              <p className="text-[11px] text-muted mt-2">
                {t("admin.events.form.tierEditNotExposed")}
              </p>
            </div>
          ) : (
            <div className="flex flex-col gap-2">
              <div className="flex items-center justify-between">
                <span className="text-[11px] font-semibold uppercase tracking-wider text-muted">
                  {t("admin.events.form.ticketTiers")}
                </span>
                <button
                  type="button"
                  onClick={onAddTier}
                  className="min-h-11 rounded-lg px-3 text-xs font-semibold text-foreground underline underline-offset-2"
                >
                  {t("admin.events.form.addTier")}
                </button>
              </div>
              {form.tiers.map((tier, index) => {
                const rowErrors = errors.tierRows?.[index] ?? {};
                return (
                  <div
                    key={index}
                    className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_110px_110px_44px] sm:items-start"
                  >
                    <div className="flex flex-col gap-1">
                      <input
                        value={tier.name}
                        onChange={(event) =>
                          onTier(index, "name", event.target.value)
                        }
                        onBlur={() => onTierBlur(index, "name")}
                        placeholder={t("admin.events.form.tierNamePlaceholder")}
                        className={inputClass}
                        aria-label={t("admin.events.form.tierNameAria", { index: number(index + 1) })}
                        {...fieldAria(`tier-${index}-name`, rowErrors.name)}
                      />
                      {rowErrors.name && (
                        <p
                          id={`tier-${index}-name-error`}
                          className="text-[11px] text-danger-ink"
                          role="alert"
                        >
                          {t(rowErrors.name)}
                        </p>
                      )}
                    </div>
                    <div className="flex flex-col gap-1">
                      <input
                        value={tier.price}
                        onChange={(event) =>
                          onTier(index, "price", event.target.value)
                        }
                        onBlur={() => onTierBlur(index, "price")}
                        placeholder={t("admin.events.form.pricePlaceholder")}
                        inputMode="decimal"
                        className={inputClass}
                        aria-label={t("admin.events.form.tierPriceAria", { index: number(index + 1) })}
                        {...fieldAria(`tier-${index}-price`, rowErrors.price)}
                      />
                      {rowErrors.price && (
                        <p
                          id={`tier-${index}-price-error`}
                          className="text-[11px] text-danger-ink"
                          role="alert"
                        >
                          {t(rowErrors.price)}
                        </p>
                      )}
                    </div>
                    <div className="flex flex-col gap-1">
                      <input
                        value={tier.quantity}
                        onChange={(event) =>
                          onTier(index, "quantity", event.target.value)
                        }
                        onBlur={() => onTierBlur(index, "quantity")}
                        placeholder={t("admin.events.form.quantityPlaceholder")}
                        inputMode="numeric"
                        className={inputClass}
                        aria-label={t("admin.events.form.tierQuantityAria", { index: number(index + 1) })}
                        {...fieldAria(
                          `tier-${index}-quantity`,
                          rowErrors.quantity,
                        )}
                      />
                      {rowErrors.quantity && (
                        <p
                          id={`tier-${index}-quantity-error`}
                          className="text-[11px] text-danger-ink"
                          role="alert"
                        >
                          {t(rowErrors.quantity)}
                        </p>
                      )}
                    </div>
                    <button
                      type="button"
                      onClick={() => onRemoveTier(index)}
                      disabled={form.tiers.length === 1}
                      className="grid h-11 w-11 justify-self-end place-items-center rounded-lg text-muted hover:bg-tag-bg hover:text-foreground disabled:opacity-30 sm:justify-self-auto"
                      aria-label={t("admin.events.form.removeTierAria", { index: number(index + 1) })}
                    >
                      <X size={14} />
                    </button>
                  </div>
                );
              })}
              {errors.tiers && (
                <p className="text-[11px] text-danger-ink" role="alert">
                  {t(errors.tiers)}
                </p>
              )}
              <p className="text-[11px] text-muted">
                {t("admin.events.form.pricesStoredCents")}
              </p>
            </div>
          )}

          {submitError && (
            <div
              role="alert"
              className="flex items-start gap-2 bg-danger-bg border border-danger/30 rounded-lg px-3 py-2 text-[12px] text-danger-ink"
            >
              <AlertCircle size={14} className="mt-0.5 shrink-0" />
              <span>{t(submitError)}</span>
            </div>
          )}
        </fieldset>

        <div className="sticky bottom-0 mt-auto flex items-center justify-end gap-2 border-t border-border bg-card p-3 sm:p-5">
          <button
            type="button"
            onClick={onClose}
            disabled={submitting}
            className="h-11 px-4 rounded-full text-[13px] font-medium text-muted hover:bg-tag-bg transition-colors disabled:opacity-40"
          >
            {t("admin.events.form.cancel")}
          </button>
          <button
            type="button"
            onClick={onSubmit}
            disabled={submitting}
            aria-busy={submitting}
            className="h-11 px-5 rounded-full bg-primary hover:bg-primary-hover text-graphite text-[13px] font-semibold transition-colors disabled:opacity-60 inline-flex items-center gap-2"
          >
            {submitting && <Loader2 size={14} className="animate-spin" />}
            {editing ? t("admin.events.form.saveChanges") : createRetryLocked ? t("admin.events.form.retryCreate") : t("admin.events.form.createEvent")}
          </button>
        </div>
      </aside>
    </div>
  );
}
