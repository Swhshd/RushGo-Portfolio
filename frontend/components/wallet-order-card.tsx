import { useCallback, useState } from "react";
import Link from "next/link";
import {
  AlertCircle,
  CalendarDays,
  ChevronDown,
  ChevronUp,
  Download,
  MapPin,
  Ticket,
} from "lucide-react";
import { EventArtwork } from "@/components/event-artwork";
import { WalletRefundPanel } from "@/components/wallet-refund-panel";
import { PendingBookingActions } from "@/components/pending-booking-actions";
import { walletOrderHref, type WalletOrder, type WalletTab } from "@/lib/wallet";
import type { BuyerRefundEntryAccess } from "@/lib/booking-refunds";
import { refundOrderSummary, type RefundOrderSummaryUpdate } from "@/lib/refund-presentation";
import {
  buildCalendarInvite,
  calendarFileName,
  formatUsdCents,
  formatWalletDate,
  walletEventLocation,
} from "@/lib/wallet";
import { useI18n, type TranslationKey } from "@/lib/i18n/index";

const STATUS_CLASSES: Record<string, string> = {
  success: "bg-success-bg text-success-ink",
  pending: "bg-tag-bg text-primary",
  danger: "bg-danger-bg text-danger-ink",
  muted: "bg-tag-bg text-muted",
};

export function WalletOrderCard({
  userId,
  order,
  expanded,
  onToggle,
  hasIssuedTicket,
  hasCheckedInTicket,
  ticketsKnown,
  hasAnyTicket,
  onRefundAccessChange,
  onTicketsChanged,
  onBookingChanged,
  tab = "all",
}: {
  userId: string;
  order: WalletOrder;
  expanded: boolean;
  onToggle: () => void;
  hasIssuedTicket: boolean;
  hasCheckedInTicket: boolean;
  ticketsKnown: boolean;
  hasAnyTicket: boolean;
  onRefundAccessChange: (bookingId: string, access: BuyerRefundEntryAccess) => void;
  onTicketsChanged?: () => void;
  onBookingChanged?: () => void;
  tab?: WalletTab;
}) {
  const { locale, t, number } = useI18n();
  const detailsId = `wallet-order-${order.id}-details`;
  const titleId = `wallet-order-${order.id}-title`;
  const event = order.event;
  const location = walletEventLocation(event);
  const eventTitle = event === undefined
    ? t("customer.wallet.loadingEventDetails")
    : event === null
      ? t("customer.wallet.eventTitleUnavailable")
      : event.title || t("customer.wallet.untitledEvent");
  const statusKeys: Record<typeof order.status, TranslationKey> = {
    PENDING: "customer.wallet.statusPending",
    CONFIRMED: "customer.wallet.statusConfirmed",
    FAILED: "customer.wallet.statusFailed",
    CANCELLED: "customer.wallet.statusCancelled",
    EXPIRED: "customer.wallet.statusExpired",
    UNKNOWN: "customer.wallet.statusUnknown",
  };
  const statusLabel = t(statusKeys[order.status]);
  const [refundSummaryUpdate, setRefundSummaryUpdate] = useState<RefundOrderSummaryUpdate>("loading");
  const refundSummary = refundOrderSummary(refundSummaryUpdate);
  const quantityKey: TranslationKey = order.isConfirmedBooking
    ? order.quantity === 1 ? "customer.wallet.seatsCountOne" : "customer.wallet.seatsCountMany"
    : order.quantity === 1 ? "customer.wallet.itemsCountOne" : "customer.wallet.itemsCountMany";
  const quantityLabel = t(quantityKey, { count: number(order.quantity) });
  const displayTierName = (item: WalletOrder["items"][number]) =>
    item.tierNameIsFallback
      ? t("customer.wallet.tierDetailsUnavailable")
      : item.tierName;
  const invite = buildCalendarInvite(order);
  const notifyRefundAccessChange = useCallback(
    (access: BuyerRefundEntryAccess) => onRefundAccessChange(order.id, access),
    [onRefundAccessChange, order.id],
  );
  const notifyRefundSummaryChange = useCallback(
    (summary: RefundOrderSummaryUpdate) => setRefundSummaryUpdate(summary),
    [],
  );

  function downloadCalendar() {
    if (!invite) return;
    const blob = new Blob([invite], { type: "text/calendar;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = calendarFileName(order);
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  return (
    <article
      className="overflow-hidden rounded-xl border border-border bg-card"
      aria-labelledby={titleId}
    >
      <div className="flex flex-col sm:flex-row">
        <div className="h-40 w-full shrink-0 overflow-hidden bg-tag-bg sm:h-auto sm:w-40">
          {event ? (
            <EventArtwork
              src={event.image_url}
              title={event.title}
              category={event.category}
            />
          ) : (
            <div
              className="grid h-full w-full place-items-center"
              aria-hidden="true"
            >
              <Ticket size={28} className="text-muted" strokeWidth={1.5} />
            </div>
          )}
        </div>

        <div className="flex min-w-0 flex-1 flex-col gap-2 p-4">
          <div className="flex flex-wrap items-start justify-between gap-2">
            <div className="min-w-0">
              <h2
                id={titleId}
                className="truncate text-[15px] font-semibold text-foreground"
              >
                {eventTitle}
              </h2>
              {event ? (
                <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted">
                  <span className="inline-flex items-center gap-1">
                    <CalendarDays size={12} />
                    {formatWalletDate(event.date, locale)}
                  </span>
                  {location ? (
                    <span className="inline-flex items-center gap-1">
                      <MapPin size={12} />
                      {location}
                    </span>
                  ) : null}
                </div>
              ) : event === null ? (
                <p className="mt-1 text-xs text-muted">
                  {t("customer.wallet.orderEventUnavailable")}
                </p>
              ) : (
                <p className="mt-1 text-xs text-muted">
                  {t("customer.wallet.loadingEventDetails")}
                </p>
              )}
            </div>
            <div className="flex max-w-full shrink-0 flex-col items-end gap-1.5">
              <span className="flex flex-wrap items-center justify-end gap-1.5 text-right">
                <span className="text-[10px] font-medium text-muted">{t("aftersales.bookingStatusLabel")}:</span>
                <span
                  className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-[3px] text-xs font-semibold ${
                    STATUS_CLASSES[order.statusTone] ?? STATUS_CLASSES.muted
                  }`}
                >
                  <span className="h-1.5 w-1.5 rounded-full bg-current" />
                  {statusLabel}
                </span>
              </span>
              {order.isConfirmedBooking && ticketsKnown && !hasAnyTicket ? (
                <span
                  role="status"
                  className="max-w-full rounded-full bg-tag-bg px-2.5 py-1 text-right text-xs font-semibold leading-snug text-primary whitespace-normal"
                >
                  {t("customer.wallet.issuancePending")}
                </span>
              ) : null}
              {order.isConfirmedBooking && refundSummary ? (
                <span
                  role="status"
                  className={`max-w-full rounded-full px-2.5 py-1 text-right text-xs font-semibold leading-snug whitespace-normal ${STATUS_CLASSES[refundSummary.tone]}`}
                >
                  {t("aftersales.refundLabel", { status: t(refundSummary.key) })}
                </span>
              ) : null}
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[13px] text-muted">
            <span>
              {quantityLabel}
            </span>
            <span className="truncate">
              {order.items.length > 0
                ? order.items.map(displayTierName).join(", ")
                : t("customer.wallet.noTicketItems")}
            </span>
            <span className="font-mono tabular-nums text-foreground">
              {formatUsdCents(order.totalCents, locale)}
            </span>
          </div>

          <div className="mt-auto flex flex-wrap items-center justify-between gap-2 pt-2">
            <span className="font-mono text-[11px] text-muted">
              {order.id.slice(0, 8)}
            </span>
            <div className="flex flex-wrap items-center gap-2">
              <Link href={walletOrderHref(order.id, tab)} className="inline-flex h-[32px] items-center rounded-full border border-border px-3 text-xs font-medium text-primary transition-colors hover:bg-tag-bg">
                {t("customer.wallet.viewOrderDetails")}
              </Link>
              <button
                type="button"
                onClick={onToggle}
              aria-expanded={expanded}
              aria-controls={detailsId}
              className="inline-flex h-[32px] items-center gap-1 rounded-full border border-border px-3 text-xs font-medium text-muted transition-colors hover:bg-tag-bg hover:text-foreground"
            >
              {expanded ? <ChevronUp size={13} /> : <ChevronDown size={13} />}
                {expanded ? t("customer.wallet.hideDetails") : t("aftersales.orderDetails")}
              </button>
            </div>
          </div>
          {order.isConfirmedBooking && !expanded ? <p className="text-xs text-muted">{t("aftersales.orderHint")}</p> : null}
        </div>
      </div>

      <div
        id={detailsId}
        role="region"
        hidden={!expanded}
        aria-label={t("customer.wallet.orderDetailsAria", { event: eventTitle })}
        className="border-t border-border bg-background/60 p-4 text-[13px]"
      >
          <div className="flex flex-wrap items-center gap-x-6 gap-y-1 text-xs text-muted">
            <span>
              {t("customer.wallet.booking")} <code className="font-mono">{order.id}</code>
            </span>
            <span>{t("customer.wallet.created", { date: formatWalletDate(order.createdAt, locale) })}</span>
            {order.status === "UNKNOWN" && order.rawStatus ? (
              <span>{t("customer.wallet.apiStatus", { status: order.rawStatus })}</span>
            ) : null}
          </div>

          {order.status === "PENDING" ? (
            <PendingBookingActions
              bookingId={order.id}
              bookingStatus={order.rawStatus}
              onBookingChanged={() => onBookingChanged?.()}
            />
          ) : null}
          <div className="mt-3 overflow-x-auto">
            <table className="w-full min-w-[420px] border-collapse text-left">
              <thead>
                <tr className="text-[11px] uppercase tracking-wider text-muted">
                  <th
                    scope="col"
                    className="border-b border-border py-2 pr-3 font-semibold"
                  >
                    {t("customer.wallet.tier")}
                  </th>
                  <th
                    scope="col"
                    className="border-b border-border py-2 pr-3 font-semibold"
                  >
                    {t("customer.wallet.quantity")}
                  </th>
                  <th
                    scope="col"
                    className="border-b border-border py-2 pr-3 font-semibold"
                  >
                    {t("customer.wallet.unitPrice")}
                  </th>
                  <th
                    scope="col"
                    className="border-b border-border py-2 font-semibold"
                  >
                    {t("customer.wallet.seats")}
                  </th>
                </tr>
              </thead>
              <tbody>
                {order.items.map((item) => (
                  <tr key={item.id}>
                    <td className="border-b border-border py-2 pr-3">
                      {displayTierName(item)}
                    </td>
                    <td className="border-b border-border py-2 pr-3 tabular-nums">
                      {item.quantity}
                    </td>
                    <td className="border-b border-border py-2 pr-3 font-mono tabular-nums">
                        {formatUsdCents(item.unitPriceCents, locale)}
                    </td>
                    <td className="border-b border-border py-2">
                      {item.seatIds.length > 0 ? (
                        <span className="font-mono text-[11px] text-muted">
                          {item.seatIds.join(", ")}
                        </span>
                      ) : (
                        <span className="text-muted">{t("customer.wallet.noSeatsAssigned")}</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
            <div className="text-xs text-muted">
              {order.seatIds.length > 0
                ? t(order.seatIds.length === 1 ? "customer.wallet.seatIdAssigned" : "customer.wallet.seatIdsAssigned", { count: number(order.seatIds.length) })
                : t("customer.wallet.noSeatIdsAssigned")}
            </div>
            <div className="flex flex-wrap items-center gap-2">
              {invite && (
                <button
                  type="button"
                  onClick={downloadCalendar}
                  className="inline-flex h-[32px] items-center gap-1.5 rounded-full border border-border px-3 text-xs font-medium text-muted transition-colors hover:bg-tag-bg hover:text-foreground"
                >
                  <Download size={13} />
                  {t("customer.wallet.addToCalendar")}
                </button>
              )}
              <span className="font-mono text-sm tabular-nums text-foreground">
                  {t("customer.wallet.totalAmount", { amount: formatUsdCents(order.totalCents, locale) })}
              </span>
            </div>
          </div>

          <p className="mt-3 flex items-start gap-2 text-[11px] text-muted">
            <AlertCircle size={13} className="mt-0.5 shrink-0" />
            {order.isConfirmedBooking ? (
              <span>
                  {t(hasAnyTicket
                    ? "aftersales.issuedOrderHint"
                    : ticketsKnown
                      ? "customer.wallet.issuancePendingHint"
                      : "customer.wallet.confirmedBookingRecord")}
              </span>
            ) : (
              <span>
                  {t("customer.wallet.unconfirmedBookingRecord")}
              </span>
            )}
          </p>
          {order.isConfirmedBooking ? (
            <WalletRefundPanel
              userId={userId}
              bookingId={order.id}
              bookingStatus={order.status}
              amountCents={order.totalCents}
              hasIssuedTicket={hasIssuedTicket}
              hasCheckedInTicket={hasCheckedInTicket}
              onRefundAccessChange={notifyRefundAccessChange}
              onRefundSummaryChange={notifyRefundSummaryChange}
              onTicketsChanged={onTicketsChanged}
            />
          ) : null}
        </div>
    </article>
  );
}
