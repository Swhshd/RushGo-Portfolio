"use client";

import Link from "next/link";
import { Calendar, Plus } from "lucide-react";
import { Event } from "@/lib/types";
import { EventArtwork } from "@/components/event-artwork";
import { useI18n, customerCategoryTranslationKey } from "@/lib/i18n/index";

export function EventCard({ event }: { event: Event }) {
  const { t, date, usdCents } = useI18n();
  const sellableTiers = event.tiers.filter(
    (tier) => tier.accountingReady === true && tier.available > 0,
  );
  const lowestPrice = sellableTiers.length ? Math.min(...sellableTiers.map((tier) => tier.price)) : null;
  const hasSellableTier = sellableTiers.length > 0;
  const hasUnreadyTier = event.tiers.some((tier) => tier.accountingReady !== true);
  const hasReadyTier = event.tiers.some((tier) => tier.accountingReady === true);

  const eventDate = event.startsAt
    ? date(event.startsAt, { month: "short", day: "numeric", year: "numeric" })
    : event.date;
  const categoryKey = customerCategoryTranslationKey(event.category);

  return (
    <Link href={`/events/${event.id}`} className="group flex-1 min-w-0">
      <div className="h-full bg-tag-bg border border-border rounded-lg overflow-hidden group-hover:border-muted group-hover:-translate-y-0.5 transition-[border-color,transform]">
        <div className="relative aspect-[16/9] overflow-hidden">
          <EventArtwork
            src={event.image}
            title={event.title}
            category={event.category}
          />
        </div>
        <div className="p-5 flex flex-col gap-3">
          <h3 className="font-semibold text-base leading-snug line-clamp-2">
            {event.title}
          </h3>
          <div className="flex items-center gap-1 text-muted text-xs">
            <Calendar size={12} />
            <span>
              {eventDate} · {event.venue}, {event.location}
            </span>
          </div>
          <div className="flex items-center justify-between">
            {hasSellableTier ? (
              <div className="flex items-center gap-1">
                <Plus size={12} className="text-primary" />
                <span className="text-sm font-semibold text-primary">{lowestPrice === null ? t("customer.eventCard.viewEvent") : t("customer.eventCard.fromPrice", { price: usdCents(Math.round(lowestPrice * 100)) })}</span>
              </div>
            ) : hasUnreadyTier ? (
              <span className="text-sm font-semibold text-muted">{t("common.status.temporarilyUnavailable")}</span>
            ) : hasReadyTier ? (
              <span className="text-sm font-semibold text-muted">{t("common.status.soldOut")}</span>
            ) : (
              <span className="text-sm font-semibold text-muted">{t("common.status.unavailable")}</span>
            )}
            <span
              className="text-xs px-2 py-1 rounded-sm font-medium bg-tag-bg text-muted"
            >
              {categoryKey ? t(categoryKey) : event.category}
            </span>
          </div>
        </div>
      </div>
    </Link>
  );
}
