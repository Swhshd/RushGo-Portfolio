"use client";

import { useParams, useRouter } from "next/navigation";
import Link from "next/link";
import { useEffect, useState } from "react";
import { Calendar, MapPin, Music, Ticket, ChevronRight, Loader2 } from "lucide-react";
import { useBooking } from "@/lib/booking-context";
import { EventInterestsPanel } from "@/components/event-interests";
import { EventArtwork } from "@/components/event-artwork";
import { useI18n, customerCategoryTranslationKey } from "@/lib/i18n/index";

export default function EventDetailPage() {
  const { t, date, number, usdCents } = useI18n();
  const { id } = useParams<{ id: string }>();
  const router = useRouter();
  const { getEvent, ensureEvent, isLoading } = useBooking();
  const event = getEvent(id);
  const [loadErrorId, setLoadErrorId] = useState<string | null>(null);
  const loadError = loadErrorId === id;

  useEffect(() => {
    if (event) return;
    let active = true;
    void ensureEvent(id).catch(() => { if (active) setLoadErrorId(id); });
    return () => { active = false; };
  }, [ensureEvent, event, id]);

  if (isLoading || (!event && !loadError)) {
    return (
      <div className="flex items-center justify-center h-96">
        <Loader2 size={32} className="animate-spin text-primary" />
      </div>
    );
  }

  if (!event) {
    return (
      <div className="flex flex-col items-center justify-center gap-4 h-96 px-5 text-center">
        <p role={loadError ? "alert" : "status"} className="text-muted text-lg">{t("customer.event.notFound")}</p>
        {loadError && <button type="button" onClick={() => { setLoadErrorId(null); void ensureEvent(id, true).catch(() => setLoadErrorId(id)); }} className="min-h-11 rounded-lg border border-border px-5 font-semibold">{t("customer.home.tryAgain")}</button>}
      </div>
    );
  }

  // A tier is purchasable only when the server has projected it as ready and
  // its current inventory is positive. Unknown readiness always fails closed.
  const hasSellableTier = event.tiers.some(
    (tier) => tier.accountingReady === true && tier.available > 0,
  );
  const hasUnreadyTier = event.tiers.some((tier) => tier.accountingReady !== true);
  const hasReadyTier = event.tiers.some((tier) => tier.accountingReady === true);
  const availabilityLabel = hasSellableTier
    ? t("common.status.available")
    : hasUnreadyTier
      ? t("common.status.temporarilyUnavailable")
      : hasReadyTier
        ? t("common.status.soldOut")
        : t("common.status.unavailable");
  const emptyStateMessage = !hasReadyTier
    ? t("customer.event.noTiersOnSale")
    : hasUnreadyTier
      ? t("customer.event.noTiersAvailableForPurchase")
      : t("customer.event.allTiersSoldOut");
  const eventDate = event.startsAt
    ? date(event.startsAt, { month: "short", day: "numeric", year: "numeric" })
    : event.date;
  const eventTime = event.startsAt
    ? date(event.startsAt, { hour: "numeric", minute: "2-digit" })
    : event.time;
  const categoryKey = customerCategoryTranslationKey(event.category);
  const tierAvailability = (description: string) => {
    const match = /^(\d+) of (\d+) remaining$/.exec(description);
    return match
      ? t("customer.event.tierAvailability", { available: number(Number(match[1])), total: number(Number(match[2])) })
      : description;
  };

  return (
    <div className="flex flex-col">
      {/* Banner */}
      <div className="relative h-[280px] overflow-hidden">
        <EventArtwork src={event.image} title={event.title} category={event.category} />
        <div className="absolute inset-0 event-hero-overlay" />
        <div className="absolute inset-0 flex flex-col justify-end px-5 py-8 md:p-12 gap-3">
          <div className="flex items-center gap-2 text-white/70 text-sm">
            <Link href="/" className="hover:text-white">{t("customer.event.home")}</Link>
            <ChevronRight size={14} />
            <Link href={`/?category=${event.category.toLowerCase()}`} className="hover:text-white">{categoryKey ? t(categoryKey) : event.category}</Link>
            <ChevronRight size={14} />
            <span className="text-white">{event.title.split("—")[0].trim()}</span>
          </div>
          <h1 className="text-white text-3xl font-bold leading-tight">{event.title}</h1>
          <div className="flex flex-wrap items-center gap-3 md:gap-4 text-muted-on-dark text-sm">
            <span>{eventDate} · {eventTime}</span>
            <span>{event.venue}, {event.location}</span>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex flex-col lg:flex-row gap-8 px-5 md:px-12 py-8">
        {/* Left — About */}
        <div className="flex-1 flex flex-col gap-6">
          <EventInterestsPanel key={id} eventId={id} />
          <div className="bg-card border border-border rounded-lg p-6 md:p-8 shadow-sm flex flex-col gap-6">
            <h2 className="text-lg font-bold">{t("customer.event.about")}</h2>
            <p className="text-muted text-sm leading-relaxed">{event.description}</p>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 pt-4 border-t border-border">
              <div className="flex flex-col gap-1">
                <div className="flex items-center gap-1.5 text-muted text-xs font-medium">
                  <Calendar size={12} /> {t("customer.event.dateTime")}
                </div>
                <p className="text-sm font-medium">{eventDate} · {eventTime}</p>
              </div>
              <div className="flex flex-col gap-1">
                <div className="flex items-center gap-1.5 text-muted text-xs font-medium">
                  <MapPin size={12} /> {t("customer.event.venue")}
                </div>
                <p className="text-sm font-medium">{event.venue}, {event.location}</p>
              </div>
              <div className="flex flex-col gap-1">
                <div className="flex items-center gap-1.5 text-muted text-xs font-medium">
                  <Music size={12} /> {t("customer.event.genre")}
                </div>
                <p className="text-sm font-medium">{event.genre}</p>
              </div>
            </div>
          </div>
        </div>

        {/* Right — Ticket Selection */}
        <div className="w-full lg:w-[400px] shrink-0">
          <div className="bg-card border border-border rounded-lg shadow-sm flex flex-col">
            <div className="flex items-center justify-between p-6 border-b border-border">
              <h2 className="text-lg font-bold">{t("customer.event.ticketTiers")}</h2>
              {hasSellableTier ? (
                <span className="text-xs px-2 py-1 rounded-sm bg-success-bg text-success-ink font-medium">
                  {t("common.status.available")}
                </span>
              ) : (
                <span className="text-xs px-2 py-1 rounded-sm bg-tag-bg text-muted font-medium">
                  {availabilityLabel}
                </span>
              )}
            </div>

            <div className="flex flex-col p-6 gap-4">
              {event.tiers.map((tier) => (
                <div
                  key={tier.id}
                  className="flex items-center justify-between p-4 rounded-lg border border-border text-left"
                >
                  <div className="flex flex-col gap-0.5">
                    <span className="font-semibold text-sm">{tier.name}</span>
                    <span className="text-xs text-muted">{tierAvailability(tier.description)}</span>
                    {tier.accountingReady !== true ? (
                      <span className="text-xs text-muted mt-1">
                        {t("customer.event.accountingPending")}
                      </span>
                    ) : tier.available > 0 ? (
                      <span className="text-xs text-muted mt-1">
                        {t(tier.available === 1 ? "customer.event.ticketLeftOne" : "customer.event.ticketsLeftMany", { count: number(tier.available) })}
                      </span>
                    ) : (
                      <span className="text-xs text-muted mt-1">
                        {t("common.status.soldOut")}
                      </span>
                    )}
                  </div>
                  <span className="text-lg font-bold text-primary">{usdCents(Math.round(tier.price * 100))}</span>
                </div>
              ))}

              {hasSellableTier ? (
                <button
                  onClick={() => router.push(`/events/${event.id}/seats`)}
                  className="flex items-center justify-center gap-2 bg-primary hover:bg-primary-hover text-graphite font-semibold rounded-lg h-12 transition-colors mt-2"
                >
                  <Ticket size={16} />
                  {t("customer.event.selectSeats")}
                </button>
              ) : (
                <p role="status" className="text-sm text-muted mt-2">
                  {emptyStateMessage}
                </p>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
