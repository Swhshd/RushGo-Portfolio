"use client";

import { Suspense, useEffect, useMemo, useState, type FormEvent } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import Link from "next/link";
import { Calendar, Loader2, MapPin, Ticket } from "lucide-react";
import { discoveryCategories, discoveryHref, discoveryStateForFeature, loadDiscoveryPage, readDiscovery, type DiscoveryPage, type DiscoveryState, type DiscoverySort } from "@/lib/discovery";
import type { Event } from "@/lib/types";
import { EventCard } from "@/components/event-card";
import { EventArtwork } from "@/components/event-artwork";
import { apiClient } from "@/lib/api/client";
import { useI18n, customerCategoryTranslationKey } from "@/lib/i18n/index";

export default function HomePage() {
  return <Suspense fallback={<DiscoveryLoading />}><DiscoveryPage /></Suspense>;
}

function DiscoveryLoading() {
  const { t } = useI18n();
  return <div role="status" aria-label={t("customer.home.loadingEvents")} className="flex items-center justify-center h-96"><Loader2 size={32} className="animate-spin text-primary" /></div>;
}

function AdvancedDiscoveryFilters({ state, onApply }: {
  state: DiscoveryState;
  onApply: (update: Partial<DiscoveryState>) => void;
}) {
  const { t } = useI18n();
  const [draft, setDraft] = useState(() => ({
    dateFrom: state.dateFrom,
    dateTo: state.dateTo,
    location: state.location,
    minPrice: state.minPriceCents === undefined ? "" : String(state.minPriceCents),
    maxPrice: state.maxPriceCents === undefined ? "" : String(state.maxPriceCents),
    sort: state.sort,
  }));
  const [filterError, setFilterError] = useState(false);

  function apply(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const min = draft.minPrice.trim() === "" ? undefined : Number(draft.minPrice);
    const max = draft.maxPrice.trim() === "" ? undefined : Number(draft.maxPrice);
    if ((min !== undefined && (!Number.isSafeInteger(min) || min < 0)) || (max !== undefined && (!Number.isSafeInteger(max) || max < 0)) || (min !== undefined && max !== undefined && min > max) || (draft.dateFrom && draft.dateTo && draft.dateFrom > draft.dateTo)) {
      setFilterError(true);
      return;
    }
    setFilterError(false);
    onApply({
      dateFrom: draft.dateFrom,
      dateTo: draft.dateTo,
      location: draft.location.trim(),
      minPriceCents: min,
      maxPriceCents: max,
      sort: draft.sort,
      page: 1,
    });
  }

  function clear() {
    setFilterError(false);
    setDraft({ dateFrom: "", dateTo: "", location: "", minPrice: "", maxPrice: "", sort: "date_asc" });
    onApply({ dateFrom: "", dateTo: "", location: "", minPriceCents: undefined, maxPriceCents: undefined, sort: "date_asc", page: 1 });
  }

  return <form onSubmit={apply} className="rounded-xl border border-border bg-card p-4 sm:p-5">
    <h3 className="text-sm font-semibold">{t("customer.home.advancedFilters")}</h3>
    <div className="mt-3 grid min-w-0 grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
      <label className="flex min-w-0 flex-col gap-1.5 text-xs font-medium text-muted">{t("customer.home.dateFrom")}<input type="date" value={draft.dateFrom} onChange={(event) => setDraft((current) => ({ ...current, dateFrom: event.target.value }))} className="h-11 min-w-0 rounded-lg border border-control-border bg-background px-3 text-sm text-foreground" /></label>
      <label className="flex min-w-0 flex-col gap-1.5 text-xs font-medium text-muted">{t("customer.home.dateTo")}<input type="date" value={draft.dateTo} onChange={(event) => setDraft((current) => ({ ...current, dateTo: event.target.value }))} className="h-11 min-w-0 rounded-lg border border-control-border bg-background px-3 text-sm text-foreground" /></label>
      <label className="flex min-w-0 flex-col gap-1.5 text-xs font-medium text-muted">{t("customer.home.locationFilter")}<input type="search" value={draft.location} onChange={(event) => setDraft((current) => ({ ...current, location: event.target.value }))} className="h-11 min-w-0 rounded-lg border border-control-border bg-background px-3 text-sm text-foreground" /></label>
      <label className="flex min-w-0 flex-col gap-1.5 text-xs font-medium text-muted">{t("customer.home.minPrice")}<input type="number" min="0" step="1" inputMode="numeric" value={draft.minPrice} onChange={(event) => setDraft((current) => ({ ...current, minPrice: event.target.value }))} className="h-11 min-w-0 rounded-lg border border-control-border bg-background px-3 text-sm text-foreground" /></label>
      <label className="flex min-w-0 flex-col gap-1.5 text-xs font-medium text-muted">{t("customer.home.maxPrice")}<input type="number" min="0" step="1" inputMode="numeric" value={draft.maxPrice} onChange={(event) => setDraft((current) => ({ ...current, maxPrice: event.target.value }))} className="h-11 min-w-0 rounded-lg border border-control-border bg-background px-3 text-sm text-foreground" /></label>
      <label className="flex min-w-0 flex-col gap-1.5 text-xs font-medium text-muted">{t("customer.home.sort")}<select value={draft.sort} onChange={(event) => setDraft((current) => ({ ...current, sort: event.target.value as DiscoverySort }))} className="h-11 min-w-0 rounded-lg border border-control-border bg-background px-3 text-sm text-foreground">
        <option value="date_asc">{t("customer.home.sortDateAsc")}</option><option value="date_desc">{t("customer.home.sortDateDesc")}</option><option value="price_asc">{t("customer.home.sortPriceAsc")}</option><option value="price_desc">{t("customer.home.sortPriceDesc")}</option>
      </select></label>
    </div>
    {filterError && <p role="alert" className="mt-3 text-xs text-danger-ink">{t("customer.home.filtersError")}</p>}
    <div className="mt-4 flex flex-wrap justify-end gap-2"><button type="button" onClick={clear} className="min-h-11 rounded-full border border-border px-4 text-xs font-semibold">{t("customer.home.clearFilters")}</button><button type="submit" className="min-h-11 rounded-full bg-primary px-5 text-xs font-semibold text-graphite">{t("customer.home.applyFilters")}</button></div>
  </form>;
}

function DiscoveryPage() {
  const { t, number, date, text } = useI18n();
  const params = useSearchParams();
  const router = useRouter();
  const [advancedAvailable, setAdvancedAvailable] = useState<boolean | null>(null);
  const searchString = params.toString();
  const state = useMemo(() => readDiscovery(new URLSearchParams(searchString)), [searchString]);
  const key = discoveryHref(state);
  const requestState: DiscoveryState = useMemo(() => discoveryStateForFeature(state, advancedAvailable === true), [advancedAvailable, state]);
  const requestKey = discoveryHref(requestState);
  const [result, setResult] = useState<{ key: string; page: DiscoveryPage } | { key: string; error: true } | null>(null);
  const [retry, setRetry] = useState(0);
  const [featureUnavailable, setFeatureUnavailable] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    void apiClient.getPublicFeatures(controller.signal).then((response) => {
      if (controller.signal.aborted) return;
      setFeatureUnavailable(response.source_state !== "available");
      setAdvancedAvailable(response.source_state === "available" && response.flags.discovery_advanced_filters === true);
    }).catch(() => {
      if (controller.signal.aborted) return;
      setFeatureUnavailable(true);
      setAdvancedAvailable(false);
    });
    return () => controller.abort();
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    void loadDiscoveryPage(requestState, controller.signal).then(
      (page) => { if (!controller.signal.aborted) setResult({ key: requestKey, page }); },
      () => { if (!controller.signal.aborted) setResult({ key: requestKey, error: true }); },
    );
    return () => controller.abort();
  }, [requestKey, requestState, retry]);

  const loading = !result || result.key !== requestKey;
  const page = !loading && result && "page" in result ? result.page : null;
  const events = page?.events ?? [];
  const featured = events.find((event) => event.featured) || events[0];
  const featuredCategoryKey = featured ? customerCategoryTranslationKey(featured.category) : null;
  const featuredCategory = featured
    ? featuredCategoryKey ? t(featuredCategoryKey) : featured.category
    : "";
  const featuredDate = featured
    ? featured.startsAt ? date(featured.startsAt, { month: "short", day: "numeric", year: "numeric" }) : featured.date
    : "";
  const featuredTime = featured
    ? featured.startsAt ? date(featured.startsAt, { hour: "numeric", minute: "2-digit" }) : featured.time
    : "";
  const totalPages = page ? Math.max(1, Math.ceil(page.total / Math.max(1, page.pageSize))) : 1;

  const discoveryFailed = !loading && result && "error" in result;

  return (
    <div className="flex min-w-0 flex-col">
      {featured && <div className="relative h-[380px] overflow-hidden bg-graphite">
        <EventArtwork src={featured.image} title={featured.title} category={featured.category} />
        <div className="absolute inset-0 event-hero-overlay" />
        <div className="absolute inset-0 flex flex-col justify-center gap-5 px-5 py-8 md:px-12">
          <div className="flex w-fit items-center gap-2 text-primary"><span className="h-1.5 w-1.5 rounded-full bg-primary" /><span className="text-xs font-mono font-medium uppercase tracking-widest">{t("customer.home.featured", { category: featuredCategory })}</span></div>
          <h1 className="max-w-2xl text-3xl font-bold leading-tight text-on-dark md:text-5xl">{featured.title}</h1>
          <div className="flex flex-col gap-3 text-sm text-muted-on-dark sm:flex-row sm:gap-6"><span className="flex items-center gap-2"><Calendar size={14} />{featuredDate}, {featuredTime}</span><span className="flex items-center gap-2"><MapPin size={14} />{featured.venue}, {featured.location}</span></div>
          <Link href={`/events/${featured.id}`} className="flex h-12 w-fit items-center gap-2 rounded-lg bg-primary px-6 font-semibold text-graphite transition-colors hover:bg-primary-hover"><Ticket size={16} />{t("customer.home.getTickets")}</Link>
        </div>
      </div>}

      <div className="flex min-w-0 flex-col gap-6 px-5 py-8 md:px-12">
        {featureUnavailable && <p role="status" className="rounded-lg border border-border bg-tag-bg p-3 text-xs leading-5 text-muted">{t("customer.home.advancedUnavailable")}</p>}
        <div className="flex flex-col items-start justify-between gap-4 sm:flex-row sm:items-center">
          <div className="flex flex-col gap-1"><h2 className="text-2xl font-bold">{state.search ? t("customer.home.searchResults") : t("customer.home.trendingEvents")}</h2>
            {page && !discoveryFailed && <p role="status" className="text-sm text-muted">{state.search
              ? t(page.total === 1 ? "customer.home.searchResultsCountOne" : "customer.home.searchResultsCountMany", { count: number(page.total), search: state.search })
              : t(page.total === 1 ? "customer.home.eventsToExploreOne" : "customer.home.eventsToExploreMany", { count: number(page.total) })}</p>}
          </div>
          <div aria-label={t("customer.home.categories")} className="flex max-w-full items-center gap-1 overflow-x-auto rounded-lg bg-tag-bg p-1">
            {discoveryCategories.map((item) => <Link key={item.label} href={discoveryHref(requestState, { category: item.category })} aria-current={requestState.category === item.category ? "page" : undefined} className={`whitespace-nowrap rounded-sm px-3 py-1.5 text-sm font-medium transition-colors md:px-4 ${requestState.category === item.category ? "bg-card font-semibold text-primary" : "text-muted hover:text-foreground"}`}>
              {item.category ? (customerCategoryTranslationKey(item.category) ? t(customerCategoryTranslationKey(item.category)!) : item.label) : t("customer.home.allCategories")}
            </Link>)}
          </div>
        </div>

        {advancedAvailable && <AdvancedDiscoveryFilters key={key} state={state} onApply={(update) => router.push(discoveryHref(state, update))} />}

        {loading ? <DiscoveryLoading /> : discoveryFailed ? <div role="alert" className="flex flex-col items-center gap-4 rounded-lg border border-border bg-card p-10 text-center"><p className="text-muted">{text("Events could not be loaded. Please try again.")}</p><button type="button" onClick={() => { setResult(null); setRetry((value) => value + 1); }} className="min-h-11 rounded-lg border border-border bg-tag-bg px-5 font-medium">{t("customer.home.tryAgain")}</button></div> : events.length === 0 ? (
          <div className="flex flex-col items-center gap-3 rounded-lg border border-border bg-card px-6 py-16 text-center"><Ticket size={28} className="mb-1 text-muted" /><h3 className="text-xl font-semibold">{t("customer.home.noEvents")}</h3><p className="text-muted">{t("customer.home.tryAnotherSearch")}</p><Link href={requestState.search ? discoveryHref(requestState, { search: "" }) : "/"} className="mt-3 rounded-lg bg-primary px-5 py-3 font-semibold text-graphite">{requestState.search ? t("customer.home.clearSearch") : t("customer.home.discoverAll")}</Link></div>
        ) : <>
          <div className="grid min-w-0 grid-cols-1 gap-6 md:grid-cols-2 lg:grid-cols-3">{events.map((event: Event) => <EventCard key={event.id} event={event} />)}</div>
          {page && totalPages > 1 && <nav aria-label={t("customer.home.categories")} className="flex flex-wrap items-center justify-center gap-3 pt-2">
            <button type="button" onClick={() => router.push(discoveryHref(requestState, { page: page.page - 1 }))} disabled={page.page <= 1} className="min-h-11 rounded-full border border-border px-4 text-sm disabled:opacity-40">{t("customer.home.previousPage")}</button>
            <span className="text-xs text-muted">{t("customer.home.pageOf", { page: number(page.page), pages: number(totalPages) })}</span>
            <button type="button" onClick={() => router.push(discoveryHref(requestState, { page: page.page + 1 }))} disabled={page.page >= totalPages || page.page >= 1000} className="min-h-11 rounded-full border border-border px-4 text-sm disabled:opacity-40">{t("customer.home.nextPage")}</button>
          </nav>}
        </>}
      </div>
    </div>
  );
}
