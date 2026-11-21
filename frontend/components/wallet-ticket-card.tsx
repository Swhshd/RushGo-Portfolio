"use client";

import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";
import Link from "next/link";
import { QrCode, RefreshCw, TicketCheck } from "lucide-react";
import { QRCodeSVG } from "qrcode.react";
import { apiClient } from "@/lib/api/client";
import { ApiError } from "@/lib/api/errors";
import type { ApiTicket } from "@/lib/api/types";
import type { BuyerRefundEntryAccess } from "@/lib/booking-refunds";
import { canDisplayCredentialVersion, highestCredentialVersion, ticketCredentialVersionStorageKey } from "@/lib/ticket-credential-version";
import { getPendingRotationKeyServerSnapshot, getPendingRotationKeySnapshot, setPendingRotationKey, subscribePendingRotationKey } from "@/lib/ticket-credential-storage";
import { getTicketClockNow, getTicketClockServerSnapshot, getTicketClockSnapshot, refreshTicketClock, subscribeTicketClock } from "@/lib/ticket-clock";
import { walletOrderHref, type WalletTab } from "@/lib/wallet";
import { useI18n, type TranslationKey } from "@/lib/i18n/index";

type Pass = { token: string; expiresAt: number; credentialVersion: number };
type CredentialVersionMessage = { ticketId: string; credentialVersion: number };
type WalletTicketCardProps = { ticket: ApiTicket; eventTitle: string; eventDate?: string; refundAccess: BuyerRefundEntryAccess; tab?: WalletTab };

export function WalletTicketCard(props: WalletTicketCardProps) {
  const { ticket, refundAccess } = props;
  const bookingStatus = typeof ticket.booking_status === "string" ? ticket.booking_status.toUpperCase() : "";
  const ticketEligible = ticket.status === "ISSUED" && bookingStatus === "CONFIRMED";
  const credentialEligible = ticketEligible && refundAccess === "ALLOWED";
  const subscribeToPendingKey = useCallback(
    (listener: () => void) => subscribePendingRotationKey(ticket.id, listener),
    [ticket.id],
  );
  const getPendingKey = useCallback(() => getPendingRotationKeySnapshot(ticket.id), [ticket.id]);
  const pendingKeySnapshot = useSyncExternalStore(
    subscribeToPendingKey,
    getPendingKey,
    getPendingRotationKeyServerSnapshot,
  );

  return (
    <WalletTicketCardView
      key={`${ticket.id}:${credentialEligible ? "eligible" : refundAccess.toLowerCase()}`}
      {...props}
      credentialEligible={credentialEligible}
      pendingKey={pendingKeySnapshot.key}
      pendingKeyLoaded={pendingKeySnapshot.loaded}
    />
  );
}

function WalletTicketCardView({
  ticket,
  eventTitle,
  eventDate,
  refundAccess,
  tab = "all",
  credentialEligible,
  pendingKey,
  pendingKeyLoaded,
}: WalletTicketCardProps & {
  refundAccess: BuyerRefundEntryAccess;
  credentialEligible: boolean;
  pendingKey: string | null;
  pendingKeyLoaded: boolean;
}) {
  const { t, text, number, date } = useI18n();
  const [pass, setPass] = useState<Pass | null>(null);
  const clockSubscriptionEnabled = credentialEligible && pass !== null;
  const subscribeToClock = useCallback(
    (listener: () => void) => clockSubscriptionEnabled ? subscribeTicketClock(listener) : () => {},
    [clockSubscriptionEnabled],
  );
  const getClockSnapshot = useCallback(
    () => clockSubscriptionEnabled ? getTicketClockSnapshot() : 0,
    [clockSubscriptionEnabled],
  );
  const now = useSyncExternalStore(subscribeToClock, getClockSnapshot, getTicketClockServerSnapshot);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [rotationBlocked, setRotationBlocked] = useState(false);
  const passRef = useRef<Pass | null>(null);
  const highestCredentialVersionRef = useRef(0);
  const versionChannelRef = useRef<BroadcastChannel | null>(null);
  const bookingStatus = typeof ticket.booking_status === "string" ? ticket.booking_status.toUpperCase() : "";
  const bookingConfirmed = bookingStatus === "CONFIRMED";
  const ticketEligible = ticket.status === "ISSUED" && bookingConfirmed;
  const refundBlocked = ticketEligible && refundAccess === "BLOCKED";
  const refundUnknown = ticketEligible && refundAccess === "UNKNOWN";
  const refundRestricted = refundBlocked || refundUnknown;
  const active = credentialEligible && !rotationBlocked && pass !== null && pass.expiresAt > now;
  const remaining = active ? Math.max(0, Math.ceil((pass.expiresAt - now) / 1000)) : 0;

  useEffect(() => {
    passRef.current = pass;
  }, [pass]);

  useEffect(() => {
    const versionStorage = ticketCredentialVersionStorageKey(ticket.id);
    try {
      const storedVersion = Number(window.localStorage.getItem(versionStorage));
      highestCredentialVersionRef.current = Number.isSafeInteger(storedVersion) && storedVersion > 0 ? storedVersion : 0;
    } catch {
      highestCredentialVersionRef.current = 0;
    }
  }, [ticket.id]);

  useEffect(() => {
    const versionStorage = ticketCredentialVersionStorageKey(ticket.id);
    const applyNewerVersion = (version: number) => {
      if (!Number.isSafeInteger(version) || version < 1) return;
      highestCredentialVersionRef.current = highestCredentialVersion(highestCredentialVersionRef.current, version);
      if (passRef.current && passRef.current.credentialVersion < highestCredentialVersionRef.current) {
        setNotice("A newer entry code was generated in another tab. This older code is hidden.");
      }
      setPass((current) => {
        if (current && current.credentialVersion < highestCredentialVersionRef.current) return null;
        return current;
      });
    };
    let channel: BroadcastChannel | null = null;
    try {
      if (typeof BroadcastChannel !== "undefined") channel = new BroadcastChannel("rushgo-ticket-credential-version");
    } catch {
      channel = null;
    }
    versionChannelRef.current = channel;
    if (channel) {
      channel.onmessage = (event: MessageEvent<CredentialVersionMessage>) => {
        if (event.data?.ticketId === ticket.id) applyNewerVersion(event.data.credentialVersion);
      };
    }
    const onStorage = (event: StorageEvent) => {
      if (event.key !== versionStorage || event.newValue === null) return;
      applyNewerVersion(Number(event.newValue));
    };
    window.addEventListener("storage", onStorage);
    return () => {
      window.removeEventListener("storage", onStorage);
      channel?.close();
      if (versionChannelRef.current === channel) versionChannelRef.current = null;
    };
  }, [ticket.id]);

  useEffect(() => {
    if (!pass) return;
    const timer = window.setInterval(() => {
      const current = getTicketClockNow();
      if (pass.expiresAt <= current) {
        setPass(null);
        setNotice("This entry code expired. Generate a new code when you are ready to enter.");
      }
    }, 500);
    return () => window.clearInterval(timer);
  }, [pass]);

  async function rotateCredential() {
    if (loading || !pendingKeyLoaded || !credentialEligible || rotationBlocked) return;
    const idempotencyKey = pendingKey ?? crypto.randomUUID();
    if (!pendingKey) {
      setPendingRotationKey(ticket.id, idempotencyKey);
    }
    // A network failure can happen after the server commits a rotation. Clear
    // the old value before the request so an ambiguous result cannot display a
    // credential that may already have been invalidated.
    setPass(null);
    setError(null);
    setNotice(null);
    setLoading(true);
    try {
      const response = await apiClient.rotateTicketCredential(ticket.id, idempotencyKey);
      const expiresAt = Date.parse(response.expires_at);
      if (response.outcome === "ALREADY_APPLIED") {
        if (!Number.isSafeInteger(response.credential_version) || response.credential_version < 1) {
          throw new Error("The ticket rotation receipt was incomplete.");
        }
        publishCredentialVersion(response.credential_version);
        setNotice("That request already completed, but its one-time code cannot be restored. Generate a new code only if you still need one.");
        clearPendingRotationKey();
        return;
      }
      if (response.outcome !== "ROTATED" || !response.credential || !Number.isFinite(expiresAt) || expiresAt <= getTicketClockNow() || !Number.isSafeInteger(response.credential_version) || response.credential_version < 1) {
        throw new Error("The ticket credential response was incomplete.");
      }
      const highestVersion = publishCredentialVersion(response.credential_version);
      if (!canDisplayCredentialVersion(highestVersion, response.credential_version)) {
        setNotice("A newer entry code was generated in another tab. This older response was discarded.");
        clearPendingRotationKey();
        return;
      }
      setPass({ token: response.credential, expiresAt, credentialVersion: response.credential_version });
      refreshTicketClock();
      clearPendingRotationKey();
    } catch (cause) {
      if (cause instanceof ApiError && cause.code === "TICKET_IDEMPOTENCY_CONFLICT") {
        clearPendingRotationKey();
        setError("This request key was already used for another action. Start a fresh code request if you still need one.");
      } else if (cause instanceof ApiError && cause.code === "TICKET_IDEMPOTENCY_IN_PROGRESS") {
        setError("The previous request is still processing. Check again with the same request key.");
      } else if (cause instanceof ApiError && cause.code === "TICKET_AUTHORITY_UNAVAILABLE") {
        setError("Ticket eligibility is temporarily unavailable. The code is hidden; retry this request with the same key later.");
      } else if (cause instanceof ApiError && cause.code === "TICKET_NOT_ELIGIBLE") {
        clearPendingRotationKey();
        setRotationBlocked(true);
        setError("This ticket can no longer create an entry code. Refresh your wallet to check its latest status.");
      } else {
        setError("A secure entry code could not be confirmed. Retry the same request before starting a new one.");
      }
    } finally {
      setLoading(false);
    }
  }

  function publishCredentialVersion(version: number): number {
    const versionStorage = ticketCredentialVersionStorageKey(ticket.id);
    let storedVersion = 0;
    try {
      storedVersion = Number(window.localStorage.getItem(versionStorage));
      if (!Number.isSafeInteger(storedVersion) || storedVersion < 1) storedVersion = 0;
    } catch {
      storedVersion = 0;
    }
    const highestVersion = Math.max(highestCredentialVersionRef.current, storedVersion, version);
    highestCredentialVersionRef.current = highestVersion;
    try {
      window.localStorage.setItem(versionStorage, String(highestVersion));
    } catch {
      // The in-memory version and BroadcastChannel still protect open tabs.
    }
    try {
      versionChannelRef.current?.postMessage({ ticketId: ticket.id, credentialVersion: highestVersion } satisfies CredentialVersionMessage);
    } catch {
      // The stored version is the fallback when BroadcastChannel is unavailable.
    }
    return highestVersion;
  }

  function clearPendingRotationKey() {
    setPendingRotationKey(ticket.id, null);
  }

  const checkedIn = ticket.status === "CHECKED_IN";
  const revoked = ticket.status === "REVOKED";
  const bookingUnavailableLabels: Record<string, TranslationKey> = {
    CANCELLED: "customer.wallet.ticketStatus.bookingCancelled",
    FAILED: "customer.wallet.ticketStatus.bookingFailed",
    EXPIRED: "customer.wallet.ticketStatus.bookingExpired",
    PENDING: "customer.wallet.ticketStatus.bookingPending",
  };
  const knownTicketStatus = ticket.status === "ISSUED" || checkedIn || revoked;
  const statusLabel = revoked
    ? t("customer.wallet.ticketStatus.revoked")
    : refundBlocked
    ? t("customer.wallet.ticketStatus.refundBlocked")
    : refundUnknown
      ? t("customer.wallet.ticketStatus.refundUnknown")
      : checkedIn
    ? t("customer.wallet.ticketStatus.checkedIn")
    : credentialEligible
        ? t("customer.wallet.ticketStatus.issued")
        : bookingUnavailableLabels[bookingStatus]
          ? t(bookingUnavailableLabels[bookingStatus])
          : t(knownTicketStatus ? "customer.wallet.ticketStatus.eligibilityUnavailable" : "customer.wallet.ticketStatus.statusUnavailable");
  const unavailableMessage = refundBlocked
    ? t("customer.wallet.refundEntryBlocked")
    : refundUnknown
      ? t("customer.wallet.refundEntryUnknown")
      : bookingUnavailableLabels[bookingStatus]
    ? t(bookingUnavailableLabels[bookingStatus])
    : !bookingConfirmed || !knownTicketStatus
      ? t("customer.wallet.ticketEligibilityUnconfirmed")
      : t("customer.wallet.ticketCannotCreateCode");
  const formattedEventDate = eventDate
    ? date(eventDate, { weekday: "short", month: "short", day: "numeric", year: "numeric" }) || t("customer.wallet.dateUnavailable")
    : t("customer.wallet.admissionForEvent");
  const checkedInDate = ticket.checked_in_at
    ? date(ticket.checked_in_at, { weekday: "short", month: "short", day: "numeric", year: "numeric" })
    : "";
  const displayedNotice = notice ? text(notice) : null;

  return (
    <article className="relative overflow-hidden rounded-xl border border-border bg-card" aria-labelledby={`ticket-${ticket.id}-title`}>
      <div className="absolute inset-y-0 left-0 w-1 bg-primary" aria-hidden="true" />
      <div className="grid gap-5 p-5 pl-6 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-center sm:p-6 sm:pl-7">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <span className="font-mono text-[10px] uppercase tracking-[0.22em] text-primary">{t("customer.wallet.admissionTitle")}</span>
            <span className={`rounded-full px-2.5 py-1 text-[11px] font-semibold ${checkedIn ? "bg-success-bg text-success-ink" : revoked || refundBlocked ? "bg-danger-bg text-danger-ink" : "bg-tag-bg text-muted"}`}>
              {statusLabel}
            </span>
          </div>
          <h2 id={`ticket-${ticket.id}-title`} className="mt-3 text-lg font-semibold tracking-tight text-foreground">
            {eventTitle}
          </h2>
          <p className="mt-1 text-sm text-muted">{formattedEventDate}</p>
          <div className="mt-5 grid gap-3 border-t border-border pt-4 text-sm sm:grid-cols-2">
            <div>
              <p className="text-[10px] uppercase tracking-[0.16em] text-muted">{t("customer.wallet.seatReference")}</p>
              <p className="mt-1 break-all font-mono text-xs text-foreground">{ticket.seat_id}</p>
            </div>
            <div>
              <p className="text-[10px] uppercase tracking-[0.16em] text-muted">{t("customer.wallet.ticketReference")}</p>
              <p className="mt-1 font-mono text-xs text-foreground">{ticket.id.slice(0, 12)}</p>
            </div>
          </div>
          {checkedIn && checkedInDate ? <p className="mt-4 text-xs text-success-ink">{t("customer.wallet.checkedInAt", { date: checkedInDate })}</p> : null}
        </div>

        <div className="flex flex-col items-start gap-3 sm:items-end">
          {refundRestricted ? null : !active ? (
            <button
              type="button"
              onClick={rotateCredential}
              disabled={loading || !pendingKeyLoaded || !credentialEligible || rotationBlocked}
              className="inline-flex min-h-11 items-center gap-2 rounded-full bg-primary px-5 text-sm font-semibold text-graphite transition-colors hover:bg-primary-hover disabled:cursor-not-allowed disabled:opacity-45"
            >
              {loading ? <RefreshCw size={15} className="animate-spin" /> : <QrCode size={16} />}
              {loading
                ? t("customer.wallet.preparingCode")
                : pendingKey
                  ? t("customer.wallet.checkPreviousRequest")
                  : notice === "That request already completed, but its one-time code cannot be restored. Generate a new code only if you still need one."
                    ? t("customer.wallet.generateNewCode")
                    : t("customer.wallet.showEntryQr")}
            </button>
          ) : (
            <div className="flex flex-wrap gap-2 sm:justify-end">
              <button type="button" onClick={rotateCredential} disabled={loading || !pendingKeyLoaded || !credentialEligible || rotationBlocked} className="inline-flex min-h-10 items-center gap-2 rounded-full border border-border px-4 text-xs font-semibold text-foreground transition-colors hover:bg-tag-bg disabled:opacity-50">
                <RefreshCw size={13} className={loading ? "animate-spin" : ""} />
                {t("customer.wallet.refreshCode")}
              </button>
              <button type="button" onClick={() => setPass(null)} className="min-h-10 rounded-full border border-border px-4 text-xs font-medium text-muted hover:bg-tag-bg hover:text-foreground">
                {t("customer.wallet.hideCode")}
              </button>
            </div>
          )}
          {active && pass ? (
            <div className="w-full rounded-lg border border-primary/30 bg-[#F2F0EA] p-3 text-center sm:w-[252px]">
              <QRCodeSVG value={pass.token} size={220} level="M" marginSize={4} bgColor="#F2F0EA" fgColor="#06080B" title={t("customer.wallet.entryCredentialAria", { event: eventTitle })} className="mx-auto h-auto max-w-full" />
              <p className="mt-2 font-mono text-[11px] font-semibold tabular-nums text-graphite" aria-live="polite">
                {t("customer.wallet.codeExpiresIn", {
                  minutes: number(Math.floor(remaining / 60), { minimumIntegerDigits: 2, useGrouping: false }),
                  seconds: number(remaining % 60, { minimumIntegerDigits: 2, useGrouping: false }),
                })}
              </p>
              <details className="mt-2 text-left">
                <summary className="cursor-pointer text-[11px] font-medium text-graphite underline underline-offset-2">{t("customer.wallet.showManualCode")}</summary>
                <code className="mt-2 block break-all rounded bg-white p-2 font-mono text-[10px] leading-relaxed text-graphite">{pass.token}</code>
              </details>
            </div>
          ) : null}
          {error ? <p role="alert" className="max-w-[260px] text-xs text-danger-ink sm:text-right">{text(error, "customer.wallet.credentialConfirmationFailed")}</p> : null}
          {displayedNotice ? <p role="status" aria-live="polite" className="max-w-[260px] text-xs text-muted sm:text-right">{displayedNotice}</p> : null}
          {refundRestricted ? <p role="status" className="max-w-[260px] text-xs text-danger-ink sm:text-right">{unavailableMessage}</p> : !credentialEligible ? <p className="max-w-[260px] text-xs text-muted sm:text-right">{unavailableMessage}</p> : null}
          {active ? <p className="max-w-[252px] text-[10px] leading-relaxed text-muted sm:text-right">{t("customer.wallet.codeOnlineNotice")}</p> : null}
          <Link href={walletOrderHref(ticket.booking_id, tab)} className="inline-flex min-h-11 items-center text-sm font-medium text-primary underline underline-offset-4 focus-visible:outline focus-visible:outline-2">{t("customer.wallet.viewOrderDetails")}</Link>
        </div>
      </div>
      {checkedIn ? <div className="flex items-center gap-2 border-t border-border bg-success-bg/40 px-6 py-3 text-xs text-success-ink"><TicketCheck size={14} /> {t("customer.wallet.checkedInNotice")}</div> : null}
    </article>
  );
}
