"use client";

import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";
import Link from "next/link";
import { AlertCircle, ArrowLeft, Camera, CameraOff, Loader2, ScanLine } from "lucide-react";
import { useParams } from "next/navigation";
import { useAuth } from "@/lib/auth-context";
import { apiClient } from "@/lib/api/client";
import { loginHref } from "@/lib/auth-navigation";
import { useI18n, type TranslationKey } from "@/lib/i18n";
import { cameraFailure, type CameraError } from "@/lib/scanner-camera";

type DetectedCode = { rawValue?: string };
type NativeQRDetector = { detect(source: CanvasImageSource): Promise<DetectedCode[]> };
type NativeQRDetectorConstructor = new (options: { formats: string[] }) => NativeQRDetector;
type WindowWithBarcodeDetector = Window & { BarcodeDetector?: NativeQRDetectorConstructor };

type ScanState = { kind: "idle" | "checking" | "accepted" | "already" | "denied" | "error" };

const SCAN_STATE_KEYS: Record<ScanState["kind"], TranslationKey> = {
  idle: "admin.scanner.ready",
  checking: "admin.scanner.checking",
  accepted: "admin.scanner.admissionAccepted",
  already: "admin.scanner.alreadyCheckedIn",
  denied: "admin.scanner.entryRefused",
  error: "admin.scanner.confirmFailed",
};

const CAMERA_ERROR_KEYS: Record<CameraError, TranslationKey> = {
  frame: "admin.scanner.cameraFrameError",
  unsupported: "admin.scanner.cameraUnsupportedError",
  permission: "admin.scanner.cameraPermissionError",
  insecure: "admin.scanner.cameraSecureContext",
  device: "admin.scanner.cameraDeviceError",
  busy: "admin.scanner.cameraBusyError",
};

const emptySubscribe = () => () => {};

export default function EventCheckInPage() {
  const params = useParams<{ id: string }>();
  const eventID = typeof params?.id === "string" ? params.id : "";
  const { t } = useI18n();
  const hydrated = useSyncExternalStore(emptySubscribe, () => true, () => false);
  const { user, isLoading: authLoading } = useAuth();
  const mediaDevices = typeof navigator === "undefined" ? undefined : navigator.mediaDevices;
  const getUserMedia = mediaDevices?.getUserMedia;
  const Detector = typeof window === "undefined" ? undefined : (window as WindowWithBarcodeDetector).BarcodeDetector;
  const secureContext = typeof window !== "undefined" && window.isSecureContext;
  const cameraSupported = hydrated && secureContext && typeof getUserMedia === "function" && typeof Detector === "function";
  const videoRef = useRef<HTMLVideoElement | null>(null);
  const streamRef = useRef<MediaStream | null>(null);
  const detectorRef = useRef<NativeQRDetector | null>(null);
  const frameRef = useRef<number | null>(null);
  const detectorBusyRef = useRef(false);
  const scanBusyRef = useRef(false);
  const credentialInputRef = useRef<HTMLInputElement | null>(null);
  const [cameraActive, setCameraActive] = useState(false);
  const [cameraLoading, setCameraLoading] = useState(false);
  const [cameraError, setCameraError] = useState<CameraError | null>(null);
  const [credential, setCredential] = useState("");
  const [scanState, setScanState] = useState<ScanState>({ kind: "idle" });

  useEffect(() => {
    return () => {
      if (frameRef.current !== null) window.cancelAnimationFrame(frameRef.current);
      streamRef.current?.getTracks().forEach((track) => track.stop());
      streamRef.current = null;
      detectorRef.current = null;
    };
  }, []);

  const stopCamera = useCallback(() => {
    if (frameRef.current !== null) window.cancelAnimationFrame(frameRef.current);
    frameRef.current = null;
    streamRef.current?.getTracks().forEach((track) => track.stop());
    streamRef.current = null;
    detectorRef.current = null;
    detectorBusyRef.current = false;
    setCameraActive(false);
    setCameraLoading(false);
  }, []);

  const submitCredential = useCallback(async (value: string) => {
    const token = value.trim();
    if (!token || scanBusyRef.current || !eventID) return;
    scanBusyRef.current = true;
    stopCamera();
    setCredential("");
    setScanState({ kind: "checking" });
    try {
      const result = await apiClient.checkInTicket(eventID, token, crypto.randomUUID());
      if (result.result === "accepted") {
        setScanState({ kind: "accepted" });
      } else if (result.result === "already_checked_in") {
        setScanState({ kind: "already" });
      } else {
        setScanState({ kind: "denied" });
      }
    } catch {
      setScanState({ kind: "error" });
    } finally {
      scanBusyRef.current = false;
    }
  }, [eventID, stopCamera]);

  async function scanFrame() {
    const video = videoRef.current;
    const detector = detectorRef.current;
    if (!video || !detector || !streamRef.current) return;
    if (!detectorBusyRef.current && video.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA) {
      detectorBusyRef.current = true;
      try {
        const codes = await detector.detect(video);
        const token = codes.find((code) => code.rawValue)?.rawValue;
        if (token) {
          stopCamera();
          void submitCredential(token);
          return;
        }
      } catch {
        setCameraError("frame");
        stopCamera();
        return;
      } finally {
        detectorBusyRef.current = false;
      }
    }
    if (streamRef.current) frameRef.current = window.requestAnimationFrame(() => void scanFrame());
  }

  async function startCamera() {
    if (cameraLoading || cameraActive) return;
    setCameraError(null);
    setCameraLoading(true);
    if (!secureContext) {
      setCameraError("insecure");
      setCameraLoading(false);
      return;
    }
    if (typeof getUserMedia !== "function" || typeof Detector !== "function") {
      setCameraError("unsupported");
      setCameraLoading(false);
      credentialInputRef.current?.focus();
      return;
    }
    try {
      const stream = await getUserMedia.call(mediaDevices, { video: { facingMode: { ideal: "environment" } }, audio: false });
      streamRef.current = stream;
      detectorRef.current = new Detector({ formats: ["qr_code"] });
      if (!videoRef.current) throw new Error("Camera view unavailable");
      videoRef.current.srcObject = stream;
      await videoRef.current.play();
      setCameraActive(true);
      setCameraLoading(false);
      frameRef.current = window.requestAnimationFrame(() => void scanFrame());
    } catch (error) {
      stopCamera();
      setCameraError(cameraFailure(error));
      credentialInputRef.current?.focus();
    }
  }

  if (!hydrated || authLoading) {
    return <div role="status" aria-live="polite" className="mx-auto flex min-h-[60vh] max-w-3xl items-center justify-center gap-3 px-5 text-sm text-muted"><Loader2 size={18} className="animate-spin" />{t("admin.scanner.sessionLoading")}</div>;
  }
  if (!user) {
    return <div className="mx-auto flex min-h-[60vh] max-w-lg flex-col items-center justify-center gap-4 px-5 text-center"><AlertCircle size={24} className="text-primary" /><p className="text-muted">{t("admin.scanner.signInRequired")}</p><Link href={loginHref(`/events/${eventID}/check-in`, "AUTH_REQUIRED")} className="rounded-full bg-primary px-5 py-3 text-sm font-semibold text-graphite">{t("admin.scanner.signIn")}</Link></div>;
  }

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-6 px-5 py-8 md:px-10 md:py-12">
      <Link href={`/events/${eventID}`} className="inline-flex min-h-10 w-fit items-center gap-2 text-sm text-muted hover:text-foreground"><ArrowLeft size={15} />{t("admin.scanner.backToEvent")}</Link>
      <header className="grid gap-4 border-b border-border pb-6 md:grid-cols-[1fr_auto] md:items-end">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.22em] text-signal">{t("admin.scanner.onlineAdmissionStaff")}</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">{t("admin.scanner.title")}</h1>
          <p className="mt-2 max-w-2xl text-sm leading-relaxed text-muted">{t("admin.scanner.description")}</p>
        </div>
        <span className="font-mono text-xs text-muted">{t("admin.scanner.eventId", { id: eventID.slice(0, 8) })}</span>
      </header>

      <section className="grid gap-6 lg:grid-cols-[minmax(0,1.15fr)_minmax(300px,0.85fr)]">
        <div className="overflow-hidden rounded-xl border border-border bg-card">
          <div className="flex items-center justify-between border-b border-border px-5 py-4">
            <div className="flex items-center gap-2 text-sm font-semibold"><ScanLine size={16} className="text-signal" />{t("admin.scanner.liveScanner")}</div>
            <span className={`font-mono text-[10px] uppercase tracking-wider ${cameraActive ? "text-success-ink" : "text-muted"}`}>{cameraActive ? t("admin.scanner.cameraActive") : t("admin.scanner.onlineOnly")}</span>
          </div>
          <div className="p-4 sm:p-5">
            <div className="relative aspect-[4/3] overflow-hidden rounded-lg border border-border bg-[#090D12]">
              <video ref={videoRef} autoPlay muted playsInline aria-label={t("admin.scanner.cameraView")} className={`${cameraActive ? "block" : "hidden"} h-full w-full object-cover`} />
              {!cameraActive ? <div className="absolute inset-0 grid place-items-center p-6 text-center"><div><div className="mx-auto grid h-16 w-16 place-items-center rounded-full border border-signal/30 bg-signal/10 text-signal"><ScanLine size={27} /></div><p className="mt-4 text-sm font-medium text-foreground">{t("admin.scanner.positionQr")}</p><p className="mt-1 text-xs text-muted">{t("admin.scanner.serverConfirms")}</p></div></div> : null}
              {cameraActive ? <div aria-hidden="true" className="pointer-events-none absolute inset-x-[12%] top-1/2 h-px bg-signal shadow-[0_0_16px_3px_rgba(56,191,209,0.45)]" /> : null}
            </div>
            <div className="mt-4 flex flex-wrap gap-2">
              {!cameraActive ? <button type="button" onClick={() => void startCamera()} disabled={cameraLoading || !cameraSupported} className="inline-flex min-h-11 items-center gap-2 rounded-full bg-signal px-5 text-sm font-semibold text-graphite disabled:cursor-not-allowed disabled:opacity-50"><Camera size={15} />{cameraLoading ? t("admin.scanner.openingCamera") : t("admin.scanner.startCamera")}</button> : <button type="button" onClick={stopCamera} className="inline-flex min-h-11 items-center gap-2 rounded-full border border-border px-5 text-sm font-semibold text-foreground hover:bg-tag-bg"><CameraOff size={15} />{t("admin.scanner.stopCamera")}</button>}
              {!cameraSupported ? <span className="self-center text-xs text-muted">{t(!secureContext ? "admin.scanner.cameraSecureContext" : "admin.scanner.cameraUnsupportedError")}</span> : null}
            </div>
            {cameraError ? <p role="alert" className="mt-3 text-xs text-danger-ink">{t(CAMERA_ERROR_KEYS[cameraError])}</p> : null}
          </div>
        </div>

        <div className="flex flex-col gap-5">
          <form onSubmit={(event) => { event.preventDefault(); void submitCredential(credential); }} className="rounded-xl border border-border bg-card p-5 sm:p-6">
            <label htmlFor="ticket-credential" className="text-sm font-semibold">{t("admin.scanner.manualCode")}</label>
            <p className="mt-1 text-xs leading-relaxed text-muted">{t("admin.scanner.manualHelp")}</p>
            <input ref={credentialInputRef} id="ticket-credential" autoComplete="off" autoCapitalize="off" spellCheck={false} value={credential} onChange={(event) => setCredential(event.target.value)} className="mt-4 min-h-12 w-full rounded-lg border border-control-border bg-background px-3 font-mono text-sm text-foreground placeholder:text-muted focus:border-signal" placeholder={t("admin.scanner.codePlaceholder")} />
            <button type="submit" disabled={!credential.trim() || scanState.kind === "checking"} className="mt-3 inline-flex min-h-11 w-full items-center justify-center rounded-lg bg-primary px-4 text-sm font-semibold text-graphite hover:bg-primary-hover disabled:cursor-not-allowed disabled:opacity-45">{t("admin.scanner.checkOnline")}</button>
          </form>

          <div aria-live="polite" role={scanState.kind === "denied" || scanState.kind === "error" ? "alert" : "status"} className={`rounded-xl border p-5 ${scanState.kind === "accepted" ? "border-success/40 bg-success-bg text-success-ink" : scanState.kind === "denied" || scanState.kind === "error" ? "border-danger/40 bg-danger-bg text-danger-ink" : scanState.kind === "already" ? "border-primary/40 bg-tag-bg text-primary" : "border-border bg-card text-muted"}`}>
            <p className="font-mono text-[10px] uppercase tracking-[0.18em]">{t("admin.scanner.serverResult")}</p>
            <p className="mt-2 text-sm font-medium">{t(SCAN_STATE_KEYS[scanState.kind])}</p>
          </div>
          <p className="text-[11px] leading-relaxed text-muted">{t("admin.scanner.credentialSafety")}</p>
        </div>
      </section>
    </div>
  );
}
