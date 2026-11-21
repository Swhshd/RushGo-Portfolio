"use client";

import { useEffect, useState, type FormEvent } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { AlertCircle, CheckCircle2, Loader2, LockKeyhole, Mail, UserRound } from "lucide-react";
import { useAuth } from "@/lib/auth-context";
import { apiClient } from "@/lib/api/client";
import { loginHref } from "@/lib/auth-navigation";
import { useI18n } from "@/lib/i18n";

type AccountLocale = "en" | "zh";
type EmailCodeState = { verificationId: string; expiresAt: string; nextResendAt: string };

function toAccountLocale(value: string | undefined): AccountLocale {
  return value === "zh" ? "zh" : "en";
}

function displayDate(value: string, locale: string): string {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeStyle: "short" }).format(date);
}

export default function AccountSettingsPage() {
  const { t, locale, setLocale } = useI18n();
  const router = useRouter();
  const { user, isLoading: authLoading, clearSession, syncUser } = useAuth();
  const [profileResultKey, setProfileResultKey] = useState<string | null>(null);
  const [profileErrorKey, setProfileErrorKey] = useState<string | null>(null);
  const [profileRetry, setProfileRetry] = useState(0);
  const [name, setName] = useState("");
  const [accountLocale, setAccountLocale] = useState<AccountLocale>("en");
  const [profileBusy, setProfileBusy] = useState(false);
  const [profileError, setProfileError] = useState(false);
  const [profileSaved, setProfileSaved] = useState(false);

  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [passwordBusy, setPasswordBusy] = useState(false);
  const [passwordError, setPasswordError] = useState(false);

  const [newEmail, setNewEmail] = useState("");
  const [verificationCode, setVerificationCode] = useState("");
  const [emailCode, setEmailCode] = useState<EmailCodeState | null>(null);
  const [emailBusy, setEmailBusy] = useState(false);
  const [emailCodeError, setEmailCodeError] = useState(false);
  const [emailConfirmError, setEmailConfirmError] = useState(false);
  const [codeClock, setCodeClock] = useState(0);
  const [securityComplete, setSecurityComplete] = useState(false);
  const profileRequestKey = user ? `${user.id}:${profileRetry}` : "";
  const loading = Boolean(user) && profileResultKey !== profileRequestKey && profileErrorKey !== profileRequestKey;
  const loadError = Boolean(user) && profileErrorKey === profileRequestKey;

  useEffect(() => {
    if (!authLoading && !user) router.replace(loginHref("/account", "AUTH_REQUIRED"));
  }, [authLoading, router, user]);

  useEffect(() => {
    if (!user) return;
    let active = true;
    const requestKey = profileRequestKey;
    void apiClient.getProfile().then((profile) => {
      if (!active) return;
      setName(profile.name);
      const nextLocale = toAccountLocale(profile.locale);
      setAccountLocale(nextLocale);
      setLocale(nextLocale === "zh" ? "zh-CN" : "en-US");
      setProfileResultKey(requestKey);
      setProfileErrorKey(null);
    }).catch(() => {
      if (active) setProfileErrorKey(requestKey);
    });
    return () => { active = false; };
  }, [profileRequestKey, setLocale, user]);

  useEffect(() => {
    if (!emailCode) return;
    const timer = window.setInterval(() => setCodeClock(Date.now()), 15000);
    return () => window.clearInterval(timer);
  }, [emailCode]);

  async function saveProfile(event: FormEvent) {
    event.preventDefault();
    if (profileBusy || !name.trim()) return;
    setProfileBusy(true);
    setProfileError(false);
    setProfileSaved(false);
    try {
      const updated = await apiClient.updateProfile({ name: name.trim(), locale: accountLocale });
      syncUser(updated);
      setName(updated.name);
      const savedLocale = toAccountLocale(updated.locale);
      setAccountLocale(savedLocale);
      setLocale(savedLocale === "zh" ? "zh-CN" : "en-US");
      setProfileSaved(true);
    } catch {
      setProfileError(true);
    } finally {
      setProfileBusy(false);
    }
  }

  function completeSecurityChange() {
    setCurrentPassword("");
    setNewPassword("");
    setVerificationCode("");
    setEmailCode(null);
    setSecurityComplete(true);
    clearSession();
  }

  async function changePassword(event: FormEvent) {
    event.preventDefault();
    if (passwordBusy || !currentPassword || !newPassword) return;
    setPasswordBusy(true);
    setPasswordError(false);
    try {
      const result = await apiClient.changePassword(currentPassword, newPassword);
      if (result.sign_in_required) completeSecurityChange();
    } catch {
      setPasswordError(true);
    } finally {
      setPasswordBusy(false);
    }
  }

  async function requestEmailCode(event: FormEvent) {
    event.preventDefault();
    if (emailBusy || !currentPassword || !newEmail.trim()) return;
    setEmailBusy(true);
    setEmailCodeError(false);
    setEmailConfirmError(false);
    try {
      const result = await apiClient.requestEmailChangeCode(currentPassword, newEmail.trim());
      setEmailCode({ verificationId: result.verification_id, expiresAt: result.expires_at, nextResendAt: result.next_resend_at });
      setVerificationCode("");
      setCodeClock(Date.now());
    } catch {
      setEmailCodeError(true);
    } finally {
      setEmailBusy(false);
    }
  }

  async function confirmEmailChange(event: FormEvent) {
    event.preventDefault();
    if (!emailCode || emailBusy || !currentPassword || !verificationCode.trim()) return;
    setEmailBusy(true);
    setEmailConfirmError(false);
    try {
      const result = await apiClient.completeEmailChange(currentPassword, emailCode.verificationId, verificationCode.trim());
      if (result.sign_in_required) completeSecurityChange();
    } catch {
      setEmailConfirmError(true);
    } finally {
      setEmailBusy(false);
    }
  }

  if (authLoading || loading) {
    return <main className="mx-auto flex min-h-72 max-w-3xl items-center justify-center gap-3 px-4 text-sm text-muted"><Loader2 className="animate-spin text-primary" size={20} />{t("customer.account.loading")}</main>;
  }

  if (securityComplete) {
    return (
      <main className="mx-auto flex min-h-[50vh] w-full max-w-xl flex-col justify-center px-4 py-10">
        <section className="rounded-2xl border border-border bg-card p-5 sm:p-8">
          <CheckCircle2 className="mb-4 text-success" size={28} />
          <h1 className="text-xl font-bold">{t("customer.account.securityChanged")}</h1>
          <p className="mt-2 text-sm leading-6 text-muted">{t("customer.account.signInRequired")}</p>
          <Link href="/login?redirect=%2Faccount" className="mt-6 inline-flex min-h-11 items-center rounded-full bg-primary px-5 text-sm font-semibold text-graphite">{t("customer.account.signIn")}</Link>
        </section>
      </main>
    );
  }

  if (!user) {
    return <main role="status" className="mx-auto flex min-h-72 max-w-3xl items-center justify-center gap-3 px-4 text-sm text-muted"><Loader2 size={20} className="animate-spin text-primary" />{t("customer.account.loading")}</main>;
  }

  if (loadError) {
    return <main className="mx-auto flex w-full max-w-3xl flex-col gap-4 px-4 py-8"><p role="alert" className="flex items-center gap-2 rounded-xl border border-danger/30 bg-danger-bg p-4 text-sm text-danger-ink"><AlertCircle size={17} />{t("customer.account.loadError")}</p><button type="button" onClick={() => setProfileRetry((value) => value + 1)} className="min-h-11 self-start rounded-full border border-border px-4 text-sm font-semibold">{t("customer.account.retry")}</button></main>;
  }

  const canResendAt = emailCode ? Date.parse(emailCode.nextResendAt) : 0;
  const canResend = !emailCode || !Number.isFinite(canResendAt) || codeClock >= canResendAt;

  return (
    <main className="mx-auto flex w-full max-w-3xl flex-col gap-5 px-4 py-6 sm:py-9">
      <header>
        <h1 className="text-2xl font-bold tracking-tight">{t("customer.account.title")}</h1>
        <p className="mt-1 text-sm text-muted">{t("customer.account.description")}</p>
      </header>

      <section className="rounded-2xl border border-border bg-card p-4 sm:p-6">
        <h2 className="flex items-center gap-2 text-base font-semibold"><UserRound size={18} />{t("customer.account.profileHeading")}</h2>
        <form onSubmit={saveProfile} className="mt-4 flex flex-col gap-4">
          <label className="flex flex-col gap-1.5 text-sm font-medium">{t("customer.account.name")}<input required maxLength={100} value={name} onChange={(event) => { setName(event.target.value); setProfileSaved(false); }} className="h-11 rounded-lg border border-control-border bg-background px-3" /></label>
          <label className="flex flex-col gap-1.5 text-sm font-medium">{t("customer.account.locale")}<select value={accountLocale} onChange={(event) => { setAccountLocale(event.target.value as AccountLocale); setProfileSaved(false); }} className="h-11 rounded-lg border border-control-border bg-background px-3"><option value="en">{t("customer.account.english")}</option><option value="zh">{t("customer.account.chinese")}</option></select></label>
          {profileError && <p role="alert" className="text-sm text-danger-ink">{t("customer.account.profileError")}</p>}
          {profileSaved && <p role="status" className="flex items-center gap-2 text-sm text-success"><CheckCircle2 size={16} />{t("customer.account.profileSaved")}</p>}
          <button disabled={profileBusy || !name.trim()} className="min-h-11 self-start rounded-full bg-primary px-5 text-sm font-semibold text-graphite disabled:opacity-50">{profileBusy ? <Loader2 size={16} className="animate-spin" /> : t("customer.account.saveProfile")}</button>
        </form>
      </section>

      <section className="rounded-2xl border border-border bg-card p-4 sm:p-6">
        <h2 className="flex items-center gap-2 text-base font-semibold"><LockKeyhole size={18} />{t("customer.account.passwordHeading")}</h2>
        <form onSubmit={changePassword} className="mt-4 flex flex-col gap-4">
          <label className="flex flex-col gap-1.5 text-sm font-medium">{t("customer.account.currentPassword")}<input required type="password" autoComplete="current-password" value={currentPassword} onChange={(event) => setCurrentPassword(event.target.value)} className="h-11 rounded-lg border border-control-border bg-background px-3" /></label>
          <label className="flex flex-col gap-1.5 text-sm font-medium">{t("customer.account.newPassword")}<input required type="password" autoComplete="new-password" minLength={8} value={newPassword} onChange={(event) => setNewPassword(event.target.value)} className="h-11 rounded-lg border border-control-border bg-background px-3" /></label>
          {passwordError && <p role="alert" className="text-sm text-danger-ink">{t("customer.account.passwordError")}</p>}
          <button disabled={passwordBusy} className="min-h-11 self-start rounded-full border border-border px-5 text-sm font-semibold disabled:opacity-50">{passwordBusy ? <Loader2 size={16} className="animate-spin" /> : t("customer.account.changePassword")}</button>
        </form>
      </section>

      <section className="rounded-2xl border border-border bg-card p-4 sm:p-6">
        <h2 className="flex items-center gap-2 text-base font-semibold"><Mail size={18} />{t("customer.account.emailHeading")}</h2>
        <form onSubmit={requestEmailCode} className="mt-4 flex flex-col gap-4">
          <label className="flex flex-col gap-1.5 text-sm font-medium">{t("customer.account.currentPassword")}<input required type="password" autoComplete="current-password" value={currentPassword} onChange={(event) => setCurrentPassword(event.target.value)} className="h-11 rounded-lg border border-control-border bg-background px-3" /></label>
          <label className="flex flex-col gap-1.5 text-sm font-medium">{t("customer.account.newEmail")}<input required type="email" autoComplete="email" value={newEmail} onChange={(event) => { setNewEmail(event.target.value); setEmailCode(null); setEmailCodeError(false); }} className="h-11 rounded-lg border border-control-border bg-background px-3" /></label>
          {emailCode && <div className="rounded-lg bg-tag-bg p-3 text-xs leading-5 text-muted"><p>{t("customer.account.verificationExpires", { date: displayDate(emailCode.expiresAt, locale) })}</p>{!canResend && <p>{t("customer.account.resendAfter", { date: displayDate(emailCode.nextResendAt, locale) })}</p>}</div>}
          {emailCodeError && <p role="alert" className="text-sm text-danger-ink">{t("customer.account.emailCodeError")}</p>}
          <button disabled={emailBusy || !currentPassword || !newEmail.trim() || !canResend} className="min-h-11 self-start rounded-full border border-border px-5 text-sm font-semibold disabled:opacity-50">{emailBusy ? <Loader2 size={16} className="animate-spin" /> : t("customer.account.requestCode")}</button>
        </form>
        {emailCode && <form onSubmit={confirmEmailChange} className="mt-4 flex flex-col gap-4 border-t border-border pt-4">
          <p role="status" className="text-sm text-success">{t("customer.account.codeSent")}</p>
          <label className="flex flex-col gap-1.5 text-sm font-medium">{t("customer.account.verificationCode")}<input required inputMode="numeric" autoComplete="one-time-code" value={verificationCode} onChange={(event) => setVerificationCode(event.target.value)} className="h-11 rounded-lg border border-control-border bg-background px-3" /></label>
          {emailConfirmError && <p role="alert" className="text-sm text-danger-ink">{t("customer.account.emailConfirmError")}</p>}
          <button disabled={emailBusy || !currentPassword || !verificationCode.trim()} className="min-h-11 self-start rounded-full bg-primary px-5 text-sm font-semibold text-graphite disabled:opacity-50">{emailBusy ? <Loader2 size={16} className="animate-spin" /> : t("customer.account.confirmEmail")}</button>
        </form>}
      </section>
    </main>
  );
}
