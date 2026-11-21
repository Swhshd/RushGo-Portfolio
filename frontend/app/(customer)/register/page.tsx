"use client";

import { Suspense, useState, useRef, useEffect } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import Link from "next/link";
import { Loader2 } from "lucide-react";
import { useAuth } from "@/lib/auth-context";
import { apiClient } from "@/lib/api/client";
import { validateNewPassword } from "@/lib/auth-password";
import { safeAuthRedirect } from "@/lib/auth-navigation";
import type { ApiRegistrationCodeResponse } from "@/lib/api/types";
import { useI18n } from "@/lib/i18n/index";

export default function RegisterPage() {
  return <Suspense fallback={null}><RegisterForm /></Suspense>;
}

function RegisterForm() {
  const params = useSearchParams();
  const requestedRedirect = params.get("redirect");
  const redirect = requestedRedirect ? safeAuthRedirect(requestedRedirect, "/") : null;
  const signInHref = redirect ? "/login?" + new URLSearchParams({ redirect }).toString() : "/login";
  const { t, text, number } = useI18n();
  const router = useRouter();
  const { register } = useAuth();
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [error, setError] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [proof, setProof] = useState<ApiRegistrationCodeResponse | null>(null);
  const [code, setCode] = useState("");
  const [sending, setSending] = useState(false);
  const [remaining, setRemaining] = useState(0);
  const currentEmail = useRef(email);
  const registrationCodeRequest = useRef(0);
  useEffect(() => { if (!proof) return; const tick = () => setRemaining(Math.max(0, Math.ceil((Date.parse(proof.next_resend_at) - Date.now()) / 1000))); tick(); const timer = setInterval(tick, 1000); return () => clearInterval(timer); }, [proof]);
  async function sendCode() {
    const requestedEmail = email;
    const requestId = ++registrationCodeRequest.current;
    setError("");
    setSending(true);
    setProof(null);
    setCode("");

    try {
      const result = await apiClient.requestRegistrationCode(requestedEmail);
      if (
        requestId === registrationCodeRequest.current &&
        currentEmail.current === requestedEmail
      ) {
        setProof(result);
        setRemaining(
          Math.max(
            0,
            Math.ceil((Date.parse(result.next_resend_at) - Date.now()) / 1000),
          ),
        );
      }
    } catch (err) {
      if (
        requestId === registrationCodeRequest.current &&
        currentEmail.current === requestedEmail
      ) {
        setError(
          err instanceof Error
            ? err.message
            : "Verification mail is temporarily unavailable.",
        );
      }
    } finally {
      if (
        requestId === registrationCodeRequest.current &&
        currentEmail.current === requestedEmail
      ) {
        setSending(false);
      }
    }
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    const policyError = validateNewPassword(password); if (policyError) { setError(policyError); return; }
    if (password !== confirmPassword) { setError("Passwords do not match."); return; }
    if (!proof || !/^\d{6}$/.test(code)) { setError("Request and enter your verification code."); return; }
    setIsLoading(true);
    try {
      await register({ email, password, name, verification_id: proof.verification_id, verification_code: code });
      const next = new URLSearchParams({ success: "registered" });
      if (redirect) next.set("redirect", redirect);
      router.push("/login?" + next.toString());
    } catch (err) {
        setError(err instanceof Error ? err.message : "Registration failed.");
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <div className="flex items-center justify-center px-5 py-8 min-h-[calc(100vh-4rem)]">
      <div className="w-full max-w-md bg-card border border-border rounded-lg p-8 shadow-sm">
        <h1 className="text-2xl font-bold mb-6">{t("customer.auth.createAccount")}</h1>
        {error && (
          <div role="alert" className="bg-danger-bg text-danger-ink text-sm p-3 rounded-lg mb-4">{text(error)}</div>
        )}
        <form onSubmit={handleSubmit} className="flex flex-col gap-4">
          <div className="flex flex-col gap-1.5">
            <label htmlFor="register-name" className="text-xs font-medium text-muted">{t("customer.auth.fullName")}</label>
            <input
              id="register-name"
              name="name"
              autoComplete="name"
              type="text"
              required
              value={name}
              onChange={(e) => setName(e.target.value)}
              className="border border-control-border rounded-lg px-3 py-2.5 text-sm bg-card focus:border-primary transition-colors"
              placeholder={t("customer.auth.namePlaceholder")}
            />
          </div>
          <div className="flex flex-col gap-1.5">
            <label htmlFor="register-email" className="text-xs font-medium text-muted">{t("customer.auth.email")}</label>
            <input
              id="register-email"
              name="email"
              autoComplete="email"
              spellCheck={false}
              type="email"
              required
              value={email}
              onChange={(e) => {
                const nextEmail = e.target.value;
                currentEmail.current = nextEmail;
                registrationCodeRequest.current += 1;
                setEmail(nextEmail);
                setProof(null);
                setCode("");
                setRemaining(0);
                setSending(false);
              }}
              className="border border-control-border rounded-lg px-3 py-2.5 text-sm bg-card focus:border-primary transition-colors"
              placeholder={t("customer.auth.emailPlaceholder")}
            />
          </div>
          <div className="flex flex-col gap-1.5">
            <label htmlFor="register-password" className="text-xs font-medium text-muted">{t("customer.auth.password")}</label>
            <input
              id="register-password"
              name="password"
              autoComplete="new-password"
              type="password"
              required
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="border border-control-border rounded-lg px-3 py-2.5 text-sm bg-card focus:border-primary transition-colors"
              placeholder="••••••••"
            />
            <p className="text-xs text-muted">{t("customer.auth.passwordMinLength")}</p>
          </div>
          <div className="flex flex-col gap-1.5">
            <label htmlFor="register-confirm" className="text-xs font-medium text-muted">{t("customer.auth.passwordConfirm")}</label>
            <input
              id="register-confirm"
              autoComplete="new-password"
              type="password"
              required
              value={confirmPassword}
              onChange={(e) => setConfirmPassword(e.target.value)}
              className="border border-control-border rounded-lg px-3 py-2.5 text-sm bg-card focus:border-primary transition-colors"
              placeholder="••••••••"
            />
          </div>
          <button type="button" onClick={sendCode} disabled={sending || remaining > 0 || !email} className="border border-border rounded-lg px-4 py-2.5 text-sm disabled:opacity-50">{sending ? t("customer.auth.sending") : remaining > 0 ? t("customer.auth.resendIn", { seconds: number(remaining) }) : t("customer.auth.sendCode")}</button>
          {proof && <p role="status" className="text-sm text-muted">{t("customer.auth.verificationAccepted")}</p>}
          <div className="flex flex-col gap-1.5"><label htmlFor="register-code" className="text-xs font-medium text-muted">{t("customer.auth.verificationCode")}</label><input id="register-code" type="text" inputMode="numeric" autoComplete="one-time-code" pattern="[0-9]{6}" maxLength={6} value={code} onChange={e => setCode(e.target.value)} required className="border border-control-border rounded-lg px-3 py-2.5 text-sm bg-card" /></div>
          <button
            type="submit"
            disabled={isLoading || !proof || !/^\d{6}$/.test(code) || !name || !email || !password}
            className="flex items-center justify-center gap-2 bg-primary hover:bg-primary-hover disabled:opacity-70 text-graphite font-semibold rounded-lg h-12 transition-colors mt-2"
          >
            {isLoading ? <Loader2 size={16} className="animate-spin" /> : null}
            {isLoading ? t("customer.auth.creatingAccount") : t("customer.auth.createAccount")}
          </button>
        </form>
        <p className="text-sm text-muted text-center mt-6">
          {t("customer.auth.alreadyAccount")}{" "}
          <Link href={signInHref} className="text-primary-ink font-medium hover:underline">
            {t("customer.auth.signIn")}
          </Link>
        </p>
      </div>
    </div>
  );
}
