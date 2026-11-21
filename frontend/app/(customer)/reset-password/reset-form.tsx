"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { useAuth } from "@/lib/auth-context";
import { validateNewPassword } from "@/lib/auth-password";
import { useI18n } from "@/lib/i18n/index";

export default function ResetForm() {
  const router = useRouter();
  const { resetPassword } = useAuth();
  const { t, text } = useI18n();
  const captured = useRef(false);
  const [token, setToken] = useState("");
  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (captured.current) return;
    captured.current = true;
    const value = new URLSearchParams(window.location.hash.slice(1)).get("token") || "";
    window.history.replaceState(null, "", window.location.pathname);
    setToken(value);
  }, []);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setError("");
    if (!token) {
      setError("Open a fresh reset link from your email.");
      return;
    }
    const policy = validateNewPassword(password);
    if (policy) {
      setError(policy);
      return;
    }
    if (password !== confirm) {
      setError("Passwords do not match.");
      return;
    }
    setLoading(true);
    try {
      await resetPassword(token, password);
      setToken("");
      setPassword("");
      setConfirm("");
      router.push("/login?success=reset");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Temporarily unavailable. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex min-h-[calc(100vh-4rem)] items-center justify-center px-5 py-8">
      <div className="w-full max-w-md rounded-lg border border-border bg-card p-8 shadow-sm">
        <h1 className="mb-6 text-2xl font-bold">{t("customer.auth.resetTitle")}</h1>
        {!token && <p role="status" className="mb-4 text-sm text-muted">{t("customer.auth.openFreshResetLink")}</p>}
        {error && <p role="alert" className="mb-4 text-sm text-danger-ink">{text(error)}</p>}
        <form onSubmit={submit} className="flex flex-col gap-4">
          <label htmlFor="reset-password" className="text-xs font-medium text-muted">{t("customer.auth.newPassword")}</label>
          <input
            id="reset-password"
            type="password"
            autoComplete="new-password"
            required
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            className="rounded-lg border border-control-border bg-card px-3 py-2.5 text-sm"
          />
          <p className="text-xs text-muted">{t("customer.auth.passwordMinLength")}</p>
          <label htmlFor="reset-confirm" className="text-xs font-medium text-muted">{t("customer.auth.passwordConfirm")}</label>
          <input
            id="reset-confirm"
            type="password"
            autoComplete="new-password"
            required
            value={confirm}
            onChange={(event) => setConfirm(event.target.value)}
            className="rounded-lg border border-control-border bg-card px-3 py-2.5 text-sm"
          />
          <button type="submit" disabled={loading || !token} className="h-12 rounded-lg bg-primary font-semibold text-graphite disabled:opacity-50">
            {loading ? t("customer.auth.updatingPassword") : t("customer.auth.resetTitle")}
          </button>
        </form>
        <Link href="/forgot-password" className="mt-6 block text-sm text-primary-ink">{t("customer.auth.requestNewResetLink")}</Link>
      </div>
    </div>
  );
}
