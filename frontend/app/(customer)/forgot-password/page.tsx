"use client";

import { useState } from "react";
import Link from "next/link";
import { apiClient } from "@/lib/api/client";
import { useI18n } from "@/lib/i18n/index";

export default function ForgotPasswordPage() {
  const { t, text } = useI18n();
  const [email, setEmail] = useState("");
  const [requestReceived, setRequestReceived] = useState(false);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setError("");
    setLoading(true);
    try {
      await apiClient.requestPasswordReset(email);
      setRequestReceived(true);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Temporarily unavailable. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex min-h-[calc(100vh-4rem)] items-center justify-center px-5 py-8">
      <div className="w-full max-w-md rounded-lg border border-border bg-card p-8 shadow-sm">
        <h1 className="mb-6 text-2xl font-bold">{t("customer.auth.forgotTitle")}</h1>
        {requestReceived && <p role="status" className="mb-4 text-sm text-muted">{t("customer.auth.requestReceived")}</p>}
        {error && <p role="alert" className="mb-4 text-sm text-danger-ink">{text(error)}</p>}
        <form onSubmit={submit} className="flex flex-col gap-4">
          <label htmlFor="forgot-email" className="text-xs font-medium text-muted">{t("customer.auth.email")}</label>
          <input
            id="forgot-email"
            type="email"
            autoComplete="email"
            required
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            className="rounded-lg border border-control-border bg-card px-3 py-2.5 text-sm"
          />
          <button disabled={loading} className="h-12 rounded-lg bg-primary font-semibold text-graphite disabled:opacity-50">
            {loading ? t("customer.auth.requesting") : t("customer.auth.sendResetLink")}
          </button>
        </form>
        <Link href="/login" className="mt-6 block text-sm text-primary-ink">{t("customer.auth.backToSignIn")}</Link>
      </div>
    </div>
  );
}
