"use client";

import { Suspense, useState, useSyncExternalStore } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import Link from "next/link";
import { Loader2 } from "lucide-react";
import { useAuth } from "@/lib/auth-context";
import { authMessage, safeAuthRedirect } from "@/lib/auth-navigation";
import { useI18n } from "@/lib/i18n/index";

const emptySubscribe = () => () => {};

export default function LoginPage() {
  return <Suspense fallback={null}><LoginForm /></Suspense>;
}

function LoginForm() {
  const { t, text } = useI18n();
  const router = useRouter();
  const params = useSearchParams();
  const reason = params.get("reason");
  const requestedRedirect = params.get("redirect");
  const registerHref = requestedRedirect
    ? "/register?" + new URLSearchParams({ redirect: safeAuthRedirect(requestedRedirect) }).toString()
    : "/register";
  const { login } = useAuth();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const hydrated = useSyncExternalStore(emptySubscribe, () => true, () => false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    setIsLoading(true);
    try {
      const user = await login(email, password);
      // Honor an auth-bounce ?redirect= (e.g. from the admin guard),
      // otherwise land admins in the ops console and customers on Discover.
      const redirectParam = params.get("redirect");
      const fallback = user.role === "admin" ? "/admin" : "/";
      router.push(safeAuthRedirect(redirectParam, fallback));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed.");
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <div className="flex items-center justify-center px-5 py-8 min-h-[calc(100vh-4rem)]">
      <div className="w-full max-w-md bg-card border border-border rounded-lg p-8 shadow-sm">
        <h1 className="text-2xl font-bold mb-6">{t("customer.auth.signIn")}</h1>
        {params.get("success") === "registered" && <p role="status" className="text-sm text-muted mb-4">{t("customer.auth.accountCreated")}</p>}
        {params.get("success") === "reset" && <p role="status" className="text-sm text-muted mb-4">{t("customer.auth.passwordUpdated")}</p>}
        {(reason === "AUTH_REQUIRED" || reason === "SESSION_EXPIRED") && (
          <p role="status" className="text-sm text-foreground bg-tag-bg border border-border p-3 rounded-lg mb-4">{text(authMessage(reason))}</p>
        )}
        {error && (
          <div role="alert" className="bg-danger-bg text-danger-ink text-sm p-3 rounded-lg mb-4">{text(error)}</div>
        )}
        <form onSubmit={handleSubmit} method="post" data-hydrated={hydrated ? "true" : "false"} className="flex flex-col gap-4">
          <div className="flex flex-col gap-1.5">
            <label htmlFor="login-email" className="text-xs font-medium text-muted">{t("customer.auth.email")}</label>
            <input
              id="login-email"
              name="email"
              autoComplete="email"
              spellCheck={false}
              type="email"
              required
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="border border-control-border rounded-lg px-3 py-2.5 text-sm bg-card focus:border-primary transition-colors"
              placeholder={t("customer.auth.emailPlaceholder")}
            />
          </div>
          <div className="flex flex-col gap-1.5">
            <label htmlFor="login-password" className="text-xs font-medium text-muted">{t("customer.auth.password")}</label>
            <input
              id="login-password"
              name="password"
              autoComplete="current-password"
              type="password"
              required
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="border border-control-border rounded-lg px-3 py-2.5 text-sm bg-card focus:border-primary transition-colors"
              placeholder="••••••••"
            />
          </div>
          <button
            type="submit"
            disabled={isLoading}
            className="flex items-center justify-center gap-2 bg-primary hover:bg-primary-hover disabled:opacity-70 text-graphite font-semibold rounded-lg h-12 transition-colors mt-2"
          >
            {isLoading ? <Loader2 size={16} className="animate-spin" /> : null}
            {isLoading ? t("customer.auth.signingIn") : t("customer.auth.signIn")}
          </button>
        </form>
        <Link href="/forgot-password" className="block text-sm text-primary-ink mt-4">{t("customer.auth.forgotPassword")}</Link>
        <p className="text-sm text-muted text-center mt-6">
          {t("customer.auth.noAccount")}{" "}
          <Link href={registerHref} className="text-primary-ink font-medium hover:underline">
            {t("customer.auth.signUp")}
          </Link>
        </p>
      </div>
    </div>
  );
}
