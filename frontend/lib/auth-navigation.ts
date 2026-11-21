import { isCredentialRedirect } from "./recovery-navigation";
export type AuthReason = "AUTH_REQUIRED" | "SESSION_EXPIRED";

export function authMessage(reason: AuthReason): string {
  return reason === "SESSION_EXPIRED"
    ? "Your session has expired. Please sign in again."
    : "Sign in to continue with your booking.";
}

export function safeAuthRedirect(value: string | null, fallback = "/"): string {
  if (!value || !value.startsWith("/") || value.startsWith("//")) return fallback;
  try {
    // Reject encoded slashes/backslashes too: URL parsers may normalize them.
    const decoded = decodeURIComponent(value);
    if (decoded.startsWith("//") || /[\\\u0000-\u0020\u007f]/.test(decoded)) return fallback;
    const url = new URL(value, "https://rushgo.internal");
    if (url.origin !== "https://rushgo.internal" || url.pathname.startsWith("//") || isCredentialRedirect(url)) return fallback;
    return `${url.pathname}${url.search}${url.hash}`;
  } catch {
    return fallback;
  }
}

// Reuse the existing admin/customer login redirect query convention.
export function loginHref(redirect: string, reason: AuthReason): string {
  return `/login?${new URLSearchParams({ redirect: safeAuthRedirect(redirect), reason })}`;
}
