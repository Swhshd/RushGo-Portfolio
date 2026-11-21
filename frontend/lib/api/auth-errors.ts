const messages = {
  VALIDATION_ERROR: "Please check the required fields.", INVALID_CODE: "The verification code is invalid.", CODE_EXPIRED: "This code has expired. Request a new code.", CODE_ATTEMPTS_EXHAUSTED: "Too many incorrect codes. Request a new code.", RATE_LIMITED: "Please wait before trying again.", DELIVERY_UNAVAILABLE: "Verification mail is temporarily unavailable.", ACCOUNT_EXISTS: "An account already exists. Sign in or recover your password.", INVALID_RESET_TOKEN: "This reset link is invalid. Request a new link.", RESET_TOKEN_EXPIRED: "This reset link has expired. Request a new link.", RESET_TOKEN_USED: "This reset link has already been used.", TEMPORARY_UNAVAILABLE: "Temporarily unavailable. Please try again.", EMAIL_CHANGE_NOT_SUPPORTED: "Email changes require verification.", INVALID_SESSION: "The current session could not be ended.", INVALID_CREDENTIALS: "Invalid credentials.", TOKEN_EXPIRED: "Your session has expired. Please sign in again.",
} as const;
export function authResponseError(status: number, publicCode?: string): Error & { code: keyof typeof messages; status: number } {
  const code = publicCode && Object.hasOwn(messages, publicCode) ? publicCode as keyof typeof messages : "TEMPORARY_UNAVAILABLE";
  return Object.assign(new Error(messages[code]), { code, status });
}
