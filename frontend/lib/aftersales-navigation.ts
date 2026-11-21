import { isAdminEventOperationsId } from "./admin-event-operations";

/** Extract an ID only; navigation always uses our fixed local refund route.
 * A shared link is navigation, never evidence of approval permission.
 */
export function approvalEventId(input: string, origin: string): string | null {
  const value = input.trim();
  if (isAdminEventOperationsId(input.trim())) return value;
  if (!value || value.length > 2048 || /[\\\u0000-\u0020\u007f]/.test(value)) return null;
  try {
    const url = new URL(value, origin);
    if (!["http:", "https:"].includes(url.protocol) || url.origin !== new URL(origin).origin ||
        url.username || url.password || url.search || url.hash) return null;
    const match = /^\/admin\/events\/([^/]+)\/refunds\/?$/.exec(url.pathname);
    return match && isAdminEventOperationsId(match[1]) ? match[1] : null;
  } catch {
    return null;
  }
}
