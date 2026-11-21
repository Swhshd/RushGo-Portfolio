const actions = new Set(["login", "register", "registration-code", "forgot-password", "reset-password", "refresh", "logout"]);
function failure(status: number, code: string, error: string): Response {
  return Response.json({ code, error }, { status, headers: { "Cache-Control": "no-store" } });
}

export async function proxyAuthRequest(request: Request, action: string, gatewayOrigin: string): Promise<Response> {
  if (!actions.has(action)) return failure(404, "REQUEST_ERROR", "Not found.");
  if (request.method !== "POST") return failure(405, "REQUEST_ERROR", "Method not allowed.");
  const reader = request.body?.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    while (reader) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > 16 * 1024) {
        await reader.cancel();
        return failure(413, "VALIDATION_ERROR", "Request is too large.");
      }
      chunks.push(value);
    }
    const body = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) { body.set(chunk, offset); offset += chunk.byteLength; }
    const headers = new Headers({ "Content-Type": request.headers.get("content-type") || "application/json" });
    const authorization = request.headers.get("authorization");
    if (action === "logout" && authorization) headers.set("Authorization", authorization);
    // Gateway receives the actual Next peer; caller forwarding/baggage headers
    // cannot choose the abuse-budget identity or become credential telemetry.
    const upstream = await fetch(new URL(`/api/auth/${action}`, gatewayOrigin), {
      method: "POST", headers, body, cache: "no-store", redirect: "error",
      signal: AbortSignal.any([request.signal, AbortSignal.timeout(20_000)]),
    });
    const responseHeaders = new Headers({ "Cache-Control": "no-store", "Content-Type": upstream.headers.get("content-type") || "application/json" });
    const retryAfter = upstream.headers.get("retry-after");
    if (retryAfter) responseHeaders.set("Retry-After", retryAfter);
    return new Response(await upstream.arrayBuffer(), { status: upstream.status, headers: responseHeaders });
  } catch {
    return failure(503, "TEMPORARY_UNAVAILABLE", "Temporarily unavailable. Please try again.");
  } finally {
    reader?.releaseLock();
  }
}
