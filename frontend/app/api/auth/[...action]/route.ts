import { proxyAuthRequest } from "@/lib/auth-proxy";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

async function handle(request: Request, context: { params: Promise<{ action: string[] }> }) {
  const { action } = await context.params;
  return proxyAuthRequest(request, action.join("/"), process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000");
}

export { handle as POST, handle as GET, handle as HEAD, handle as PUT, handle as PATCH, handle as DELETE, handle as OPTIONS };
