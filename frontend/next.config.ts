import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  agentRules: false,
  async headers() { return [
    { source: "/admin", headers: [{ key: "Cache-Control", value: "private, no-store" }] },
    { source: "/admin/:path*", headers: [{ key: "Cache-Control", value: "private, no-store" }] },
    { source: "/api/admin/:path*", headers: [{ key: "Cache-Control", value: "private, no-store" }] },
    { source: "/reset-password", headers: [{ key: "Cache-Control", value: "no-store" }, { key: "Referrer-Policy", value: "no-referrer" }] },
    { source: "/reset-password/:path*", headers: [{ key: "Cache-Control", value: "no-store" }, { key: "Referrer-Policy", value: "no-referrer" }] },
    { source: "/api/auth/:path*", headers: [{ key: "Cache-Control", value: "no-store" }] },
  ]; },
  async rewrites() {
    return { fallback: [
      {
        source: "/api/:path*",
        destination: `${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}/api/:path*`,
      },
    ] };
  },
};

export default nextConfig;
