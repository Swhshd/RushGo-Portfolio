import { Navbar } from "@/components/navbar";
import { Suspense } from "react";

export default function CustomerLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="min-h-screen flex flex-col">
      <Suspense fallback={<div className="min-h-16 bg-card border-b border-border" />}><Navbar /></Suspense>
      <main className="flex-1">{children}</main>
    </div>
  );
}
