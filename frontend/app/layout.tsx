import type { Metadata } from "next";
import localFont from "next/font/local";
import "./globals.css";
import { AuthProvider } from "@/lib/auth-context";
import { BookingProvider } from "@/lib/booking-context";
import { I18nProvider } from "@/lib/i18n/context";
import { commonEn } from "@/lib/i18n/common";

const geistSans = localFont({
  src: "./fonts/geist-latin.woff2",
  variable: "--font-geist-sans",
  weight: "100 900",
  display: "swap",
});

const geistMono = localFont({
  src: "./fonts/geist-mono-latin.woff2",
  variable: "--font-geist-mono",
  weight: "100 900",
  display: "swap",
});

export const metadata: Metadata = {
  title: commonEn["common.metadata.title"],
  description: "An event ticketing project with event discovery, seat selection, and order workflows.",
  icons: { icon: "data:," },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en-US" suppressHydrationWarning>
      <body className={`${geistSans.variable} ${geistMono.variable} antialiased`} suppressHydrationWarning>
        <I18nProvider>
          <AuthProvider>
            <BookingProvider>{children}</BookingProvider>
          </AuthProvider>
        </I18nProvider>
      </body>
    </html>
  );
}
