"use client";

import { useI18n } from "@/lib/i18n/index";

interface SeatTooltipProps {
  content: {
    title: string;
    subtitle?: string;
    price?: number;
  };
  position: { x: number; y: number };
  visible: boolean;
}

export function SeatTooltip({ content, position, visible }: SeatTooltipProps) {
  const { usdCents } = useI18n();
  if (!visible) return null;

  return (
    <div
      className="absolute z-50 pointer-events-none px-3 py-2 rounded-lg bg-tag-bg border border-control-border shadow-lg"
      style={{
        left: position.x + 12,
        top: position.y - 8,
      }}
    >
      <p className="text-white text-sm font-bold leading-tight">{content.title}</p>
      {content.subtitle && (
        <p className="text-muted text-xs mt-0.5">{content.subtitle}</p>
      )}
      {content.price !== undefined && (
        <p className="text-xs font-semibold mt-1 text-primary">
          {usdCents(Math.round(content.price * 100))}
        </p>
      )}
    </div>
  );
}
