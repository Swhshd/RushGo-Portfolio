"use client";

import { useState } from "react";
import { VenueLayout } from "@/lib/types";
import { SeatTooltip } from "@/components/seat-tooltip";
import { useI18n } from "@/lib/i18n/index";

interface VenueMapProps {
  layout: VenueLayout;
  onSectionClick: (sectionId: string) => void;
  selectedSectionId?: string;
}

export function VenueMap({ layout, onSectionClick, selectedSectionId }: VenueMapProps) {
  const { t, number } = useI18n();
  const [hoveredSectionId, setHoveredSectionId] = useState<string | null>(null);
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 });

  const hoveredSection = layout.sections.find((s) => s.id === hoveredSectionId);

  function handleMouseMove(e: React.MouseEvent) {
    const rect = e.currentTarget.getBoundingClientRect();
    setTooltipPos({
      x: e.clientX - rect.left,
      y: e.clientY - rect.top,
    });
  }

  return (
    <div className="relative w-full bg-card border border-border rounded-lg" onMouseMove={handleMouseMove}>
      <svg
        viewBox="0 0 800 600"
        className="w-full h-auto"
        xmlns="http://www.w3.org/2000/svg"
      >
        {/* Stage */}
        {layout.stage && (
          <g>
            {layout.stage.shape === "circle" ? (
              <ellipse
                cx={layout.stage.position.x + layout.stage.position.width / 2}
                cy={layout.stage.position.y + layout.stage.position.height / 2}
                rx={layout.stage.position.width / 2}
                ry={layout.stage.position.height / 2}
                fill="var(--color-border)"
              />
            ) : (
              <rect
                x={layout.stage.position.x}
                y={layout.stage.position.y}
                width={layout.stage.position.width}
                height={layout.stage.position.height}
                rx={8}
                fill="var(--color-border)"
              />
            )}
            <text
              x={layout.stage.position.x + layout.stage.position.width / 2}
              y={layout.stage.position.y + layout.stage.position.height / 2}
              textAnchor="middle"
              dominantBaseline="central"
              fill="white"
              fontSize={14}
              fontWeight={600}
            >
              {layout.stage.label === "STAGE"
                ? t("customer.seats.stage")
                : layout.stage.label === "FIELD"
                  ? t("customer.seats.field")
                  : layout.stage.label}
            </text>
          </g>
        )}

        {/* Sections */}
        {layout.sections.map((section) => {
          const isSoldOut = section.availableSeats === 0;
          const isOnSale = section.accountingReady === true;
          const isUnavailable = isSoldOut || !isOnSale;
          const isSelected = section.id === selectedSectionId;
          const isHovered = section.id === hoveredSectionId;
          const status = !isOnSale
            ? t("common.status.temporarilyUnavailable")
            : isSoldOut
              ? t("common.status.soldOut")
              : t("customer.seats.seatsLeft", { count: number(section.availableSeats) });

          return (
            <g key={section.id}>
              <path
                d={section.path}
                role="button"
                aria-label={isUnavailable
                  ? t("customer.seats.sectionStatusAria", { section: section.name, status })
                  : t("customer.seats.selectSectionAria", { section: section.name })}
                aria-pressed={isSelected}
                tabIndex={isUnavailable ? -1 : 0}
                aria-disabled={isUnavailable}
                onKeyDown={(event) => {
                  if (!isUnavailable && (event.key === "Enter" || event.key === " ")) {
                    event.preventDefault();
                    onSectionClick(section.id);
                  }
                }}
                fill={isUnavailable ? "var(--color-border)" : "var(--color-tag-bg)"}
                opacity={isHovered && !isUnavailable ? 0.85 : 1}
                stroke={isSelected ? "var(--color-primary)" : isHovered ? "var(--color-signal)" : "var(--color-muted)"}
                strokeWidth={isSelected || isHovered ? 2 : 1}
                strokeDasharray={!isOnSale ? "6 4" : undefined}
                cursor={isUnavailable ? "default" : "pointer"}
                className="transition-opacity duration-150"
                onClick={() => {
                  if (!isUnavailable) onSectionClick(section.id);
                }}
                onMouseEnter={() => {
                  if (!isUnavailable) setHoveredSectionId(section.id);
                }}
                onMouseLeave={() => setHoveredSectionId(null)}
              />
              <text
                x={section.labelPosition.x}
                y={section.labelPosition.y}
                textAnchor="middle"
                dominantBaseline="central"
                fill="white"
                fontSize={11}
                fontWeight={600}
                pointerEvents="none"
              >
                {section.name}
              </text>
              <text
                x={section.labelPosition.x}
                y={section.labelPosition.y + 14}
                textAnchor="middle"
                dominantBaseline="central"
                fill="white"
                fontSize={9}
                opacity={0.85}
                pointerEvents="none"
              >
                {status}
              </text>
            </g>
          );
        })}
      </svg>

      <SeatTooltip
        content={{
          title: hoveredSection?.name ?? "",
          subtitle: hoveredSection ? t("customer.seats.tier", { tier: hoveredSection.tier }) : undefined,
          price: hoveredSection?.price,
        }}
        position={tooltipPos}
        visible={!!hoveredSection}
      />
    </div>
  );
}
