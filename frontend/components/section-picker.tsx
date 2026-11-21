"use client";

import { ArrowLeft } from "lucide-react";
import SeatLegend from "@/components/seat-legend";
import type { VenueSection, SeatRow, Seat, SelectedSeat } from "@/lib/types";
import { useI18n } from "@/lib/i18n/index";

interface SectionPickerProps {
  section: VenueSection;
  selectedSeats: SelectedSeat[];
  onSeatToggle: (seat: Seat, row: SeatRow, section: VenueSection) => void;
  onBack: () => void;
}

export default function SectionPicker({
  section,
  selectedSeats,
  onSeatToggle,
  onBack,
}: SectionPickerProps) {
  const { t, usdCents } = useI18n();
  const isSeatSelected = (seat: Seat) =>
    selectedSeats.some((s) => s.seatId === seat.id);

  return (
    <div className="flex flex-col gap-6 p-4 md:p-6 bg-card border border-border rounded-lg shadow-sm">
      {/* Header */}
      <div className="flex flex-col gap-3">
        <button
          onClick={onBack}
          className="flex items-center gap-1.5 text-sm text-muted hover:text-foreground transition-colors cursor-pointer self-start"
        >
          <ArrowLeft className="w-4 h-4" />
          {t("customer.seats.sectionBack")}
        </button>
        <div className="flex items-center gap-3">
          <h2 className="text-xl font-semibold">{section.name}</h2>
          <span
            className="rounded-sm px-3 py-1 text-sm font-medium text-primary bg-tag-bg"
          >
            {usdCents(Math.round(section.price * 100))}
          </span>
        </div>
      </div>

      {/* Seat Grid */}
      <div className="overflow-x-auto pb-2">
        <div className="flex flex-col items-center gap-2 w-max min-w-full">
        {section.rows.map((row) => (
          <div key={row.id} className="flex items-center gap-3">
            <span className="w-14 text-right text-xs text-muted font-medium shrink-0">
              {t("common.seat.row", { row: row.label.replace(/^Row\s+/i, "") })}
            </span>
            <div className="flex items-center gap-1.5">
              {row.seats.map((seat) => {
                const selected = isSeatSelected(seat);
                const isBooked = seat.status === "booked";
                const isReserved = seat.status === "reserved";
                const isDisabled = isBooked;
                const statusLabel = selected
                  ? t("common.status.selected")
                  : seat.status === "available"
                    ? t("common.status.available")
                    : seat.status === "reserved"
                      ? t("common.status.reserved")
                      : t("common.status.booked");
                const seatLabel = t("common.seat.seat", { seat: seat.label });
                const rowLabel = t("common.seat.row", { row: row.label.replace(/^Row\s+/i, "") });

                let backgroundColor: string;
                let color: string;
                let borderColor: string | undefined;
                let borderStyle: "solid" | "dashed" = "solid";
                let textDecoration: string | undefined;
                let cursor: string;

                if (selected) {
                  backgroundColor = "var(--color-primary)";
                  color = "var(--color-graphite)";
                  cursor = "pointer";
                } else if (isBooked) {
                  backgroundColor = "var(--color-border)";
                  color = "var(--color-muted)";
                  borderColor = "var(--color-control-border)";
                  textDecoration = "line-through";
                  cursor = "not-allowed";
                } else if (isReserved) {
                  backgroundColor = "var(--color-tag-bg)";
                  color = "var(--color-muted)";
                  borderColor = "var(--color-muted)";
                  borderStyle = "dashed";
                  cursor = "not-allowed";
                } else {
                  backgroundColor = "var(--color-tag-bg)";
                  color = "var(--color-foreground)";
                  borderColor = "var(--color-muted)";
                  cursor = "pointer";
                }

                return (
                  <button
                    key={seat.id}
                    disabled={isDisabled || isReserved}
                    aria-label={t("customer.seats.seatAria", { seat: seatLabel, row: rowLabel, status: statusLabel })}
                    aria-pressed={selected}
                    onClick={() => {
                      if (!isDisabled && !isReserved) {
                        onSeatToggle(seat, row, section);
                      }
                    }}
                    className="flex items-center justify-center rounded-lg text-xs font-medium transition-[filter] hover:brightness-90 disabled:hover:brightness-100"
                    style={{
                      width: 32,
                      height: 32,
                      backgroundColor,
                      color,
                      textDecoration,
                      border: borderColor
                        ? `2px ${borderStyle} ${borderColor}`
                        : "2px solid transparent",
                      cursor,
                    }}
                  >
                    {seat.label}
                  </button>
                );
              })}
            </div>
          </div>
        ))}
        </div>
      </div>

      {/* Legend */}
      <div className="flex justify-center pt-2">
        <SeatLegend
          items={[
            { label: t("customer.seats.legendAvailable"), color: "var(--color-tag-bg)", border: "var(--color-muted)" },
            { label: t("customer.seats.legendReserved"), color: "var(--color-tag-bg)", border: "var(--color-muted)", borderStyle: "dashed" },
            { label: t("customer.seats.legendBooked"), color: "var(--color-border)", border: "var(--color-control-border)" },
            { label: t("customer.seats.legendSelected"), color: "var(--color-primary)" },
          ]}
        />
      </div>
    </div>
  );
}
