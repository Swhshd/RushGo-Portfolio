"use client";

import { useState } from "react";
import { Ticket } from "lucide-react";
import { resolveEventArtwork } from "@/lib/event-artwork";
import { customerCategoryTranslationKey, useI18n } from "@/lib/i18n/index";

interface EventArtworkProps {
  src?: string | null;
  title: string;
  category?: string;
  className?: string;
  compact?: boolean;
}

// A new source remounts the image state, so a previous failure cannot hide it.
export function EventArtwork(props: EventArtworkProps) {
  const src = resolveEventArtwork(props.src);
  return <ArtworkSource key={src} {...props} src={src} />;
}

function ArtworkSource({ src, title, category, className = "", compact = false }: EventArtworkProps) {
  const { t } = useI18n();
  const [failed, setFailed] = useState(false);
  const showImage = Boolean(src) && !failed;
  const initials = title.split(/\s+/).filter(Boolean).slice(0, 2).map((word) => word[0]).join("").toUpperCase();
  const categoryKey = category ? customerCategoryTranslationKey(category) : null;
  const categoryLabel = categoryKey ? t(categoryKey) : category || initials;

  return (
    <div
      className={`relative h-full w-full overflow-hidden ${className}`}
      data-event-artwork={showImage ? "image" : "fallback"}
    >
      {showImage ? (
        // Keep existing remote origins working without an image proxy dependency.
        // eslint-disable-next-line @next/next/no-img-element
        <img
          src={src!}
          alt={title}
          width={1600}
          height={900}
          className="h-full w-full object-cover"
          onError={() => setFailed(true)}
        />
      ) : (
        <div role="img" aria-label={title} className="event-artwork-fallback flex h-full w-full items-center justify-center text-on-dark">
          <div aria-hidden="true" className={`flex flex-col items-center ${compact ? "gap-1" : "gap-3"}`}>
            <Ticket size={compact ? 20 : 32} className="text-primary/70" strokeWidth={1.5} />
            <span className={`font-mono font-semibold tracking-widest ${compact ? "text-[10px]" : "text-sm"}`}>
              {compact ? initials : "RushGo"}
            </span>
            {!compact && <span className="text-xs text-muted-on-dark">{categoryLabel}</span>}
          </div>
        </div>
      )}
    </div>
  );
}
