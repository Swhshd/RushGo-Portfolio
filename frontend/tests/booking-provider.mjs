import React from "react";
import { loadTypeScript } from "./render-typescript.mjs";

if (typeof sessionStorage === "undefined") {
  const saved = new Map();
  globalThis.sessionStorage = {
    getItem: (key) => saved.get(key) ?? null,
    setItem: (key, value) => saved.set(key, value),
    removeItem: (key) => saved.delete(key),
  };
}

export const cart = { eventId: "event", seats: [{ seatId: "seat", tierId: "tier", price: 999, sectionName: "Floor", rowLabel: "A", seatLabel: "1" }] };
export const booking = (status) => ({ id: "booking", event_id: "event", status, creation_state: "ready", total_amount_cents: 1000, created_at: "2026-01-01T00:00:00Z", items: [{ quantity: 1, unit_price_cents: 1000 }], paymentIntentClientSecret: "test-placeholder" });

// Drive the real provider callbacks with controlled hook state and API results.
export function provider(getBooking, options = {}) {
  const slots = [];
  let slot = 0;
  let stateChanged = false;
  let sessionKey;
  let sessionStart = 0;
  const effects = [];
  const { BookingProvider } = loadTypeScript("lib/booking-context.tsx", {
    react: { ...React,
      useState(initial) {
        const index = slot++;
        if (!(index in slots)) slots[index] = initial;
        return [slots[index], (value) => {
          const next = typeof value === "function" ? value(slots[index]) : value;
          if (!Object.is(next, slots[index])) {
            slots[index] = next;
            stateChanged = true;
          }
        }];
      },
      useCallback(callback, dependencies) {
        const index = slot++;
        const previous = slots[index];
        if (!previous || !dependencies || dependencies.some((value, i) => value !== previous.dependencies[i])) {
          slots[index] = { callback, dependencies };
        }
        return slots[index].callback;
      },
      useMemo(factory, dependencies) {
        const index = slot++;
        const previous = slots[index];
        if (!previous || !dependencies || dependencies.some((value, i) => value !== previous.dependencies[i])) {
          slots[index] = { value: factory(), dependencies };
        }
        return slots[index].value;
      },
      useRef(initial) {
        const index = slot++;
        if (!(index in slots)) slots[index] = { current: initial };
        return slots[index];
      },
      useSyncExternalStore(_subscribe, getSnapshot) { slot++; return getSnapshot(); },
      useEffect(callback, dependencies) {
        if (!options.effects) return;
        const index = slot++;
        const previous = slots[index];
        if (!previous || dependencies.some((value, i) => value !== previous.dependencies[i])) {
          slots[index] = { dependencies, cleanup: previous?.cleanup };
          effects.push(() => { previous?.cleanup?.(); slots[index].cleanup = callback(); });
        }
      },
    },
    "./api/client": { apiClient: { createBooking: async (...args) => ({ creation_state: "ready", ...await (options.createBooking || (async () => booking("PENDING")))(...args) }), getBooking, getEvent: options.getEvent || (async (id) => ({ id, title: "Demo event", date: "2026-10-30T12:00:00Z", venue: "Demo venue", location: "Demo city", category: "concerts", description: "Synthetic event", image_url: "", tiers: [] })), getSessionVersion: () => options.sessionVersion ?? 0 } },
    "./auth-context": { useAuth: () => ({ user: Object.hasOwn(options, "user") ? options.user : { id: "user" }, isLoading: options.authLoading === true }) },
    "./mock-data": { events: [] },
  });
  const render = () => {
    let value;
    let passes = 0;
    do {
      if (++passes > 10) throw new Error("Provider harness exceeded render pass limit");
      stateChanged = false;
      slot = 0;
      const sessionElement = BookingProvider({ children: null });
      const nextSessionStart = slot;
      if (sessionKey !== undefined && sessionElement.key !== sessionKey) {
        for (let index = sessionStart; index < slots.length; index++) slots[index]?.cleanup?.();
        slots.length = nextSessionStart;
      }
      sessionKey = sessionElement.key;
      sessionStart = nextSessionStart;
      slot = nextSessionStart;
      value = sessionElement.type(sessionElement.props).props.value;
      while (effects.length) effects.shift()();
    } while (stateChanged);
    return value;
  };
  render.setUser = (user) => { options.user = user; };
  render.setAuth = (user, isLoading = false) => { options.user = user; options.authLoading = isLoading; };
  render.unmount = () => { for (const value of slots) value?.cleanup?.(); };
  return render;
}
