export function canContinueWithSeatInventory(phase) {
  return phase === "LIVE";
}

export function compareSeatRevision(left, right) {
  const leftRevision = BigInt(left);
  const rightRevision = BigInt(right);
  return leftRevision < rightRevision ? -1 : leftRevision > rightRevision ? 1 : 0;
}

export function applySeatUpdates(seats, updates) {
  const nextSeats = new Map(seats.map((seat) => [seatID(seat), seat]));
  let changed = false;
  let conflict = false;
  const invalidatedSeatIds = [];

  for (const update of updates) {
    const current = nextSeats.get(update.seat_id);
    if (!current) continue;

    const currentRevision = current.seatRevision ?? current.seat_revision;
    const order = compareSeatRevision(update.seat_revision, currentRevision);
    if (order < 0) continue;
    if (order === 0) {
      if (update.status !== current.status) conflict = true;
      continue;
    }

    const next = { ...current, status: update.status };
    if (Object.hasOwn(current, "seat_revision")) next.seat_revision = update.seat_revision;
    else next.seatRevision = update.seat_revision;
    if (Object.hasOwn(current, "updated_at")) next.updated_at = update.updated_at;
    else if (Object.hasOwn(current, "updatedAt")) next.updatedAt = update.updated_at;
    nextSeats.set(update.seat_id, next);
    changed = true;
    if (update.status !== "available") invalidatedSeatIds.push(update.seat_id);
  }

  return { seats: [...nextSeats.values()], changed, conflict, invalidatedSeatIds };
}

export function reconcileSeatSnapshot(snapshotSeats, bufferedBatches) {
  let seats = snapshotSeats;
  const invalidatedSeatIds = new Set();
  let conflict = false;

  for (const batch of bufferedBatches) {
    const result = applySeatUpdates(seats, batch.seat_updates);
    seats = result.seats;
    conflict ||= result.conflict;
    for (const seatId of result.invalidatedSeatIds) invalidatedSeatIds.add(seatId);
  }

  return {
    seats,
    conflict,
    invalidatedSeatIds: [...new Set([
      ...snapshotUnavailableSeatIds(snapshotSeats),
      ...invalidatedSeatIds,
    ])],
  };
}

export function snapshotUnavailableSeatIds(snapshotSeats) {
  return snapshotSeats.filter((seat) => seat.status !== "available").map((seat) => seat.id);
}

export function invalidateUnavailableSelections(selectedSeats, authoritativeSeats, bookingInFlight = false) {
  if (bookingInFlight) return { remaining: selectedSeats, invalidated: [] };
  const available = new Set(authoritativeSeats.filter((seat) => seat.status === "available").map(seatID));
  const remaining = [];
  const invalidated = [];
  for (const selected of selectedSeats) {
    (available.has(selected.seatId) ? remaining : invalidated).push(selected);
  }
  return { remaining, invalidated };
}

export function flattenVenueSeats(layout) {
  return layout.sections.flatMap((section) => section.rows.flatMap((row) => row.seats));
}

export function applyUpdatesToVenueLayout(layout, updates) {
  const result = applySeatUpdates(flattenVenueSeats(layout), updates);
  const seatById = new Map(result.seats.map((seat) => [seatID(seat), seat]));
  const updatedIDs = new Set(updates.map((update) => update.seat_id));
  let layoutChanged = false;
  const sections = layout.sections.map((section) => {
    let sectionChanged = false;
    const rows = section.rows.map((row) => {
      let rowChanged = false;
      const seats = row.seats.map((seat) => {
        if (!updatedIDs.has(seat.id)) return seat;
        const authoritative = seatById.get(seat.id);
        if (!authoritative || (seat.status === authoritative.status && seatRevision(seat) === seatRevision(authoritative))) return seat;
        rowChanged = true;
        return {
          ...seat,
          status: authoritative.status,
          seatRevision: seatRevision(authoritative),
          updatedAt: authoritative.updatedAt ?? authoritative.updated_at,
        };
      });
      if (rowChanged) {
        sectionChanged = true;
        layoutChanged = true;
      }
      return rowChanged ? { ...row, seats } : row;
    });
    if (!sectionChanged) return section;
    const availableSeats = rows.reduce(
      (sum, row) => sum + row.seats.filter((seat) => seat.status === "available").length,
      0,
    );
    return { ...section, rows, availableSeats };
  });
  return { ...result, layout: layoutChanged ? { ...layout, sections } : layout };
}

function seatID(seat) {
  return seat.id ?? seat.seat_id;
}

function seatRevision(seat) {
  return seat.seatRevision ?? seat.seat_revision;
}

function isSeatUpdateBatch(value) {
  return Boolean(
    value &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    typeof value.message_id === "string" &&
    value.message_id.length > 0 &&
    Array.isArray(value.seat_updates) &&
    value.seat_updates.every((update) =>
      update &&
      typeof update === "object" &&
      typeof update.seat_id === "string" &&
      update.seat_id.length > 0 &&
      typeof update.ticket_tier_id === "string" &&
      ["available", "reserved", "booked"].includes(update.status) &&
      typeof update.seat_revision === "string" &&
      /^\d+$/.test(update.seat_revision) &&
      typeof update.updated_at === "string",
    ),
  );
}

export function createSeatRealtimeSession({ createSource, fetchSnapshot, onState, onError, readyTimeoutMs = 10000 }) {
  let source = null;
  let generation = 0;
  let closed = false;
  let phase = "CONNECTING";
  let seats = [];
  let buffer = [];
  let readyTimer;
  const seenMessages = new Set();

  const setState = (nextPhase, nextSeats = seats, details = {}) => {
    phase = nextPhase;
    seats = nextSeats;
    onState({ phase, seats, ...details });
  };

  const beginSnapshot = async () => {
    const currentGeneration = ++generation;
    buffer = [];
    setState("SNAPSHOT_LOADING");
    try {
      const snapshot = await fetchSnapshot();
      if (closed || currentGeneration !== generation) return;
      setState("RECONCILING");
      const reconciled = reconcileSeatSnapshot(snapshot.seats, buffer);
      buffer = [];
      if (reconciled.conflict) {
        setState("RESYNC_REQUIRED", seats, { reason: "equal_revision_conflict" });
        queueMicrotask(() => {
          if (!closed && currentGeneration === generation) void beginSnapshot();
        });
        return;
      }
      setState("LIVE", reconciled.seats, { snapshot: true, invalidatedSeatIds: reconciled.invalidatedSeatIds });
    } catch (error) {
      if (closed || currentGeneration !== generation) return;
      setState("RESYNC_REQUIRED", seats, { reason: "snapshot_failed" });
      onError?.(error);
    }
  };

  source = createSource();
  readyTimer = setTimeout(() => {
    if (!closed && phase === "CONNECTING") {
      onError?.(new Error("seat stream did not become ready"));
    }
  }, readyTimeoutMs);
  source.addEventListener("ready", () => {
    if (closed) return;
    clearTimeout(readyTimer);
    setState("STREAM_READY");
    void beginSnapshot();
  });
  source.addEventListener("seat-update", (event) => {
    if (closed) return;
    let batch;
    try {
      batch = JSON.parse(event.data);
    } catch (error) {
      setState("RESYNC_REQUIRED", seats, { reason: "malformed_delta" });
      onError?.(error);
      void beginSnapshot();
      return;
    }
    if (!isSeatUpdateBatch(batch)) {
      const error = new Error("Malformed seat-update batch.");
      setState("RESYNC_REQUIRED", seats, { reason: "malformed_delta" });
      onError?.(error);
      void beginSnapshot();
      return;
    }
    if (seenMessages.has(batch.message_id)) return;
    seenMessages.add(batch.message_id);
    if (seenMessages.size > 512) seenMessages.delete(seenMessages.values().next().value);

    if (phase !== "LIVE") {
      buffer.push(batch);
      return;
    }
    const result = applySeatUpdates(seats, batch.seat_updates);
    if (result.conflict) {
      void beginSnapshot();
      return;
    }
    if (result.changed) setState("LIVE", result.seats, { updates: batch.seat_updates, invalidatedSeatIds: result.invalidatedSeatIds });
  });
  source.onerror = () => {
    if (closed) return;
    generation++;
    buffer = [];
    phase = "CONNECTING";
    onState({ phase, seats });
  };

  return {
    close() {
      if (closed) return;
      closed = true;
      generation++;
      clearTimeout(readyTimer);
      source.close();
    },
    getState() {
      return { phase, seats, generation };
    },
  };
}
