import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

test("event date update conflicts retain their typed backend codes", () => {
  const { responseError } = loadTypeScript("lib/api/errors.ts");

  assert.equal(
    responseError("/events/event-1", 409, "date changed", "EVENT_DATE_CONFLICT").code,
    "EVENT_DATE_CONFLICT",
  );
  assert.equal(
    responseError("/events/event-1", 409, "refund approval pending", "EVENT_REFUND_TIME_PENDING").code,
    "EVENT_REFUND_TIME_PENDING",
  );
});
