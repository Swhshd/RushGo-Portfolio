import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";
import { ManualScheduler } from "./manual-time.mjs";

const { withRequestDeadline, ADMIN_READ_TIMEOUT_MS } = loadTypeScript("lib/request-deadline.ts");

test("a held read aborts and rejects at the configured admin read deadline", async () => {
  const scheduler = new ManualScheduler();
  const controller = new AbortController();
  const held = new Promise(() => {});
  const bounded = withRequestDeadline(held, controller, ADMIN_READ_TIMEOUT_MS, scheduler);

  assert.deepEqual(scheduler.pendingDelays(), [10000]);
  scheduler.runAll();

  await assert.rejects(bounded, (error) => error.name === "TimeoutError");
  assert.equal(controller.signal.aborted, true);
  assert.deepEqual(scheduler.pendingDelays(), []);
});

test("a completed read clears its deadline without aborting the caller's signal", async () => {
  const scheduler = new ManualScheduler();
  const controller = new AbortController();

  const result = await withRequestDeadline(
    Promise.resolve("read complete"),
    controller,
    ADMIN_READ_TIMEOUT_MS,
    scheduler,
  );

  assert.equal(result, "read complete");
  assert.deepEqual(scheduler.pendingDelays(), []);
  assert.equal(controller.signal.aborted, false);
});

test("caller cancellation ends a held read and clears its deadline", async () => {
  const scheduler = new ManualScheduler();
  const controller = new AbortController();
  const held = new Promise(() => {});
  const bounded = withRequestDeadline(held, controller, ADMIN_READ_TIMEOUT_MS, scheduler);

  controller.abort();

  await assert.rejects(bounded, (error) => error.name === "AbortError");
  assert.deepEqual(scheduler.pendingDelays(), []);
});
