import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { loadTypeScript } from "./render-typescript.mjs";

const { getI18n } = loadTypeScript("lib/i18n/core.ts");

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function sameDependencies(left, right) {
  return left === right || (
    Array.isArray(left) &&
    Array.isArray(right) &&
    left.length === right.length &&
    left.every((value, index) => Object.is(value, right[index]))
  );
}

// This synthetic hook harness exercises page state and effect ownership. The
// isolated browser checks own actual React StrictMode and unmount coverage.
function pageDriver(apiClient, localeState = { current: "en-US" }) {
  const slots = [];
  const pendingEffects = [];
  const activeEffects = new Map();
  let cursor = 0;
  let mounted = true;
  let stateWritesAfterUnmount = 0;

  function memoize(factory, dependencies) {
    const index = cursor++;
    const current = slots[index];
    if (current?.kind === "memo" && sameDependencies(current.dependencies, dependencies)) {
      return current.value;
    }
    const value = factory();
    slots[index] = { kind: "memo", dependencies, value };
    return value;
  }

  const hooks = {
    ...React,
    useState(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = typeof initial === "function" ? initial() : initial;
      return [
        slots[index],
        (value) => {
          if (!mounted) stateWritesAfterUnmount += 1;
          slots[index] = typeof value === "function" ? value(slots[index]) : value;
        },
      ];
    },
    useRef(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = { current: initial };
      return slots[index];
    },
    useMemo(factory, dependencies) {
      return memoize(factory, dependencies);
    },
    useCallback(callback, dependencies) {
      return memoize(() => callback, dependencies);
    },
    useEffect(setup, dependencies) {
      const index = cursor++;
      const current = slots[index];
      if (current?.kind !== "effect" || !sameDependencies(current.dependencies, dependencies)) {
        slots[index] = { kind: "effect", dependencies };
        pendingEffects.push({ index, setup });
      }
    },
  };

  const { default: Page } = loadTypeScript("app/admin/ops/page.tsx", {
    react: hooks,
    "@/lib/api/client": { apiClient },
    "@/lib/i18n": {
      useI18n: () => ({ ...getI18n(localeState.current), setLocale: (locale) => { localeState.current = locale; } }),
    },
  });

  return {
    render() {
      cursor = 0;
      return Page();
    },
    runEffects() {
      const pending = pendingEffects.splice(0);
      for (const effect of pending) {
        activeEffects.get(effect.index)?.cleanup?.();
        const result = effect.setup();
        activeEffects.set(effect.index, {
          setup: effect.setup,
          cleanup: typeof result === "function" ? result : null,
        });
      }
    },
    replayEffects() {
      const effects = [...activeEffects.values()];
      for (const effect of effects) effect.cleanup?.();
      for (const effect of effects) {
        const result = effect.setup();
        effect.cleanup = typeof result === "function" ? result : null;
      }
    },
    unmount() {
      mounted = false;
      for (const effect of activeEffects.values()) effect.cleanup?.();
      activeEffects.clear();
    },
    stateWritesAfterUnmount: () => stateWritesAfterUnmount,
  };
}

function find(tree, predicate) {
  if (!tree || typeof tree !== "object") return null;
  if (predicate(tree)) return tree;
  for (const child of React.Children.toArray(tree.props?.children)) {
    const found = find(child, predicate);
    if (found) return found;
  }
  return null;
}

function textContent(node) {
  if (node === null || node === undefined || typeof node === "boolean") return "";
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(textContent).join("");
  return node.props ? textContent(node.props.children) : "";
}

function hasText(tree, expected) {
  if (tree === expected) return true;
  if (!tree || typeof tree !== "object") return false;
  return React.Children.toArray(tree.props?.children).some((child) => hasText(child, expected));
}

function button(tree, label) {
  return find(
    tree,
    (node) => node.type === "button" && textContent(node.props.children).includes(label),
  );
}

function job(name) {
  return {
    id: "job-" + name.toLowerCase().replaceAll(" ", "-"),
    name,
    cron_expression: "0 */5 * * * *",
    timeout: 30,
    version: 4,
    is_enabled: true,
    job_type: "reservation-cleaner-job",
    valid: true,
    validation_error: "",
    updated_at: "2026-10-31T00:00:00Z",
  };
}

function requestQueue() {
  const requests = [];
  const apiClient = {
    listSchedulerJobs() {
      const request = deferred();
      requests.push(request);
      return request.promise;
    },
  };
  return { apiClient, requests };
}

test("scheduler page separates loading, request failure, retry, and an empty response", async () => {
  const fixture = requestQueue();
  const driver = pageDriver(fixture.apiClient);

  assert(hasText(driver.render(), "Loading scheduler jobs…"));
  driver.runEffects();
  assert.equal(fixture.requests.length, 1);

  fixture.requests[0].reject(new Error("Synthetic scheduler outage"));
  await assert.rejects(fixture.requests[0].promise, /Synthetic scheduler outage/);

  const failed = driver.render();
  assert(hasText(failed, "Scheduler jobs could not be loaded. Retry the read."));
  assert.equal(button(failed, "Retry") !== null, true);
  assert.equal(hasText(failed, "No scheduler job configs found."), false);

  button(failed, "Retry").props.onClick();
  assert.equal(fixture.requests.length, 2);
  const retrying = driver.render();
  assert(hasText(retrying, "Scheduler jobs could not be loaded. Retry the read."));
  assert.equal(hasText(retrying, "No scheduler job configs found."), false);

  fixture.requests[1].resolve({ schedulers: [] });
  await fixture.requests[1].promise;

  const empty = driver.render();
  assert.equal(hasText(empty, "Scheduler jobs could not be loaded. Retry the read."), false);
  assert(hasText(empty, "No scheduler job configs found."));
});

test("StrictMode effect replay ignores an earlier scheduler list response", async () => {
  const fixture = requestQueue();
  const driver = pageDriver(fixture.apiClient);

  driver.render();
  driver.runEffects();
  driver.replayEffects();
  assert.equal(fixture.requests.length, 2, "repeatable GET may run for each StrictMode setup");

  fixture.requests[1].resolve({ schedulers: [job("Current scheduler")] });
  await fixture.requests[1].promise;
  assert(hasText(driver.render(), "Current scheduler"));

  fixture.requests[0].resolve({ schedulers: [job("Stale scheduler")] });
  await fixture.requests[0].promise;

  const current = driver.render();
  assert(hasText(current, "Current scheduler"));
  assert.equal(hasText(current, "Stale scheduler"), false);
});

test("unmount cleanup prevents an old scheduler response from writing page state", async () => {
  const fixture = requestQueue();
  const driver = pageDriver(fixture.apiClient);

  driver.render();
  driver.runEffects();
  driver.unmount();

  fixture.requests[0].resolve({ schedulers: [job("Late scheduler")] });
  await fixture.requests[0].promise;

  assert.equal(driver.stateWritesAfterUnmount(), 0);
});

test("cron job status and copy rerender in Chinese without refetching", async () => {
  const fixture = requestQueue();
  const localeState = { current: "en-US" };
  const driver = pageDriver(fixture.apiClient, localeState);
  driver.render();
  driver.runEffects();
  fixture.requests[0].resolve({ schedulers: [job("Every Five Minutes")] });
  await fixture.requests[0].promise;

  const english = driver.render();
  assert(hasText(english, "Cron Jobs"));
  assert(hasText(english, "Scheduled"));
  localeState.current = "zh-CN";
  const chinese = driver.render();
  assert(hasText(chinese, "定时任务"));
  assert(hasText(chinese, "已排期"));
  assert(hasText(chinese, "每 5 分钟"));
  assert(hasText(chinese, "Every Five Minutes"));
  assert.equal(fixture.requests.length, 1);
  driver.unmount();
});
