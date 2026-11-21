import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

test("Tab wrap-around stays inside the dialog and reverses with Shift", () => {
  const { nextFocusIndex } = loadTypeScript("lib/dialog-focus.ts");

  assert.equal(nextFocusIndex(0, 3, false), 1);
  assert.equal(nextFocusIndex(1, 3, false), 2);
  assert.equal(nextFocusIndex(2, 3, false), 0);

  assert.equal(nextFocusIndex(0, 3, true), 2);
  assert.equal(nextFocusIndex(2, 3, true), 1);
  assert.equal(nextFocusIndex(-1, 3, false), 0);
  assert.equal(nextFocusIndex(-1, 3, true), 2);

  // A single focusable element keeps focus instead of escaping the dialog.
  assert.equal(nextFocusIndex(0, 1, false), 0);
  assert.equal(nextFocusIndex(0, 1, true), 0);

  assert.equal(nextFocusIndex(0, 0, false), -1);
});
