import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

const view = () => loadTypeScript("lib/admin-outbox-view.ts");

test("outbox view helpers localize source names, recorded statuses, scopes, and safe errors", () => {
  const {
    outboxSourceName,
    outboxRecordedStatusLabel,
    outboxStatusScopeLabel,
    safeAdminOutboxReadErrorMessage,
  } = view();

  assert.equal(outboxSourceName("notification", "zh-CN"), "通知意图");
  assert.equal(outboxSourceName("booking", "zh-CN"), "Booking 发件箱");
  assert.equal(outboxRecordedStatusLabel("notification", "sent", "zh-CN"), "适配器已接受");
  assert.equal(outboxRecordedStatusLabel("booking", "published", "zh-CN"), "已发布");
  assert.match(outboxStatusScopeLabel("notification", "zh-CN"), /支持的记录状态/);
  assert.equal(
    safeAdminOutboxReadErrorMessage({ code: "OUTBOX_CAPACITY_EXCEEDED" }, "first-page", "booking", "zh-CN"),
    "已达到查询上限。Booking 发件箱超出安全读取容量，不会假定结果为空。",
  );
});

test("pending ages localize sub-minute copy, numbers, and duration units", () => {
  const { formatPendingAge } = view();
  const createdAt = "2026-10-31T15:00:00Z";

  assert.equal(formatPendingAge(createdAt, "2026-10-31T15:00:59Z", "en-US"), "less than 1m");
  assert.equal(formatPendingAge(createdAt, "2026-10-31T15:00:59Z", "zh-CN"), "不足 1 分钟");
  assert.equal(formatPendingAge(createdAt, "2026-11-01T17:03:00Z", "zh-CN"), "1天 2小时 3分钟");
});
