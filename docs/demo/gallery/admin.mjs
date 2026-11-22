import { ids, event, sampleTime } from "./sample.mjs";

const bookingCreatedAt = "2027-05-19T18:40:00Z";
const ticketIssuedAt = "2027-05-19T18:42:00Z";
const refundRequestedAt = "2027-05-19T19:00:00Z";
const bookingOutboxAt = "2027-05-19T18:41:00Z";
const notificationOutboxAt = "2027-05-19T19:30:00Z";
const confirmedItemId = "00000000-0000-4000-8000-000000000201";
const schedulerJobId = "00000000-0000-4000-8000-000000000013";

const bookingRefund = {
  request_id: ids.refund,
  booking_id: ids.booking,
  event_id: ids.event,
  status: "REQUESTED",
  reason: "时间安排有变，希望申请退款。",
  decision_reason: "",
  refund_decision_id: "",
  amount_cents: 4800,
  currency: "USD",
  requested_at: refundRequestedAt,
  decided_at: "",
  payment_refund_state: "not_required",
  ticket_revocation_state: "NOT_STARTED",
  can_withdraw: true,
  approval_enabled: true,
  eligibility_code: "eligible",
};

const adminBookingDetail = {
  booking_id: ids.booking,
  user_id: ids.buyer,
  event_id: ids.event,
  booking_status: "CONFIRMED",
  total_amount_cents: 4800,
  currency: "USD",
  booking_mode: "pessimistic",
  lifecycle_generation: 1,
  lifecycle_phase: "TERMINAL",
  created_at: bookingCreatedAt,
  observed_at: sampleTime,
  items: [{
    id: confirmedItemId,
    ticket_tier_id: ids.tier,
    tier_name: event.tiers[0].name,
    quantity: 1,
    unit_price_cents: event.tiers[0].price_cents,
    seat_ids: [ids.seat],
  }],
  tickets: [{
    ticket_id: ids.ticket,
    status: "ISSUED",
    seat_id: ids.seat,
    ticket_tier_id: ids.tier,
    issued_at: ticketIssuedAt,
    checked_in_at: null,
    revoked_at: null,
  }],
  receipt: {
    outcome: "issued",
    reason_code: "issued",
    ticket_count: 1,
    receipt_count: 1,
    observed_at: sampleTime,
    created_at: ticketIssuedAt,
  },
  refund: {
    request_id: ids.refund,
    status: bookingRefund.status,
    reason: bookingRefund.reason,
    decision_reason: "",
    refund_decision_id: "",
    amount_cents: bookingRefund.amount_cents,
    currency: "USD",
    requested_at: refundRequestedAt,
    decided_at: null,
    payment_refund_state: "not_required",
    ticket_revocation_state: "NOT_STARTED",
    can_withdraw: true,
    approval_enabled: true,
  },
  source_service: "booking",
  coverage: "booking_owned_exact",
};

const bookingOutboxRecord = {
  event_id: ids.saga,
  created_at: bookingOutboxAt,
  consumer_outcome: "not_verified",
  source_service: "booking",
  event_type: "BookingConfirmed",
  recorded_status: "published",
};

const notificationOutboxRecord = {
  event_id: ids.payment,
  created_at: notificationOutboxAt,
  consumer_outcome: "not_verified",
  source_service: "notification",
  event_type: "booking_confirmation",
  recorded_status: "sent",
};

function cloneEvent() {
  return { ...event, tiers: event.tiers.map((tier) => ({ ...tier })) };
}

function queryValue(query, name, fallback = "") {
  return query instanceof URLSearchParams ? query.get(name) ?? fallback : fallback;
}

function outboxResponse(query) {
  const source = queryValue(query, "source", "booking") === "notification" ? "notification" : "booking";
  const record = source === "notification" ? notificationOutboxRecord : bookingOutboxRecord;
  const status = queryValue(query, "status", "all");
  const eventType = queryValue(query, "event_type");
  const from = queryValue(query, "from", "2027-05-19T03:00:00Z");
  const to = queryValue(query, "to", sampleTime);
  const recordTime = Date.parse(record.created_at);
  const fromTime = Date.parse(from);
  const toTime = Date.parse(to);
  const inWindow = !Number.isFinite(fromTime) || !Number.isFinite(toTime) || (recordTime >= fromTime && recordTime <= toTime);
  const matchesStatus = status === "all" || status === record.recorded_status;
  const matchesType = !eventType || eventType === record.event_type;
  const supportedEventTypes = source === "notification"
    ? ["booking_confirmation", "booking_failure", "booking_cancellation", "welcome_email", "Other"]
    : ["BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"];

  return {
    source_service: source,
    records: inWindow && matchesStatus && matchesType ? [{ ...record }] : [],
    next_cursor: "",
    has_more: false,
    observed_at: sampleTime,
    window: { from, to },
    supported_event_types: supportedEventTypes,
    source_record_limit: 5000,
    consumer_outcome: "not_verified",
  };
}

function eventListResponse(query) {
  const category = queryValue(query, "category").toLowerCase();
  const search = queryValue(query, "search").trim().toLowerCase();
  const matchesCategory = !category || category === event.category.toLowerCase();
  const searchText = [event.title, event.description, event.venue, event.location].join(" ").toLowerCase();
  const matchesSearch = !search || searchText.includes(search);
  const matches = matchesCategory && matchesSearch;
  const page = Math.max(1, Number(queryValue(query, "page", "1")) || 1);
  const pageSize = Math.min(100, Math.max(1, Number(queryValue(query, "page_size", "20")) || 20));

  return {
    events: matches && page === 1 ? [cloneEvent()] : [],
    total: matches ? 1 : 0,
    page,
    page_size: pageSize,
  };
}

function ticketOperationsResponse() {
  return {
    event_id: ids.event,
    event_title: event.title,
    event_venue: event.venue,
    event_date: event.date,
    observed_at: sampleTime,
    source_service: "booking",
    source_status: "available",
    coverage: "recorded_ticket_snapshot",
    total_recorded_count: 1,
    issued_count: 1,
    checked_in_count: 0,
    revoked_count: 0,
    successful_checkin_count: 0,
    latest_checkin_at: null,
    recent_checkins: [],
    delivery_exceptions: [],
    source_record_limit: 5000,
    live_admission_eligibility: "not_evaluated",
  };
}

function eventTicketsResponse(query) {
  const status = queryValue(query, "status", "ALL");
  const pageSize = Math.min(100, Math.max(1, Number(queryValue(query, "page_size", "20")) || 20));
  const ticket = {
    ticket_id: ids.ticket,
    status: "ISSUED",
    issued_at: ticketIssuedAt,
    checked_in_at: null,
    revoked_at: null,
  };

  return {
    event_id: ids.event,
    observed_at: sampleTime,
    source_service: "booking",
    source_status: "available",
    coverage: "recorded_ticket_snapshot",
    source_record_limit: 5000,
    status_filter: status,
    page_size: pageSize,
    has_more: false,
    next_cursor: null,
    tickets: status === "ALL" || status === "ISSUED" ? [ticket] : [],
  };
}

function operationsResponse(query) {
  const from = queryValue(query, "from", "2027-05-13T03:00:00Z");
  const to = queryValue(query, "to", sampleTime);
  const offset = Math.max(0, Number(queryValue(query, "offset", "0")) || 0);
  const limit = Math.max(1, Number(queryValue(query, "limit", "50")) || 50);
  const inWindow = Date.parse(bookingCreatedAt) >= Date.parse(from) && Date.parse(bookingCreatedAt) <= Date.parse(to);
  const items = inWindow && offset === 0 ? [{
    booking_id: ids.booking,
    event_id: ids.event,
    booking_status: "CONFIRMED",
    created_at: bookingCreatedAt,
    ticket_count: 1,
    refund_status: "REQUESTED",
    payment_source_state: "available",
    issues: [{
      kind: "refund_request",
      source: "booking",
      state: "REQUESTED",
      observed_at: sampleTime,
      next_action: "审核这笔退款申请。",
    }],
    detail_path: `/admin/orders/${ids.booking}`,
  }] : [];

  return {
    items,
    has_more: false,
    offset,
    limit,
    from,
    to,
    observed_at: sampleTime,
    scope: "有界的管理员读取样例",
    source: "booking",
    notification_path: "/admin/outbox?source=notification",
    notification_source_state: "available",
    notification_items: [],
    notification_has_more: false,
    notification_observed_at: sampleTime,
  };
}

function metricsResponse(query) {
  return {
    from: queryValue(query, "from", "2027-05-13T03:00:00Z"),
    to: queryValue(query, "to", sampleTime),
    record_limit: 10000,
    scope: "按查询时间范围汇总的样例记录",
    booking: {
      source: "booking",
      observed_at: sampleTime,
      orders: 2,
      issued_tickets: 1,
      checked_in_tickets: 0,
      refund_requests: 1,
      confirmed_refunds: 0,
    },
    payment: {
      source: "payment",
      source_state: "available",
      unavailable_reason: "",
      observed_at: sampleTime,
      groups: [{
        currency: "USD",
        mode: "test",
        state: "recorded_complete",
        payments: 1,
        unknown_payments: 0,
        confirmed_refunds: 0,
        collected_known_minor: event.tiers[0].price_cents,
        refunded_known_minor: 0,
        net_minor: event.tiers[0].price_cents,
      }],
    },
  };
}

async function waitForHeading(page, title) {
  await page.getByRole("heading", { name: title, exact: true }).first().waitFor({ state: "visible" });
}

async function waitForText(page, value) {
  await page.getByText(value, { exact: true }).first().waitFor({ state: "visible" });
}

export const adminRoutes = [
  {
    route: "/admin",
    url: "/admin",
    slug: "admin-overview",
    role: "admin",
    title: "总览",
    purpose: "查看管理员数据源的近期读取结果和管理入口。",
    features: ["读取活动和定时任务概况", "并列查看 Booking 与通知发件箱", "前往活动、任务和发件箱管理"],
    state: "展示一个活动、一个定时任务，以及已发布的订单确认和通知适配器回执。",
    source: "fixture",
    ready: async (page) => {
      await waitForHeading(page, "总览");
      await waitForText(page, "BookingConfirmed");
    },
  },
  {
    route: "/admin/events",
    url: "/admin/events",
    slug: "admin-events",
    role: "admin",
    title: "活动",
    purpose: "检索活动列表并查看票档、售票状态和库存。",
    features: ["按关键词和分类筛选", "按页查看活动记录", "打开活动运营和退款入口"],
    state: "海港爵士之夜正在售票，池座余量为 23 张。",
    source: "fixture",
    ready: async (page) => {
      await waitForHeading(page, "活动");
      await waitForText(page, event.title);
    },
  },
  {
    route: "/admin/events/[id]/refunds",
    url: `/admin/events/${ids.event}/refunds`,
    slug: "admin-event-refunds",
    role: "admin",
    title: "活动退款审批",
    purpose: "查看该活动的退款申请、审批权限和申请状态。",
    features: ["按活动查看退款队列", "核对申请金额和退款状态", "查看活动退款审批人员"],
    state: "一笔 48 美元申请正在等待审批，票券尚未核销，未执行退款。",
    source: "fixture",
    ready: async (page) => {
      await waitForHeading(page, "活动退款审批");
      await waitForText(page, ids.booking);
    },
  },
  {
    route: "/admin/events/[id]/operations",
    url: `/admin/events/${ids.event}/operations`,
    slug: "admin-event-operations",
    role: "admin",
    title: "活动运营",
    purpose: "查看活动已记录的售票、票券和核销快照。",
    features: ["统计已签发、已核销和已撤销票券", "查看单张票券的记录状态", "打开活动退款队列"],
    state: "已签发 1 张票券，座位 A1 尚未入场核销；退款申请仍待审批。",
    source: "fixture",
    ready: async (page) => {
      await waitForHeading(page, event.title);
      await waitForText(page, event.venue);
      await waitForText(page, ids.ticket);
    },
  },
  {
    route: "/admin/orders",
    url: "/admin/orders",
    slug: "admin-order-lookup",
    role: "admin",
    title: "查询订单",
    purpose: "使用完整 Booking ID 精确查询一笔订单和售后详情。",
    features: ["输入完整订单 UUID 后打开详情", "校验不完整或格式错误的 UUID", "从订单详情返回查询表单"],
    state: "表单已预填样例订单 UUID；尚未提交查询。",
    source: "fixture",
    ready: async (page) => {
      await waitForHeading(page, "查询订单");
      const bookingId = page.locator("#admin-booking-id");
      await bookingId.waitFor({ state: "visible" });
      await bookingId.fill(ids.booking);
    },
  },
  {
    route: "/admin/orders/[id]",
    url: `/admin/orders/${ids.booking}`,
    slug: "admin-order-detail",
    role: "admin",
    title: "订单与售后详情",
    purpose: "联查订单归属、票档、座位、出票记录和退款状态。",
    features: ["展示订单确认状态与金额", "核对 A1 票券的出票和入场记录", "查看待审批退款及其边界说明"],
    state: "林澄的订单已确认，池座 A1 已出票且未核销；退款申请正在等待审批。",
    source: "fixture",
    ready: async (page) => {
      await waitForHeading(page, "订单与售后详情");
      await waitForText(page, event.title);
      await waitForText(page, ids.ticket);
    },
  },
  {
    route: "/admin/operations",
    url: "/admin/operations",
    slug: "admin-exceptions-refunds",
    role: "admin",
    title: "异常与退款",
    purpose: "按时间范围查看服务报告的订单、退款和通知异常。",
    features: ["区分订单付款与消息数据源", "显示观察时间和后续处理建议", "跳转到订单详情或退款记录"],
    state: "订单与付款数据源报告一笔待审批退款；通知异常列表为空且数据源可用。",
    source: "fixture",
    ready: async (page) => {
      await waitForHeading(page, "异常与退款");
      await waitForText(page, ids.booking);
    },
  },
  {
    route: "/admin/outbox",
    url: "/admin/outbox?source=notification",
    slug: "admin-outbox",
    role: "admin",
    title: "发件箱事件",
    purpose: "按来源、状态和时间范围查看发件箱的已记录事件。",
    features: ["筛选 Booking 或通知来源", "明确区分来源记录状态和下游消费结果", "按时间窗口分页读取"],
    state: "通知服务记录了一条 booking_confirmation；适配器已接受不代表下游消费已验证。",
    source: "fixture",
    ready: async (page) => {
      await waitForHeading(page, "发件箱事件");
      await page.locator("tbody tr").filter({ hasText: "booking_confirmation" }).waitFor({ state: "visible" });
    },
  },
  {
    route: "/admin/ops",
    url: "/admin/ops",
    slug: "admin-scheduler-jobs",
    role: "admin",
    title: "定时任务",
    purpose: "查看 scheduler-service 中的定时任务配置和运行计划。",
    features: ["检查六字段 Cron 表达式", "查看任务版本、超时和状态", "筛选任务并查看创建入口"],
    state: "预约过期清理任务有效并处于排期状态。",
    source: "fixture",
    ready: async (page) => {
      await waitForHeading(page, "定时任务");
      await waitForText(page, "预约过期清理");
    },
  },
  {
    route: "/admin/flags",
    url: "/admin/flags",
    slug: "admin-feature-flags",
    role: "admin",
    title: "功能开关",
    purpose: "查看允许列表中的展示功能设置和变更记录。",
    features: ["读取活动发现高级筛选开关", "显示数据源与观测时间", "查看近期审计记录"],
    state: "公开活动发现高级筛选已停用；没有历史变更记录。",
    source: "fixture",
    ready: async (page) => {
      await waitForHeading(page, "功能开关");
      await waitForText(page, "discovery_advanced_filters");
    },
  },
  {
    route: "/admin/metrics",
    url: "/admin/metrics",
    slug: "admin-metrics",
    role: "admin",
    title: "运营指标",
    purpose: "在限定时间范围内查看订单、票务、退款和付款汇总。",
    features: ["分别标记 Booking 和 Payment 数据源", "按币种及 TEST 模式呈现已知金额", "应用范围并导出 CSV"],
    state: "时间范围内记录 2 笔订单、1 张已签发票券和 1 笔 48 美元 TEST 收款；一笔退款申请待处理。",
    source: "fixture",
    ready: async (page) => {
      await waitForHeading(page, "运营指标");
      await waitForText(page, "订单数据源");
      await waitForText(page, "USD · TEST");
    },
  },
];

/** Return an immutable synthetic payload for an admin GET fixture, if handled. */
export function adminResponse({ path, query = new URLSearchParams(), role }) {
  if (role !== "admin") return undefined;

  if (path === "/api/admin/events") return eventListResponse(query);
  if (path === "/api/scheduler/jobs") {
    return {
      schedulers: [{
        id: schedulerJobId,
        name: "预约过期清理",
        job_type: "reservation-cleaner-job",
        cron_expression: "0 */5 * * * *",
        is_enabled: true,
        valid: true,
        validation_error: "",
        version: 1,
        timeout: 60,
        created_at: "2027-04-01T01:00:00Z",
        updated_at: "2027-05-19T18:00:00Z",
      }],
    };
  }
  if (path === "/api/admin/outbox-events") return outboxResponse(query);
  if (path === "/api/admin/operations") return operationsResponse(query);
  if (path === "/api/admin/feature-flags") {
    return {
      flags: [{
        key: "discovery_advanced_filters",
        description: "在公开活动发现页面显示高级筛选。搜索与活动访问仍由服务器决定。",
        default_enabled: false,
        enabled: false,
        version: 1,
        updated_at: null,
        actor_id: ids.admin,
      }],
      audit: [],
      audit_limit: 20,
      observed_at: sampleTime,
      source: "feature_flags_fixture",
    };
  }
  if (path === "/api/admin/metrics") return metricsResponse(query);

  if (path === `/api/admin/events/${ids.event}/engagement`) {
    return { favorites: 1, onsale: 0, restock: 0 };
  }
  if (path === `/api/admin/events/${ids.event}/ticket-operations`) return ticketOperationsResponse();
  if (path === `/api/admin/events/${ids.event}/tickets`) return eventTicketsResponse(query);
  if (path === `/api/admin/events/${ids.event}/refund-approvers`) {
    return { assignments: [{ event_id: ids.event, staff_user_id: ids.staff, active: true, changed_at: sampleTime }] };
  }
  if (path === `/api/admin/bookings/${ids.booking}`) return { ...adminBookingDetail };
  if (path === `/api/events/${ids.event}/refund-requests`) {
    return {
      refunds: [{ ...bookingRefund }],
      next_cursor: "",
      approval_enabled: true,
      approval_block_reason: "",
    };
  }

  return undefined;
}
