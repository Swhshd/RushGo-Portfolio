import { event, ids, sampleTime } from "./sample.mjs";

const tier = event.tiers[0];
const amountCents = tier.price_cents;
const a2SeatId = "00000000-0000-4000-8000-000000000102";
const confirmedItemId = "00000000-0000-4000-8000-000000000201";
const pendingItemId = "00000000-0000-4000-8000-000000000202";
const pendingBookingId = ids.pendingBooking;

async function waitFor(locator) {
  await locator.waitFor({ state: "visible", timeout: 10000 });
}

function apiSeat(number) {
  const rowIndex = Math.floor((number - 1) / 6);
  const seatInRow = ((number - 1) % 6) + 1;
  return {
    id: number === 1 ? ids.seat : number === 2 ? a2SeatId : `00000000-0000-4000-8000-${String(100 + number).padStart(12, "0")}`,
    event_id: ids.event,
    ticket_tier_id: tier.id,
    status: number === 1 ? "booked" : "available",
    position: {
      sectionId: `${tier.name}-center`,
      row: String.fromCharCode(65 + rowIndex),
      seat: seatInRow,
      x: 285 + ((seatInRow - 1) * 36),
      y: 345 + (rowIndex * 36),
    },
    created_at: sampleTime,
    updated_at: sampleTime,
    seat_revision: String(number),
  };
}

function confirmedBooking() {
  return {
    id: ids.booking,
    user_id: ids.buyer,
    event_id: ids.event,
    status: "CONFIRMED",
    total_amount_cents: amountCents,
    currency: "USD",
    items: [{
      id: confirmedItemId,
      ticket_tier_id: tier.id,
      tier_name: tier.name,
      quantity: 1,
      unit_price_cents: amountCents,
      seat_ids: [ids.seat],
    }],
    created_at: "2027-05-19T18:40:00Z",
  };
}

function pendingBooking() {
  return {
    id: pendingBookingId,
    user_id: ids.buyer,
    event_id: ids.event,
    status: "PENDING",
    total_amount_cents: amountCents,
    currency: "USD",
    items: [{
      id: pendingItemId,
      ticket_tier_id: tier.id,
      tier_name: tier.name,
      quantity: 1,
      unit_price_cents: amountCents,
      seat_ids: [a2SeatId],
    }],
    created_at: "2027-05-20T02:55:00Z",
  };
}

function pendingCheckoutSnapshot() {
  return {
    ...pendingBooking(),
    checkout_state: "processing",
    reservation_deadline: "2027-05-20T03:10:00Z",
    can_cancel: false,
  };
}

function issuedTicket() {
  return {
    id: ids.ticket,
    booking_id: ids.booking,
    event_id: ids.event,
    seat_id: ids.seat,
    ticket_tier_id: tier.id,
    status: "ISSUED",
    booking_status: "CONFIRMED",
    issued_at: "2027-05-19T18:42:00Z",
    checked_in_at: null,
  };
}

function refundSnapshot() {
  return {
    request_id: ids.refund,
    booking_id: ids.booking,
    event_id: ids.event,
    status: "REQUESTED",
    reason: "时间安排有变，希望申请退款。",
    decision_reason: "",
    refund_decision_id: "",
    amount_cents: amountCents,
    currency: "USD",
    requested_at: "2027-05-19T19:00:00Z",
    decided_at: "",
    payment_refund_state: "not_required",
    ticket_revocation_state: "NOT_STARTED",
    can_withdraw: true,
    approval_enabled: true,
    eligibility_code: "eligible",
  };
}

export const customerRoutes = [
  {
    route: "/",
    url: "/",
    slug: "customer-discover",
    role: "guest",
    title: "活动发现",
    purpose: "查看公开活动、票档价格和场馆信息。",
    features: ["海港爵士之夜活动卡片", "公开活动搜索与分类入口", "活动日期、场馆和票价摘要"],
    state: "中文只读活动样例；不涉及账号、预订或支付。",
    source: "fixture",
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: event.title }).first());
      await waitFor(page.getByRole("link", { name: "购票" }));
    },
  },
  {
    route: "/events/[id]",
    url: `/events/${ids.event}`,
    slug: "customer-event-detail",
    role: "guest",
    title: "活动详情",
    purpose: "展示活动介绍、时间地点、票档价格和选座入口。",
    features: ["活动介绍与场馆信息", "池座价格及可售数量", "前往座位图的入口"],
    state: "只读公开活动数据；选座后仍需服务端确认实时库存。",
    source: "fixture",
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: event.title }));
      await waitFor(page.getByRole("heading", { name: "票档" }));
      await waitFor(page.getByText(tier.name, { exact: true }));
    },
  },
  {
    route: "/events/[id]/seats",
    url: `/events/${ids.event}/seats`,
    slug: "customer-seat-selection",
    role: "buyer",
    title: "在线选座",
    purpose: "查看由座位快照和实时流共同确认的座位状态，并选中一个可售座位。",
    features: ["24 个池座位置", "A1 已售出、A2 可选", "选中座位和金额摘要", "实时库存连接状态"],
    state: "展示 A2 的本地样例选择；未继续结账、创建订单或预留座位。",
    source: "fixture",
    interact: async (page) => {
      const section = page.getByRole("button", { name: /选择.*池座/ });
      await waitFor(section);
      await section.click();
      const a2 = page.getByRole("button", { name: /第\s*A\s*排.*2\s*号座位.*可选/ });
      await waitFor(a2);
      await a2.click();
    },
    ready: async (page) => {
      await waitFor(page.getByText(event.title, { exact: true }).first());
      await waitFor(page.getByText("您选择的座位", { exact: true }));
      await waitFor(page.getByRole("button", { name: /第\s*A\s*排.*2\s*号座位.*已选择/ }));
    },
  },
  {
    route: "/checkout",
    url: `/checkout?${new URLSearchParams({ booking_id: pendingBookingId })}`,
    slug: "customer-checkout-status",
    role: "buyer",
    title: "继续现有订单",
    purpose: "查看既有订单的付款处理状态和安全的后续查询入口。",
    features: ["订单号和订单金额", "付款处理中状态", "查看订单和重新查询入口"],
    state: "仅回放 A2 预留前订单的处理中快照；本地拦截恢复请求，不转发到服务端，不提供 Stripe 表单或支付凭据。",
    source: "fixture",
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: "继续现有订单" }));
      await waitFor(page.getByText("付款仍在处理中。再次尝试前请先重新查询。", { exact: true }));
      await waitFor(page.getByText(pendingBookingId).first());
    },
  },
  {
    route: "/login",
    url: "/login?redirect=%2Fmy-tickets",
    slug: "customer-login",
    role: "guest",
    title: "登录",
    purpose: "查看登录表单和安全的返回路径。",
    features: ["邮箱和密码输入", "忘记密码入口", "注册入口"],
    state: "仅展示空白表单；不提交登录信息，也不创建会话。",
    source: "form",
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: "登录" }));
      await waitFor(page.getByLabel("邮箱"));
      await waitFor(page.getByLabel("密码"));
    },
  },
  {
    route: "/register",
    url: "/register",
    slug: "customer-register",
    role: "guest",
    title: "创建账号",
    purpose: "查看注册、邮箱验证码和密码规则的表单入口。",
    features: ["姓名与邮箱字段", "获取注册验证码入口", "密码确认字段"],
    state: "仅展示未提交的注册表单；未请求验证码或创建账号。",
    source: "form",
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: "创建账号" }));
      await waitFor(page.getByLabel("姓名"));
      await waitFor(page.getByRole("button", { name: "发送验证码" }));
    },
  },
  {
    route: "/forgot-password",
    url: "/forgot-password",
    slug: "customer-forgot-password",
    role: "guest",
    title: "忘记密码",
    purpose: "查看密码找回请求所需的邮箱表单。",
    features: ["邮箱输入", "提交找回请求的入口", "返回登录入口"],
    state: "仅展示未提交表单；未请求或发送密码重置邮件。",
    source: "form",
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: "忘记密码" }));
      await waitFor(page.getByLabel("邮箱"));
      await waitFor(page.getByRole("button", { name: "发送重置链接" }));
    },
  },
  {
    route: "/reset-password",
    url: "/reset-password",
    slug: "customer-reset-password",
    role: "guest",
    title: "重置密码",
    purpose: "展示安全的重置表单边界和重新获取有效邮件链接的入口。",
    features: ["新密码与确认输入", "密码长度提示", "获取新重置链接入口"],
    state: "不使用可验证的重置 token；表单保持禁用，不提交密码变更。",
    source: "form",
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: "重置密码" }));
      await waitFor(page.getByText("请打开邮件中的最新密码重置链接。", { exact: true }));
      await waitFor(page.getByRole("button", { name: "重置密码" }));
    },
  },
  {
    route: "/account",
    url: "/account",
    slug: "customer-account",
    role: "buyer",
    title: "账户设置",
    purpose: "查看当前账户资料、语言偏好和安全设置入口。",
    features: ["林澄的账户资料", "简体中文语言偏好", "密码与邮箱更新表单"],
    state: "资料由只读用户快照填充；不保存资料、不请求验证码或变更安全凭据。",
    source: "fixture",
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: "账户设置" }));
      const name = page.getByLabel("姓名");
      await waitFor(name);
      if (await name.inputValue() !== "林澄") throw new Error("Buyer profile fixture was not rendered.");
    },
  },
  {
    route: "/aftersales",
    url: "/aftersales",
    slug: "customer-aftersales",
    role: "buyer",
    title: "售后服务",
    purpose: "说明买家订单退款入口与活动审批人工作入口。",
    features: ["个人订单和退款导航", "活动退款审批入口", "审批链接不授予权限的提示"],
    state: "仅展示导航和空白活动编号表单；没有提交退款申请或审批决定。",
    source: "form",
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: "售后服务" }));
      await waitFor(page.getByRole("heading", { name: "我的订单与退款" }));
      await waitFor(page.getByRole("heading", { name: "审核活动退款" }));
    },
  },
  {
    route: "/favorites",
    url: "/favorites",
    slug: "customer-favorites",
    role: "buyer",
    title: "收藏与提醒",
    purpose: "查看已收藏活动和提醒偏好管理入口。",
    features: ["已收藏活动卡片", "活动详情入口", "提醒和收藏管理控件"],
    state: "只读收藏快照；不会取消收藏、订阅或变更邮件偏好。",
    source: "fixture",
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: "收藏与提醒" }));
      await waitFor(page.getByRole("heading", { name: event.title }));
    },
  },
  {
    route: "/my-tickets",
    url: "/my-tickets",
    entryUrl: "/",
    slug: "customer-ticket-wallet",
    role: "buyer",
    title: "我的票券",
    purpose: "查看已签发票券卡片和两条活动订单摘要。",
    features: ["已签发入场票券卡片", "A1 已确认与 A2 待处理订单", "订单详情与退款入口"],
    state: "只读票券卡片和两条订单摘要；退款进度在订单详情查看。未生成临时入场码、撤回退款或修改订单。",
    source: "fixture",
    navigateFromEntry: async (page) => {
      const walletLink = page.getByRole("link", { name: "我的票券" });
      await waitFor(walletLink);
      await walletLink.click();
    },
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: "我的票券" }));
      await waitFor(page.getByText("入场票夹", { exact: true }));
      await waitFor(page.getByText("已出票", { exact: true }));
      await waitFor(page.getByText("显示 2 / 2 个订单", { exact: true }));
    },
  },
  {
    route: "/my-tickets/orders/[id]",
    url: `/my-tickets/orders/${ids.booking}`,
    slug: "customer-order-detail",
    role: "buyer",
    title: "订单详情",
    purpose: "查看订单状态、活动信息、出票进度和退款资格状态。",
    features: ["已确认订单金额与时间", "A1 票券签发状态", "退款申请状态和整单范围说明"],
    state: "只读 A1 已确认订单快照；未申请或撤回退款，也未生成临时入场码。",
    source: "fixture",
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: "订单详情" }));
      await waitFor(page.getByRole("heading", { name: event.title }).first());
      await waitFor(page.getByText("已有票券记录。票券记录不等于实时入场许可；退款状态另行显示。", { exact: true }));
    },
  },
  {
    route: "/notifications",
    url: "/notifications",
    slug: "customer-notifications",
    role: "buyer",
    title: "消息",
    purpose: "查看订单通知、未读状态和对应订单入口。",
    features: ["一条未读订单通知", "通知时间和正文", "查看关联订单入口"],
    state: "只读通知收件箱快照；未将通知标记为已读。",
    source: "fixture",
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: "消息" }));
      await waitFor(page.getByRole("heading", { name: "预订已确认" }));
      await waitFor(page.getByText("海港爵士之夜的订单已确认，入场票券已签发。", { exact: true }));
    },
  },
  {
    route: "/work",
    url: "/work",
    slug: "customer-staff-work",
    role: "staff",
    title: "我的工作",
    purpose: "查看工作人员获得授权的活动任务和扫码入口。",
    features: ["周宁的扫码权限", "海港爵士之夜活动任务", "进入票券核销页面"],
    state: "只读工作人员任务快照；未进入核销流程或执行票券操作。",
    source: "fixture",
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: "我的工作" }));
      await waitFor(page.getByRole("heading", { name: event.title }).first());
      await waitFor(page.getByRole("link", { name: /打开检票工具/ }));
    },
  },
  {
    route: "/events/[id]/check-in",
    url: `/events/${ids.event}/check-in`,
    entryUrl: "/work",
    slug: "customer-staff-check-in",
    role: "staff",
    title: "票券核销",
    purpose: "展示工作人员在线核销票券所需的扫描和手工凭证入口。",
    features: ["在线核验说明", "扫码入口", "手工凭证输入"],
    state: "扫描器保持待命；未启用摄像头、输入凭证或提交核销请求。",
    source: "form",
    navigateFromEntry: async (page) => {
      const scannerLink = page.getByRole("link", { name: "打开检票工具" });
      await waitFor(scannerLink);
      await scannerLink.click();
    },
    ready: async (page) => {
      await waitFor(page.getByRole("heading", { name: "票券核销" }));
      await waitFor(page.getByLabel("手动输入票券码"));
      await waitFor(page.getByText("可以开始在线核验票券。", { exact: true }));
    },
  },
];

export function customerResponse({ path, query, role }) {
  if (path === "/api/seats") {
    return { seats: Array.from({ length: tier.total_quantity }, (_, index) => apiSeat(index + 1)) };
  }

  if (path === `/api/bookings/${pendingBookingId}/resume` && role === "buyer") {
    return pendingCheckoutSnapshot();
  }

  if (role === "buyer" && path === "/api/bookings") {
    const page = Number(query.get("page") || "1");
    return { bookings: page === 1 ? [confirmedBooking(), pendingBooking()] : [], total: 2 };
  }

  if (role === "buyer" && path === `/api/bookings/${pendingBookingId}`) {
    return pendingBooking();
  }

  if (role === "buyer" && path === `/api/bookings/${ids.booking}`) {
    return confirmedBooking();
  }

  if (role === "buyer" && path === "/api/tickets") {
    return { tickets: [issuedTicket()] };
  }

  if (role === "buyer" && path === `/api/bookings/${ids.booking}/refund`) {
    return refundSnapshot();
  }

  if (role === "buyer" && path === "/api/users/me/favorites") {
    return {
      items: [{ eventId: ids.event, createdAt: "2027-05-18T11:00:00Z" }],
      limit: 20,
      offset: Number(query.get("offset") || "0"),
      hasMore: false,
    };
  }

  if (role === "buyer" && path === "/api/users/me/event-alerts") {
    return {
      items: [],
      limit: 20,
      offset: Number(query.get("offset") || "0"),
      hasMore: false,
    };
  }

  if (role === "buyer" && path === "/api/users/me/notifications") {
    return {
      notifications: [{
        id: ids.payment,
        type: "booking_confirmed",
        title: "预订已确认",
        body: "海港爵士之夜的订单已确认，入场票券已签发。",
        created_at: "2027-05-19T18:43:00Z",
        read_at: null,
        detail_path: `/my-tickets/orders/${ids.booking}`,
        source: "booking",
        locale: "zh",
      }],
      total: 1,
      page: Number(query.get("page") || "1"),
      page_size: Number(query.get("page_size") || "20"),
      unread_count: 1,
    };
  }

  if (role === "staff" && path === "/api/work") {
    return {
      items: [{
        event_id: ids.event,
        access: "scanner",
        pending_refunds: 0,
        title: event.title,
        starts_at: event.date,
        event_state: event.status,
        event_source_state: "available",
      }],
      has_more: false,
      observed_at: sampleTime,
    };
  }
}
