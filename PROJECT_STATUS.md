# 项目状态

本地快照取自已提交的产品基线 `0dc872f79f437a3c6e48ac21b1d2f44dc5bafb66`。参与展示的路径和操作以 [演示记录](DEMO.md) 为准。下表中的前端入口是浏览器路由，后端实现位于 `backend/services/` 或注明的共享包。

| 模块 | 实现入口 | 前端入口 | 验证依据、限制与本轮处置 | 状态 |
|---|---|---|---|---|
| 账户与角色 | `gateway/internal/handler/auth_handler.go`、`user` | `/register`、`/login`、`/account` | User 的 service 和 gRPC 包单测通过；真实收件和用户登录未联机验收 | IMPLEMENTED_NOT_DEMO_VERIFIED |
| 活动发现页面 | `frontend/lib/discovery.ts`、`docs/demo/events-api.mjs` | `/` | 本机浏览器显示三个只读虚构活动；操作与截图见 DEMO.md | IMPLEMENTED_AND_DEMO_VERIFIED |
| 活动查询服务与详情 | `gateway/internal/handler/event_handler.go`、`event/internal/repository` | `/events/[id]` | 保留查询与详情接口；Go 查询服务和详情页未联机验收 | IMPLEMENTED_NOT_DEMO_VERIFIED |
| 活动管理 | `gateway/internal/handler/management_handler.go`、`event` | `/admin/events` | 保留创建、编辑等受权限控制的入口；管理员操作未做联机演示 | IMPLEMENTED_NOT_DEMO_VERIFIED |
| 票档、座位与库存 | `event/internal/repository`、`pkg/admission`（共享包） | `/events/[id]/seats` | Event 对已预订座位及库存记账的资格判定单测通过；本轮未运行多人并发选座 | IMPLEMENTED_NOT_DEMO_VERIFIED |
| 预订与订单 | `booking/internal/service/booking_service.go`、`gateway/internal/handler/booking_handler.go` | `/checkout`、`/my-tickets/orders/[id]` | Booking 售卖资格和请求重放的内存测试通过；未联机创建订单 | IMPLEMENTED_NOT_DEMO_VERIFIED |
| 支付与编排 | `payment/internal/gateway/stripe`、`saga/internal/service/service.go` | `/checkout` | 保留支付与补偿逻辑，本轮无外部支付调用，完整购票未验证 | IMPLEMENTED_NOT_DEMO_VERIFIED |
| 钱包、票据与核销 | `booking/internal/service/ticket_issuance.go`、`gateway/internal/handler/ticket_handler.go` | `/my-tickets`、`/events/[id]/check-in` | Booking 出票前对 Event、Payment、Saga 的资格核对测试通过；真实出票和扫码仍待联机验证 | IMPLEMENTED_NOT_DEMO_VERIFIED |
| 售后 | `booking/internal/service/unused_ticket_refund.go`、`gateway/internal/handler/booking_refund_handler.go` | `/aftersales`、`/admin/events/[id]/refunds` | 保留未使用票的退款申请与审批路径；未触发实际退款 | IMPLEMENTED_NOT_DEMO_VERIFIED |
| 通知 | `notification/internal/kafka/consumer.go`、`notification/internal/service` | `/notifications` | 保留收件箱、投递和重试代码；真实 SMTP 送达未验证 | IMPLEMENTED_NOT_DEMO_VERIFIED |
| 管理查询 | `gateway/internal/handler/admin_booking_detail_handler.go`、`admin_outbox_handler.go` | `/admin/orders`、`/admin/outbox` | 保留受权限保护的订单和 outbox 读取；未登录管理员演示 | IMPLEMENTED_NOT_DEMO_VERIFIED |
| 事务与基础设施 | `booking/internal/service/outbox_relay.go`、`scheduler`、共享包 `backend/pkg/{kafka,redis,grpcsecurity}` | 经上述业务页面间接使用 | 保留数据库迁移、消息处理和 mTLS 身份逻辑；多服务拓扑未联机验收 | IMPLEMENTED_NOT_DEMO_VERIFIED |
| T23/T24、W28 和 72 个窗口 | 历史验收工具保存在离线档案 | 无 | 停止继续推进，保留原始失败及未执行记录 | OUT_OF_RELEASE_SCOPE |

状态词另包括 PARTIAL（实现不完整）和 NOT_IMPLEMENTED（无实现）。历史性能数字、付费能力和生产运行情况均未作为本次验收指标。
