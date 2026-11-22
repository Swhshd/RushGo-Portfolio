# 项目状态

公开演示覆盖活动发现页。下面分别列出代码入口、已有的验证记录，以及仍需多服务联机验证的部分。[项目概览](README.md) · [复现演示](DEMO.md)

| 验证层次 | 已有依据 | 范围 |
| --- | --- | --- |
| 代码实现 | [Gateway 路由](backend/services/gateway/internal/router/router.go)、[服务目录](backend/services)、[前端页面](frontend/app) | 可沿路由追踪账户、活动、订单、支付、票据、售后、通知与管理查询。 |
| 单元测试 | 已有验证记录覆盖 User 的服务和 gRPC 逻辑、Event 的预留座位与记账资格判断、Booking 的售卖资格、请求重放和出票资格。本次另运行了[发现页定向测试](frontend/tests/discovery.test.mjs)。 | 单测中的内存仓储、模拟服务和样例数据仅验证相应条件与分支。 |
| 构建 | [后端说明](backend/README.md)和[前端说明](frontend/README.md)给出构建入口。本次复核由 Next.js 开发服务器实际编译并加载发现页。 | 此次未重跑全量 Go 模块构建或前端生产构建。 |
| 本地页面 | 浏览器通过 Next.js 读取[样例 API](docs/demo/events-api.mjs)，显示三个活动，搜索和分类均可复现。 | 仅覆盖发现页和只读查询，样例库存为零。 |
| 完整业务 E2E | 尚未在当前公开版本完成从注册、支付到出票和核销的整链路验收。 | Stripe TEST 回调、退款最终状态、真实邮件送达和多服务恢复均不能从页面演示推断。 |
| 高级验收 | T23、T24、W28 及 72 窗口可靠性验收已停止推进。历史失败和未执行记录保留在离线归档。 | 已移出本次发布范围，验收仍未完成。 |

## 按业务查实现与待验项

| 业务 | 实现入口 | 待联机验证 |
| --- | --- | --- |
| 账户与角色 | [User](backend/services/user/internal/service)、[Gateway 身份接口](backend/services/gateway/internal/handler/auth_handler.go) | 收取验证码、登录并以实际角色访问受限接口。 |
| 活动管理与选座 | [Event](backend/services/event/internal/service/event_service.go)、[座位仓储](backend/services/event/internal/repository) | 管理员发布活动、详情页读取、多个用户竞争同一座位及实时校准。 |
| 订单与支付 | [Booking](backend/services/booking/internal/service/booking_service.go)、[Saga](backend/services/saga/internal/service)、[Payment](backend/services/payment/internal/gateway/stripe) | 从下单到支付确认或补偿，核对订单、支付与库存的最终状态。 |
| 钱包、核销与售后 | [出票服务](backend/services/booking/internal/service/ticket_issuance.go)、[核销接口](backend/services/gateway/internal/handler/ticket_handler.go)、[退款服务](backend/services/booking/internal/service/unused_ticket_refund.go) | 签发票据、呈示有效凭证核销、审批未使用票退款并核实到账。 |
| 通知与管理查询 | [通知投递](backend/services/notification/internal/service)、[订单详情](backend/services/gateway/internal/handler/admin_booking_detail_handler.go)、[Outbox 查询](backend/services/gateway/internal/handler/admin_outbox_handler.go) | 邮件送达、登录后读取通知，以及使用管理权限查询实际订单和事件。 |
