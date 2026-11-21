# RushGo

RushGo 是一个个人实习简历项目，围绕一个具体问题展开：两个人同时选中同一座位后，订单、库存和用户看到的座位状态如何保持一致？仓库保留了管理活动、预订、支付编排、出票、核销和退款的实现。本地演示的操作步骤和数据来源见 [DEMO.md](DEMO.md)，逐项验证范围见 [PROJECT_STATUS.md](PROJECT_STATUS.md)。

## 从代码看一次预订

1. Next.js 客户端通过 Gateway 查询活动和座位。Event 服务管理票档、座位和预订归属，Redis 准入用于减少热点座位上的数据库竞争。最终的座位变更由 PostgreSQL 条件更新决定。
2. 用户进入结账时，Saga 通过 gRPC 协调订单步骤。Booking 保存订单状态，Payment 对接支付服务。支付回调、预订超时与补偿分别有对应的处理路径。
3. Booking 使用事务 outbox 一起写入状态与待发送事件。Kafka 将事件送到订阅方。客户端通过 SSE 接收座位变更，断线后用服务端快照校准视图。

前端使用 TypeScript、React 和 Next.js。后端 Go 工作区包括 Gateway、User、Event、Booking、Payment、Saga、Notification、Scheduler 八个服务及共享包。服务间调用的身份检查见 `backend/pkg/grpcsecurity`，数据模型和迁移随各服务维护。

## 本地运行

在仓库根目录打开两个 PowerShell 窗口。第一个启动只读样例 API：

```powershell
node .\docs\demo\events-api.mjs
```

第二个安装锁定依赖并启动前端：

```powershell
cd frontend
npm ci
$env:NEXT_PUBLIC_API_URL = "http://127.0.0.1:8765"
npm run dev -- --hostname 127.0.0.1 --port 3000
```

浏览器打开 `http://127.0.0.1:3000/`。无需创建账户或配置支付密钥。展示时可搜索虚构活动或按分类筛选，结束时在两个窗口分别按 `Ctrl+C`。详见[演示指南](DEMO.md)。
涉及身份验证、完整出票和邮件送达的端到端流程尚未在这个快照验收。完整服务栈还依赖数据库、Redis、Kafka 和匹配的证书配置。

![本机活动发现页显示三个虚构活动](docs/demo/screenshots/discover-page.png)

## 开发内容与来源

原始项目提供了票务应用、票档锁定、支付 Saga、Kafka 传输和超时清理。本项目在此基础上迭代了座位归属校验、热点准入、订单 outbox、实时座位同步、票据凭证、售后和管理视图。仓库保留内部 `github.com/ticketbox/*` 模块标识，以维持工作区和生成代码的引用关系。上游版本见 [hoangdaochuz/booking](https://github.com/hoangdaochuz/booking/tree/9c9d6548fa7a24d056c103adc495ecb3d08c9b3f)。

上游 README 将用途描述为教育和演示。当前仍需确认整体应用的再分发许可。迁移脚本目录中的 MIT 声明和 Geist 字体的 SIL Open Font License 分别适用于相应材料。[脚本许可](backend/scripts/LICENSE) · [字体许可](frontend/app/fonts/OFL.txt)

T23、T24、W28 及 72 窗口可靠性验收均已移出本次展示范围，其历史未完成状态保留。
