# RushGo 后端开发

后端工作区包含 Gateway、User、Event、Booking、Payment、Saga、Notification 和 Scheduler 八个 Go 服务，以及共用的 gRPC 协议与基础组件。此文档介绍本地开发栈的依赖、初始化顺序和当前尚需本机配置的项目。[项目概览](../README.md) · [前端开发](../frontend/README.md) · [只读演示](../DEMO.md) · [项目状态](../PROJECT_STATUS.md)

## 依赖与配置

| 用途 | 当前仓库配置 |
| --- | --- |
| Go 构建与 User 数据库预检 | Go 1.25.5，版本写在 [go.work](go.work) |
| 服务与基础设施 | Docker Engine 和 Docker Compose v2，或项目 Makefile 使用的 Podman/WSL 工作流 |
| 数据库 | PostgreSQL 16，每个有持久化业务状态的服务各有一个数据库 |
| 缓存与消息 | Redis 7；Kafka 与 ZooKeeper 使用 Confluent 7.6.0 镜像 tag |
| 数据库迁移 | migrate 命令行工具、Bash 和 Make；User 迁移还会运行 Go 预检程序 |
| 浏览器前端 | 后端不依赖 Node.js；单独运行 Next.js 时使用 [frontend/README.md](../frontend/README.md) 所列工具和版本 |

Compose 会发布 Gateway HTTP 端口 8000，Payment webhook 仅绑定到本机 127.0.0.1:8081。默认数据库宿主机端口依次为 User 5433、Event 5434、Booking 5435、Notification 5436、Payment 5437、Saga 5438 和 Scheduler 5439。Redis 使用 6379，Kafka 9092，Kafka UI 8080。Kafka 和 Postgres 仅供本地开发网络使用。

本地 Compose 为七个服务配置独立数据库，但它们使用同一个 PostgreSQL 所有者账号。服务连接和迁移也使用这个账号。仓库没有给本地服务建立独立 SQL 登录角色或 GRANT 的初始化脚本；这套容器配置不提供部署用的最小权限数据库角色。Compose 中各数据库的 `POSTGRES_USER`、`POSTGRES_DB` 是配置来源；迁移脚本对非 User 服务的连接用户和数据库名有固定假设，修改 Compose 数据库身份前需另行同步修订迁移脚本。

从仓库根目录打开 PowerShell，进入后端目录。首次本地运行时复制模板；如果已有 .env，保留自己的文件。

~~~powershell
Set-Location backend
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
~~~

编辑 `backend/.env`，将 POSTGRES_PASSWORD 和 JWT_SECRET 的公开示例值换成仅供本机使用的随机值。此模板中的数据库口令和 JWT 文本是演示占位值。.env 已加入忽略规则，勿提交本地凭据。

Compose 还要求环境变量 RUSHGO_GRPC_CERT_ROOT 指向仓库外的私有证书根目录。证书目录必须能被当前容器运行时挂载。基础 Compose 会读取其中的 user、event、booking、payment、saga、scheduler 和 gateway 子目录。Notification 目前缺少证书挂载，需要下文所列的本地私有 override 才能启动。

Stripe 密钥只使用 Stripe TEST 账户的测试配置。启动 Payment 时会通过 Stripe SDK 读取当前 TEST 账户，完成本地凭据绑定。该启动请求会访问 Stripe，但不会创建支付。没有随附可用支付凭据，本轮没有启动 Payment。认证邮件和 Notification 投递需要兼容配置的 SMTP 服务。需要发送邮件时，只连接本机邮件捕获器或邮件服务商的 sandbox。仓库没有 SMTP dry-run 运行模式，也没有随附可用邮件凭据。根目录的只读发现页演示不连接后端、支付或邮件服务。

## 完整本地栈的启动顺序

先启动基础设施、完成数据库迁移，再启动应用服务。不要在应用写入账户资料时运行 User M6 迁移。

先设置证书根目录，提供后文列出的本机专属 Compose override，然后启动数据库、Redis 和 Kafka。override 保存到仓库外，并通过同一组 -f 参数用于本轮所有 Compose 操作。这里使用默认数据库宿主机端口，因为现有迁移脚本写死了这些端口。

~~~powershell
$env:RUSHGO_GRPC_CERT_ROOT = Read-Host 'Private gRPC certificate root visible to the container runtime'
$env:RUSHGO_COMPOSE_OVERRIDE = Read-Host 'Path to the local private Compose override'
if (-not (Test-Path -LiteralPath $env:RUSHGO_COMPOSE_OVERRIDE)) { throw 'Private Compose override not found' }
$composeFiles = @('-f', 'docker-compose.yml', '-f', $env:RUSHGO_COMPOSE_OVERRIDE)
docker compose @composeFiles config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Compose configuration failed validation' }
docker compose @composeFiles up -d postgres-user postgres-event postgres-booking postgres-notification postgres-payment postgres-saga postgres-scheduler redis kafka
docker compose @composeFiles ps
~~~

Compose 会随 Kafka 一起启动 ZooKeeper。等待数据库和 Kafka 显示为 healthy，再运行迁移。现有用户迁移需要 AUTH_PREFLIGHT_DATABASE_URL 指向 User 数据库，其他迁移从 Bash 环境读取 POSTGRES_PASSWORD。PowerShell 中启动的 Compose 会从 `backend/.env` 读取插值；Make/Bash 进程不会自动读取该文件。`AUTH_PREFLIGHT_DATABASE_URL` 的用户名和数据库名应取自 Compose 的 `postgres-user.environment.POSTGRES_USER` 与 `POSTGRES_DB`，密码取自本机配置；其他服务的数据库身份需与 `scripts/migrate.sh` 中的固定连接保持一致。默认 User 数据库映射到 localhost:5433。输入敏感值时不要把它们写入 shell 命令行历史。

~~~bash
read -rsp 'POSTGRES_PASSWORD: ' POSTGRES_PASSWORD
printf '\n'
export POSTGRES_PASSWORD
read -rsp 'AUTH_PREFLIGHT_DATABASE_URL: ' AUTH_PREFLIGHT_DATABASE_URL
printf '\n'
export AUTH_PREFLIGHT_DATABASE_URL
make migrate
unset POSTGRES_PASSWORD AUTH_PREFLIGHT_DATABASE_URL
~~~

运行命令前安装 migrate CLI，并从 backend 目录执行。脚本按 User、Booking、Event、Notification、Payment、Saga、Scheduler 的顺序应用各自服务的迁移。User 的预检会检查迁移版本和规范化邮箱冲突。冲突、未版本化的既有表或异常迁移状态都会停止流程，需人工检查后再继续。请勿绕过预检，也不要把本地迁移脚本用于生产数据。

空数据库没有账户、管理员或活动种子数据。make seed-scheduler 只为 Scheduler 写入预订清理任务配置，不创建活动或用户。Event 的 make seed-baseline 只接受已有活动 UUID，并对全新且完整可用的票档记录基线标记。创建初始活动和账户仍需使用实际管理 API 和账户流程。

基础库与迁移就绪后，可启动应用服务。Payment 启动会访问 Stripe TEST 账户，因此只有在允许这次外部账户读取并提供本地私有 TEST 配置时才运行此步骤。Payment 还需要 Stripe TEST endpoint secret 和期望的 TEST 账户 ID。Stripe endpoint 是本地 127.0.0.1:8081/webhooks/stripe，远端服务访问本地 webhook 还需要单独的测试转发流程。

~~~powershell
docker compose @composeFiles up --build -d
docker compose @composeFiles ps
docker compose @composeFiles logs --tail 100 gateway user-service event-service booking-service payment-service saga-service scheduler-service notification-service
~~~

通过 http://127.0.0.1:8000/api/events 检查 Gateway 的公开活动查询。成功的 HTTP JSON 响应验证了 Gateway 和当前查询依赖可达。新库未创建活动时，活动列表为空。

### 当前 Compose 与 Notification 入口

目前 [docker-compose.yml](docker-compose.yml) 没有为 Notification 提供启动代码所需的完整环境，因此单独使用基础文件时 Notification 会在初始化阶段退出。Notification 初始化时会 Ping Redis。容器内需要显式设置 `REDIS_URL=redis://redis:6379`，默认回环地址指向容器自身。它还需要 User gRPC 地址 user-service:50051、服务器名 user-service、完整的 mTLS 客户端/服务器证书设置和 SMTP 配置。Compose 当前没有为它挂载证书目录，也没有传入这些地址、服务器名或 SMTP 环境变量。

将以下片段保存成仓库外的本机私有 Compose 文件，并通过 RUSHGO_COMPOSE_OVERRIDE 指向它。基础 Compose 和私有 override 合并后，再使用上文 composeFiles 变量运行 up、ps、logs 和 down。它需要 `backend/.env` 中的 SMTP sandbox 值和 RUSHGO_STRIPE_EXPECTED_ACCOUNT。SMTP 登录、密码、发件人及前端来源都在 `.env` 中私下设置。

~~~yaml
services:
  notification-service:
    depends_on:
      redis:
        condition: service_healthy
    environment:
      REDIS_URL: redis://redis:6379
      USER_SERVICE_ADDR: user-service:50051
      RUSHGO_USER_GRPC_SERVER_NAME: user-service
      RUSHGO_GRPC_SERVER_CERT_FILE: /run/secrets/rushgo-grpc/server-cert.pem
      RUSHGO_GRPC_SERVER_KEY_FILE: /run/secrets/rushgo-grpc/server-key.pem
      RUSHGO_GRPC_CLIENT_CA_FILE: /run/secrets/rushgo-grpc/ca.pem
      RUSHGO_GRPC_CLIENT_CERT_FILE: /run/secrets/rushgo-grpc/client-cert.pem
      RUSHGO_GRPC_CLIENT_KEY_FILE: /run/secrets/rushgo-grpc/client-key.pem
      RUSHGO_GRPC_SERVER_CA_FILE: /run/secrets/rushgo-grpc/ca.pem
      SMTP_HOST: ${SMTP_HOST:?set a local mail catcher or SMTP sandbox}
      SMTP_PORT: ${SMTP_PORT:?set 465 or 587}
      SMTP_TLS_MODE: ${SMTP_TLS_MODE:?set implicit or starttls}
      SMTP_USERNAME: ${SMTP_USERNAME:?set private SMTP credentials}
      SMTP_PASSWORD: ${SMTP_PASSWORD:?set private SMTP credentials}
      SMTP_FROM: ${SMTP_FROM:?set a sandbox sender}
      AUTH_FRONTEND_ORIGIN: ${AUTH_FRONTEND_ORIGIN:?set the local frontend origin}
    volumes:
      - type: bind
        source: ${RUSHGO_GRPC_CERT_ROOT:?set the private certificate root}/notification
        target: /run/secrets/rushgo-grpc
        read_only: true

  gateway:
    environment:
      NOTIFICATION_SERVICE_ADDR: notification-service:50051
      RUSHGO_NOTIFICATION_GRPC_SERVER_NAME: notification-service

  payment-service:
    environment:
      RUSHGO_STRIPE_EXPECTED_ACCOUNT: ${RUSHGO_STRIPE_EXPECTED_ACCOUNT:?set the expected TEST account ID}
~~~

SMTP 配置没有 dry-run 开关，Notification 启动后可能实际投递积压邮件。只接本机邮件捕获器或专用 sandbox，并使用测试收件人。Payment 的远程账户读取发生在启动时，提供私有 TEST 密钥和 webhook endpoint secret 后才能初始化该服务。

User 服务允许 SMTP 配置完全留空以启动已有账户登录；注册验证码和账户邮件在没有 SMTP 时无法送达。Notification 的 SMTP sender 初始化则要求主机、用户名、密码、发件地址、465/implicit 或 587/starttls 组合，以及合法的前端来源地址。已配置 SMTP 时，User 同样要求 AUTH_FRONTEND_ORIGIN。

在 User、Gateway 和满足邮件配置的前提下，可从前端 /register 创建普通购票人账户。注册会通过 SMTP 发送验证邮件。新数据库没有默认用户，唯一的 scheduler/baseline 脚本也不会建立应用账户。新注册角色固定为 user。仓库没有受支持的管理员创建、角色提升或 bootstrap 命令；管理员账户需要由维护者通过另行受控流程准备。不要把猜测的账户或直接修改生产角色的 SQL 写入本地启动步骤。

Gateway 由 Compose 配置 User、Event、Booking、Payment、Scheduler 地址、Redis 和 gRPC 客户端凭据。它需要这些服务可达才能提供完整 API。Notification 地址是可选配置；使用上面的私有 override 后，Gateway 才启用 Notification 管理事件来源。

Payment 在启动阶段会初始化数据库、Kafka、Booking/Event authority 与 mTLS，并使用 STRIPE_SECRET_KEY 读取 TEST 账户身份。私有 override 传入预期 TEST 账户 ID，避免启动时接受其他账户。其 webhook 使用 STRIPE_SECRET_WEBHOOK 校验本地 /webhooks/stripe 回调。账户身份读取会访问 Stripe API，即使不提交任何支付也会发生。需要完整 Payment 服务时，先确认此 TEST 读取已获准，再从本地私密 .env 载入 TEST 密钥与 webhook secret。当前回合没有进行此账户读取。

## gRPC mTLS 证书

证书根目录不包含在仓库中，项目也没有可供本地服务使用的通用 CA 或服务证书生成命令。测试代码会临时生成测试证书，但那不是开发栈的证书签发工具。使用本机已批准的私有证书颁发流程准备证书，再通过 Compose bind mount 提供给服务。不要关闭 mTLS 来跳过验证。

Compose 将各子目录挂载到容器的 /run/secrets/rushgo-grpc。下表按基础 Compose 的默认 `local` 环境列出文件。Event、Saga、Scheduler 的常规 RPC 在这个环境使用明文；独立的内部权限接口有各自的传输要求。切换外部运行环境后，常规 RPC 也需要匹配的服务端凭据与客户端服务器名，不能直接沿用这张表。[传输选择](pkg/grpcsecurity/internal_transport.go)

| 子目录 | 客户端身份文件 | 服务端身份文件 |
| --- | --- | --- |
| gateway | client-cert.pem、client-key.pem、ca.pem | 无 |
| user | 无 | server-cert.pem、server-key.pem、ca.pem |
| event | 无 | server-cert.pem、server-key.pem、ca.pem |
| booking | client-cert.pem、client-key.pem、ca.pem | server-cert.pem、server-key.pem |
| payment | client-cert.pem、client-key.pem、ca.pem | server-cert.pem、server-key.pem |
| saga | client-cert.pem、client-key.pem、ca.pem | server-cert.pem、server-key.pem |
| scheduler | client-cert.pem、client-key.pem、ca.pem | 无 |
| notification | client-cert.pem、client-key.pem、ca.pem | server-cert.pem、server-key.pem |

基础 Compose 没有 notification bind mount；上表列出它目前代码实际所需的文件，override 需要显式挂载。Compose 的每个 client/server 环境变量都指向 client-cert.pem、client-key.pem、server-cert.pem、server-key.pem 和 ca.pem。

客户端证书必须带与代码匹配的 URI SAN，例如 Gateway 是 spiffe://rushgo.internal/gateway，Booking 是 spiffe://rushgo.internal/booking。其余客户端服务使用 user、event、payment、saga、scheduler 和 notification。服务端证书用于 TLS 服务端身份，DNS SAN 要匹配 Compose 中配置的服务器名。现有配置使用 user-service、event-service、booking-service、payment-service 和 saga-service；私有 Notification 配置使用 notification-service。客户端身份与客户端证书 URI SAN 的匹配由服务端检查。信任链应由挂载的 ca.pem 验证。私钥文件在 Linux 容器内需要是常规文件，且组和其他用户不可读写。保留现有证书验证和私钥文件权限检查。

Ticket authority 和部分内部 RPC 在 Event、Saga 及其他服务端口上运行，也依赖相同 CA 信任和服务身份。证书有效期、URI SAN、DNS SAN、密钥对应关系或挂载位置不正确时，相关服务会在启动或握手时拒绝连接。

## 迁移与可选初始化

make migrate 只通过默认 Compose 文件映射的宿主机端口连接数据库，端口配置固定在 [scripts/migrate.sh](scripts/migrate.sh)。本地端口覆盖文件把 Postgres 宿主机端口改成 15433 至 15439，但迁移脚本仍尝试连接 5433 至 5439。因此使用 docker-compose.local.yml 时不要直接运行 make migrate。当前脚本没有迁移端口参数；该端口配置差异需要后续单独修正。

可选的 docker-compose.local.yml 只为解决特定宿主机端口冲突而覆盖映射，含有 !override 标签，需要支持该 Compose 标签的版本。它不是一般启动的必要文件，而且覆盖后的 PostgreSQL 端口与当前 migrate.sh 不相配。采用其他端口方案前先解决迁移脚本的固定端口依赖。

Scheduler 的初始清理任务配置可通过以下命令建立。该脚本会检查并修复无效默认值；Scheduler 已运行时还会重启它。只在一次性开发数据库中执行。

~~~bash
make seed-scheduler
~~~

已有新建活动且票档、座位满足脚本条件时，可针对明确的活动 ID 执行 Event 生命周期基线辅助脚本。该操作会改写被指定活动票档的基线标记；它不创建或重置票档、座位、活动或订单。

~~~bash
event_id='00000000-0000-4000-8000-000000000001'
EVENT_IDS="$event_id" make seed-baseline
~~~

用你已创建活动的真实 UUID 替换示例 ID。脚本不会创建活动，也只接受满足完整库存和座位约束的票档。

数据库状态、迁移和可选脚本有变更时，先阅读对应 SQL 和 Bash 脚本。请只对本地开发数据库运行。

## 前端连接、日志与停止

前端是独立的 Next.js 项目。在新的 PowerShell 窗口从仓库根目录运行，API 指向 Gateway。

~~~powershell
Set-Location frontend
npm ci
$env:NEXT_PUBLIC_API_URL = 'http://127.0.0.1:8000'
npm run dev -- --hostname 127.0.0.1 --port 3000
~~~

打开 http://127.0.0.1:3000/。需要完整服务配置、功能边界和界面演示时，分别查看[前端说明](../frontend/README.md)和[根目录 DEMO](../DEMO.md)。

Compose 命令都要带同一组基础文件和私有 override 参数。状态、日志、停止可使用启动时定义的 composeFiles 变量。

停止并保留本地数据库和 Redis 卷。

~~~powershell
docker compose @composeFiles down
~~~

不要附加卷删除参数，除非已确认本机数据可以永久丢弃。docker compose down 会停止 Compose 容器，但命名卷会保留。

可选的 docker-compose.observability.yml 增加本地 OpenTelemetry Collector、Prometheus、Tempo 和 Grafana。只有需要观测界面时才加载它。启动和后续管理都需保留相同的 Compose 文件集合。

~~~powershell
$observabilityComposeFiles = $composeFiles + @('-f', 'docker-compose.observability.yml')
docker compose @observabilityComposeFiles up --build -d
docker compose @observabilityComposeFiles ps
docker compose @observabilityComposeFiles logs -f
docker compose @observabilityComposeFiles down
~~~

## 构建

安装 Go 1.25.5 或更新版本。从 backend 目录通过 PowerShell 构建每个 Go 工作区模块。

~~~powershell
$modules = @('pkg', 'services/booking', 'services/event', 'services/gateway', 'services/notification', 'services/payment', 'services/saga', 'services/scheduler', 'services/user')
foreach ($module in $modules) {
    Push-Location $module
    try {
        go build ./...
        if ($LASTEXITCODE -ne 0) { throw "go build failed for $module" }
    }
    finally {
        Pop-Location
    }
}
~~~

make build 从 Makefile 逐个构建服务入口。make test 会运行全部 Go 服务测试，其中部分集成测试需要独立数据库、Kafka 或测试凭据；运行前先查看对应测试要求。本轮在 Windows PowerShell 用 Docker Compose 5.6.0 实测了基础配置、端口覆盖、观测配置，以及上文私有 override 示例的 `config --quiet` 解析。示例解析仅使用占位输入。证书握手、数据库迁移、服务启动和业务联机仍待具备私有开发配置后验证。浏览器展示与完整业务 E2E 的范围见[项目状态](../PROJECT_STATUS.md)。
