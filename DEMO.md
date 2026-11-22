# RushGo 本地演示

模式 A 适合直接在自己的浏览器里试搜索与分类。模式 B 适合查看购票、工作人员及管理页面，或重建页面图册。两种演示都使用真实 Next.js 页面和本地样例，无需 Docker、Go、账户或支付密钥。要把前端接到完整业务服务，请按[后端开发指南](backend/README.md)和[前端配置](frontend/README.md)操作。

## 启动只读发现页

在检出的仓库根目录打开两个 PowerShell 窗口。Windows 上实测 Node.js 24.11.1 与 npm 11.7.0；首次 `npm ci` 要能下载锁文件指定的依赖。请先确认 `node --version`、`npm --version` 可用，本机 `8765` 和 `3000` 两个端口空闲。

**窗口一**运行只读样例 API，保留窗口。

```powershell
node .\docs\demo\events-api.mjs
```

**窗口二**安装前端依赖，将 Next.js 的 API 地址指向窗口一，再启动前端。

```powershell
cd frontend
npm ci
$env:NEXT_PUBLIC_API_URL = "http://127.0.0.1:8765"
npm run dev -- --hostname 127.0.0.1 --port 3000
```

Next.js 就绪后在浏览器打开 <http://127.0.0.1:3000/>。活动详情、账户、订票、支付、出票、退款与后台操作需要额外接口；本模式提供首页搜索与分类。

## 在发现页试一遍

| 操作 | 预期结果 |
| --- | --- |
| 打开首页 | 活动列表有 Afterglow Sessions、Harbor Jazz Weekend 和 Arena Finals Night。 |
| 在顶部搜索框输入 `Harbor`，按 Enter | 列表仅显示 Harbor Jazz Weekend。 |
| 清空搜索并选择 `Sports` | 列表仅显示 Arena Finals Night。选择 `Concerts` 可看到两场音乐活动。 |
| 保留 `Harbor` 搜索并选择 `Sports` | 两个条件同时生效，结果为空。 |

在另一个 PowerShell 窗口可检查浏览器前后的两段 HTTP 连接。第一条应返回 `3`，第二条应返回 `Harbor Jazz Weekend`。

```powershell
(Invoke-RestMethod 'http://127.0.0.1:8765/api/events').total
(Invoke-RestMethod 'http://127.0.0.1:3000/api/events?search=Harbor').events.title
```

![RushGo 只读样例活动发现页](docs/demo/screenshots/discover-page.png)

[样例 API](docs/demo/events-api.mjs) 只监听本机，提供 `GET /api/features` 与 `GET /api/events`。三场活动的日期、地点和金额均为样例，销售暂停、票档库存为零。

## 停止发现页演示

在窗口一和窗口二分别按 `Ctrl+C`。在窗口二继续使用同一 PowerShell 会话前，清除这次设置的变量。

```powershell
Remove-Item Env:NEXT_PUBLIC_API_URL -ErrorAction SilentlyContinue
```

## 多页面预览与截图

模式 B 使用一个 PowerShell 窗口。在仓库根目录开始，准备一个没有业务密钥的本地检出。`127.0.0.1:1` 只用作此夹具构建的占位地址，浏览器由脚本自动打开，**不要把这个端口作为网站打开**。前端项目锁文件提供 Playwright；Windows 可使用已安装的 Microsoft Edge，或在 `frontend` 目录运行 `npx playwright install chromium` 安装匹配的浏览器。

```powershell
cd frontend
npm ci
$env:NEXT_PUBLIC_API_URL = "http://127.0.0.1:1"
npm run build
cd ..
```

**打开页面供人查看。** 每次运行一条；关闭预览窗口后再运行下一条。脚本为选座页和管理页分别提供合成购票人和管理员身份，自动填入样例 ID。预览在隔离的浏览器上下文和动态分配的本机端口运行。

```powershell
node .\docs\demo\capture-gallery.mjs --preview '/events/[id]/seats'
node .\docs\demo\capture-gallery.mjs --preview '/admin/events'
```

**重新生成图册。** 从同一仓库根目录执行下面的命令。脚本按路由生成桌面和代表性手机 PNG，更新图册与 JSON 索引后自动退出。更改前端后先重新构建，并从干净的前端检出生成。[截图目录](docs/demo/screenshots) · [页面图册](docs/SCREENSHOTS.md) · [逐图来源](docs/demo/screenshot-index.json)

```powershell
node .\docs\demo\capture-gallery.mjs
Remove-Item Env:NEXT_PUBLIC_API_URL -ErrorAction SilentlyContinue
```

如果只查看预览，关窗后同样可以执行最后一行清除变量。`--preview` 接受图册里的路由模板。浏览器拦截外部请求和业务写入；结账恢复接口只在浏览器内返回固定订单快照，选座页使用本机 SSE 就绪事件。预览不提交登录、核销或审批，不执行真实交易，不发送邮件。[生成脚本](docs/demo/capture-gallery.mjs) · [验证范围](PROJECT_STATUS.md)

## 常见问题

- **端口已占用。** 先看是否已开过样例 API。`8765` 是脚本固定端口；若只有前端端口冲突，可将 `--port 3000` 改为 `--port 3001`，浏览器和 HTTP 检查地址也改用 `3001`。
- **API 有活动，首页没有。** 先清除搜索和分类，再执行上面的两条 HTTP 检查。请在启动 Next.js 的同一窗口设置地址，只填服务地址，末尾不加 `/api`，随后重启前端。
- **预览提示找不到构建或浏览器。** 先在 `frontend` 完成 `npm ci` 和 `npm run build`；若本机没有匹配的 Chromium 或 Edge，在该目录执行 `npx playwright install chromium` 后重试。
