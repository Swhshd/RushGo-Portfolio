# RushGo 前端开发

[简体中文](README.md) | [English](README.en.md)

前端使用 Next.js App Router。只想看界面，可以直接运行[只读发现页或多页面预览](../DEMO.md)；调试真实 API 前，按[后端指南](../backend/README.md)准备 Gateway 和依赖服务。

## 开发服务器

在仓库根目录打开 PowerShell。Windows 上实测使用 Node.js 24.11.1 和 npm 11.7.0。项目没有单独固定 npm 版本；`npm ci` 使用 v3 锁文件。Next.js 自身要求 Node.js 20.9.0 以上，完整锁文件中的开发依赖还要求 Node.js `^20.19.0 || ^22.13.0 || >=24`。[依赖清单](package.json) · [锁文件](package-lock.json)

```powershell
cd frontend
npm ci
$env:NEXT_PUBLIC_API_URL = "http://127.0.0.1:8000"
npm run dev -- --hostname 127.0.0.1 --port 3000
```

打开 <http://127.0.0.1:3000/>。配置值为 Gateway 的服务地址，末尾不加 `/api`。Next.js 将匹配的 `/api/...` 请求转发到该地址；未设置时使用 `http://localhost:8000`。修改地址后，开发模式重启 Next.js，生产模式重新构建。开发服务器用 `Ctrl+C` 停止。[转发规则](next.config.ts)

## 构建与检查

本轮在独立检出中实测了 `npm ci`、开发服务器和含 TypeScript 检查的生产构建。下面列出 `frontend` 目录中的常用命令。生产服务需要先完成构建；如果 `3000` 已被开发服务器占用，请先停止当前开发进程或选择其他前端端口。

```powershell
npm run lint
npm run build
npx tsc --noEmit
npm run start -- --hostname 127.0.0.1 --port 3000
```

结束时按 `Ctrl+C`，在配置过地址的窗口执行 `Remove-Item Env:NEXT_PUBLIC_API_URL -ErrorAction SilentlyContinue`。图册构建使用 `127.0.0.1:1` 作为夹具占位地址；用户实际查看的是预览工具自动打开的页面。[多页面预览命令](../DEMO.md#多页面预览与截图)

## Checkout 与账单

RushGo 将卡片组件交给 Stripe。付款表单未采集付款人姓名，因此省略可选姓名字段。卡账单描述使用 Stripe 账户已配置的商户描述；两种本地界面演示都无需支付密钥。[Stripe 账单描述规则](https://docs.stripe.com/get-started/account/statement-descriptors)
