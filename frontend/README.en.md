# RushGo frontend development

[简体中文](README.md) | [English](README.en.md)

The frontend uses the Next.js App Router. To explore the interface, start the [read-only discovery demo or multi-page preview](../DEMO.en.md). To debug requests against the backend, first prepare Gateway and its dependencies using the [backend guide](../backend/README.en.md).

## Development server

Open PowerShell at the repository root. The tested Windows environment used Node.js 24.11.1 and npm 11.7.0. The project does not pin npm separately. `npm ci` installs from the v3 lockfile. Next.js requires Node.js 20.9.0 or later, and the full set of locked development dependencies requires `^20.19.0 || ^22.13.0 || >=24`. [Package manifest](package.json) · [Lockfile](package-lock.json)

```powershell
cd frontend
npm ci
$env:NEXT_PUBLIC_API_URL = "http://127.0.0.1:8000"
npm run dev -- --hostname 127.0.0.1 --port 3000
```

Open <http://127.0.0.1:3000/>. Set the variable to the Gateway base URL, omitting the `/api` suffix. Next.js forwards matching `/api/...` requests to that service and defaults to `http://localhost:8000` when the variable is unset. After changing the address, restart the development server or rebuild the production frontend. Press `Ctrl+C` to stop the development server. [Rewrite configuration](next.config.ts)

## Build and checks

The dependency install, development server, and production build with TypeScript checks were exercised in a separate checkout. Run the following commands from `frontend`. Build before starting the production server. If the development server already occupies port `3000`, stop it or choose another frontend port.

```powershell
npm run lint
npm run build
npx tsc --noEmit
npm run start -- --hostname 127.0.0.1 --port 3000
```

Press `Ctrl+C` when finished. In the PowerShell window where you set the address, run `Remove-Item Env:NEXT_PUBLIC_API_URL -ErrorAction SilentlyContinue`. The gallery build uses `127.0.0.1:1` as a fixture placeholder. View the page in the browser window that the preview tool opens. [Multi-page preview commands](../DEMO.en.md#multi-page-preview-and-screenshots)

## Checkout and billing

RushGo passes the card element to Stripe. Checkout omits the optional payer name because the form does not collect one. Card statements use the descriptor configured on the Stripe account. Neither local UI demo requires payment credentials. [Stripe statement descriptor rules](https://docs.stripe.com/get-started/account/statement-descriptors)
