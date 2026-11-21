# Local interface demo

This demo shows RushGo's event discovery page with three fictional events served by a read-only local fixture. It verifies the frontend page and its search and category controls. The fixture does not create events, reserve seats, place orders, or process payments.

## Start

Open two PowerShell terminals from the repository root.

In the first terminal, start the sample API:

```powershell
node .\docs\demo\events-api.mjs
```

In the second terminal, start the frontend with its API rewrite pointed to that fixture:

```powershell
Set-Location .\frontend
npm ci
$env:NEXT_PUBLIC_API_URL = "http://127.0.0.1:8765"
npm run dev -- --hostname 127.0.0.1 --port 3000
```

Open <http://127.0.0.1:3000/>. Use the search box to filter the sample events, or select a category. The fixture marks all ticket inventory unavailable, so the displayed event cards cannot be treated as offers for purchase.

## Stop

Press Ctrl+C in each terminal. The API only binds to `127.0.0.1` and accepts GET requests for `/api/features` and `/api/events`.

## Scope

The sample event names, descriptions, locations, and dates are fictional. They are local demonstration data, not records from a live service. This demo does not validate authentication, event publishing, seat inventory, checkout, order status, ticket issuance, refunds, notifications, Stripe, or the backend services.

The captured page is saved at [`docs/demo/screenshots/discover-page.png`](docs/demo/screenshots/discover-page.png).
