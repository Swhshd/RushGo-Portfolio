# RushGo frontend

This Next.js application contains the customer event discovery, account, checkout, ticket, after-sales, and administration interfaces. API-backed use requires the RushGo backend services.

For a bounded local interface walkthrough using fictional, read-only event data, follow [the root demo guide](../DEMO.md).

## Development checks

```powershell
npm ci
npm run lint
npx tsc --noEmit
npm run build
```

The checkout interface is not proof of a completed purchase. A real purchase requires the backend booking and payment services and is outside the local interface walkthrough.
