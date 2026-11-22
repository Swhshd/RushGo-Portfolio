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

## Checkout and billing

The RushGo checkout passes the card element to Stripe. It omits the optional payer name because the form does not collect one. For card statements, the Payment service uses the Stripe account's configured descriptor. The local discovery demo makes no payment requests. [Stripe statement descriptor rules](https://docs.stripe.com/get-started/account/statement-descriptors)
