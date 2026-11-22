# RushGo project status

[简体中文](PROJECT_STATUS.md) | [English](PROJECT_STATUS.en.md)

The public demo includes a read-only event discovery page and a local gallery of buyer, staff, and administrator interfaces. The table identifies the implementation, the evidence available, and the scenarios that still need to be tested with multiple services running together. [Project overview](README.en.md) · [Run the demos](DEMO.en.md)

| Area | Evidence | Scope and limitations |
| --- | --- | --- |
| Implementation | The [Gateway routes](backend/services/gateway/internal/router/router.go), [services](backend/services), and [frontend pages](frontend/app) provide entry points for tracing a request. | The code covers accounts, events, bookings, payments, tickets, after-sales requests, notifications, and administrative queries. |
| Unit tests | Recorded checks cover User service and gRPC logic, Event seat reservations and eligibility for accounting, and Booking eligibility to sell and issue tickets as well as request replay. Targeted frontend tests have exercised discovery, event artwork, admin navigation, bilingual UI copy, and checkout state. Checkout tests use a simulated Stripe client. [Discovery tests](frontend/tests/discovery.test.mjs) · [Checkout tests](frontend/tests/checkout-payment-state.test.mjs) | In-memory repositories, simulated services, and sample data cover the conditions and branches exercised by each test. |
| Build | The frontend production build passed, including its TypeScript check. [Backend build guide](backend/README.en.md) · [Frontend build guide](frontend/README.en.md) | This build check covered the frontend. The full Go build was not rerun. |
| Read-only discovery | The browser loads three events through Next.js and the [sample API](docs/demo/events-api.mjs). Search and category filters can be reproduced locally. | Coverage is limited to discovery and read-only queries. The fixture has no sellable inventory. |
| Multi-page UI preview | The [page gallery](docs/SCREENSHOTS.en.md) identifies each page, role, and sample. The [capture script](docs/demo/capture-gallery.mjs) uses isolated browser contexts. | The preview shows forms, seats, tickets, after-sales requests, and admin queries. Identities, orders, and operational records are synthetic. Backend authorization on protected routes still needs integration testing. |
| Ticket wallet and check-in | The gallery opens the wallet through site navigation and the check-in tool from the staff work page. | When these URLs were opened directly with a local fixture identity, React reported a mismatch on the initial render. The screenshots show the state reached through in-app navigation. |
| End-to-end purchase | The current public version has not completed an integrated run from registration and payment through ticket issuance and check-in. | The UI demos do not verify Stripe TEST callbacks, final refund outcomes, email delivery, or recovery across services. |
| Reliability acceptance | Work on T23, T24, W28, and the 72-window reliability run has stopped. Historical failures and unexecuted checks are recorded in the offline archive. | These checks are outside the release scope and acceptance is still incomplete. |

## Implementation and outstanding integration checks

| Area | Implementation | Still to verify with the services running |
| --- | --- | --- |
| Accounts and roles | [User](backend/services/user/internal/service) · [Gateway identity handlers](backend/services/gateway/internal/handler/auth_handler.go) | Receive verification codes and access protected endpoints under the authenticated user's actual role. |
| Event management and seat selection | [Event](backend/services/event/internal/service/event_service.go) · [Seat repository](backend/services/event/internal/repository) | Publish an event as an administrator, retrieve its details, and reconcile seat availability in real time when buyers compete for the same seat. |
| Orders and payments | [Booking](backend/services/booking/internal/service/booking_service.go) · [Saga](backend/services/saga/internal/service) · [Payment](backend/services/payment/internal/gateway/stripe) | Follow a booking through payment confirmation or compensation and reconcile the final order, payment, and inventory states. |
| Wallet, check-in, and after-sales requests | [Ticket issuance](backend/services/booking/internal/service/ticket_issuance.go) · [Check-in handler](backend/services/gateway/internal/handler/ticket_handler.go) · [Refund service](backend/services/booking/internal/service/unused_ticket_refund.go) | Issue a ticket, present an entry credential at check-in, approve a refund, and verify that the funds were returned. |
| Notifications and admin queries | [Notification delivery](backend/services/notification/internal/service) · [Order details](backend/services/gateway/internal/handler/admin_booking_detail_handler.go) · [Outbox queries](backend/services/gateway/internal/handler/admin_outbox_handler.go) | Verify email delivery, inbox access after sign-in, and administrative queries for actual orders and events. |
