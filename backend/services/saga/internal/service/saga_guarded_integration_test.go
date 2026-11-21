package service

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	bookingfixture "github.com/ticketbox/booking/testfixture"
	eventfixture "github.com/ticketbox/event/testfixture"
	paymentfixture "github.com/ticketbox/payment/testfixture"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/registry"
	"github.com/ticketbox/saga/internal/repository"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func guardedPool(t *testing.T, name string, port int) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	dsn := fmt.Sprintf("postgres://reliability_fixture@127.0.0.1:%d/ticketbox_%s_test?sslmode=disable", port, name)
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "saga_guarded_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	u, _ := url.Parse(dsn)
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	if out, err := exec.Command("migrate", "-path", "../../../"+name+"/migrations", "-database", u.String(), "up").CombinedOutput(); err != nil {
		t.Fatalf("actual %s migrations: %s", name, out)
	}
	p, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}
func fixtureRPC(t *testing.T, register func(*grpc.Server), interceptor grpc.UnaryServerInterceptor) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	register(server)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() { server.Stop(); _ = lis.Close() })
	conn, err := grpc.NewClient("passthrough:///guarded-fixture", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// Every downstream boundary here is its actual service/server/repository, with
// disposable migrated PostgreSQL schemas. Only response-loss barriers are faked.
func TestSagaReplayAgainstRealGuardedRepositoriesIntegration(t *testing.T) {
	for _, scenario := range []string{"confirm_ack_lost", "book_ack_lost", "account_ack_lost", "terminal_after_seat_reallocated"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			sagaDB := replayPG(t)
			bookingDB := guardedPool(t, "booking", 25435)
			eventDB := guardedPool(t, "event", 25434)
			paymentDB := guardedPool(t, "payment", 25437)
			booking, user, event, tier, seat := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
			deadline := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
			if _, err := eventDB.Exec(ctx, `INSERT INTO events(id,title,category,venue,location,date,publication_status,sales_status)
				VALUES($1,'fixture','concert','fixture','fixture',NOW()+INTERVAL '1 day','published','selling')`, event); err != nil {
				t.Fatal(err)
			}
			if _, err := eventDB.Exec(ctx, `INSERT INTO ticket_tiers(id,event_id,name,price_cents,total_quantity,available_quantity,lifecycle_accounting_baseline) VALUES($1,$2,'fixture',725,1,1,'verified')`, tier, event); err != nil {
				t.Fatal(err)
			}
			if _, err := eventDB.Exec(ctx, `INSERT INTO seats(id,event_id,ticket_tier_id) VALUES($1,$2,$3)`, seat, event, tier); err != nil {
				t.Fatal(err)
			}
			if _, err := bookingDB.Exec(ctx, `INSERT INTO bookings(id,user_id,event_id,total_amount_cents,managed,lifecycle_generation,lifecycle_phase,currency,booking_mode,reservation_deadline) VALUES($1,$2,$3,725,true,1,'OPEN','usd','pessimistic',$4)`, booking, user, event, deadline); err != nil {
				t.Fatal(err)
			}
			if _, err := bookingDB.Exec(ctx, `INSERT INTO booking_items(booking_id,ticket_tier_id,quantity,unit_price_cents,seat_ids) VALUES($1,$2,1,725,$3)`, booking, tier, []uuid.UUID{seat}); err != nil {
				t.Fatal(err)
			}
			r := repository.NewSagaRepository(sagaDB)
			in, _, err := domain.CanonicalInput(domain.SagaInput{BookingID: booking, UserID: user, EventID: event, Generation: 1, SeatIDs: []uuid.UUID{seat}, AmountCents: 725, Currency: "usd", BookingMode: "pessimistic", ReservationDeadline: deadline})
			if err != nil {
				t.Fatal(err)
			}
			v, _, err := r.CreateOrGetSaga(ctx, in, initialSteps())
			if err != nil {
				t.Fatal(err)
			}
			lost := false
			interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
				// NOWAIT on the same Saga/step rows proves no Saga transaction spans RPC.
				tx, err := sagaDB.Begin(ctx)
				if err != nil {
					return nil, err
				}
				defer tx.Rollback(context.Background())
				if _, err = tx.Exec(ctx, `SELECT id FROM sagas WHERE id=$1 FOR UPDATE NOWAIT`, v.ID); err != nil {
					return nil, err
				}
				if _, err = tx.Exec(ctx, `SELECT id FROM saga_steps WHERE saga_id=$1 FOR UPDATE NOWAIT`, v.ID); err != nil {
					return nil, err
				}
				if err = tx.Commit(ctx); err != nil {
					return nil, err
				}
				result, err := next(ctx, req)
				lose := false
				switch command := req.(type) {
				case *bookingv1.LifecycleDecisionRequest:
					lose = scenario == "confirm_ack_lost" && command.Kind == "confirm"
				case *eventv1.SeatLifecycleRequest:
					lose = scenario == "book_ack_lost" && command.Action == "book" || scenario == "account_ack_lost" && command.Action == "account"
				}
				if err == nil && lose && !lost {
					lost = true
					return nil, status.Error(codes.Unavailable, "fixture ACK lost after committed effect")
				}
				return result, err
			}
			bconn := fixtureRPC(t, func(server *grpc.Server) {
				bookingv1.RegisterBookingServiceServer(server, bookingfixture.SagaReplayServer(bookingDB))
			}, interceptor)
			bc := bookingv1.NewBookingServiceClient(bconn)
			econn := fixtureRPC(t, func(server *grpc.Server) {
				eventv1.RegisterEventServiceServer(server, eventfixture.SagaReplayServer(eventDB))
			}, interceptor)
			ec := eventv1.NewEventServiceClient(econn)
			pconn := fixtureRPC(t, func(server *grpc.Server) {
				paymentv1.RegisterPaymentServiceServer(server, paymentfixture.SagaReplayServer(paymentDB, bc))
			}, interceptor)
			pc := paymentv1.NewPaymentServiceClient(pconn)
			paymentID, err := paymentfixture.SeedCollection(ctx, paymentDB, booking, user, 725)
			if err != nil {
				t.Fatal(err)
			}
			// Reserve is an actual production Event RPC with original S0 operation ID.
			if _, err := ec.ApplySeatLifecycle(ctx, seatRequest(in, v.Steps[0].ID, "reserve")); err != nil {
				t.Fatal(err)
			}
			if _, err := sagaDB.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"<2`, v.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := sagaDB.Exec(ctx, `UPDATE sagas SET current_step_index=2,status='PROCESSING' WHERE id=$1`, v.ID); err != nil {
				t.Fatal(err)
			}
			service := NewSagaService(zap.NewNop(), r, registry.NewSagaStepRegistry(), bc, pc, ec)
			var other uuid.UUID
			if scenario == "terminal_after_seat_reallocated" {
				decision := bookingDecisionID(booking, "cancel")
				if _, err := bc.DecideLifecycle(ctx, &bookingv1.LifecycleDecisionRequest{BookingId: booking.String(), Generation: 1, DecisionId: decision, Kind: "cancel", ExpectedVersion: 1}); err != nil {
					t.Fatal(err)
				}
				if _, err := ec.ApplySeatLifecycle(ctx, seatRequest(in, compensationID(v.Steps[0].ID), "close")); err != nil {
					t.Fatal(err)
				}
				other = uuid.New()
				b := in
				b.BookingID = other
				if _, err := ec.ApplySeatLifecycle(ctx, seatRequest(b, uuid.New(), "reserve")); err != nil {
					t.Fatal(err)
				}
			}
			_ = claimAndRun(t, service, r, v.ID)
			got, err := r.GetSagaById(ctx, v.ID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "book_ack_lost" || scenario == "account_ack_lost" {
				if _, err := service.SettleOrderSaga(ctx, booking, 1, "", paymentID.String(), "retry_unknown"); err != nil {
					t.Fatal(err)
				}
				if err := claimAndRun(t, service, r, v.ID); err != nil {
					t.Fatal(err)
				}
				got, err = r.GetSagaById(ctx, v.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			var available int
			var owner uuid.UUID
			var seatState, bookingState string
			var refundState string
			if err := eventDB.QueryRow(ctx, `SELECT available_quantity FROM ticket_tiers WHERE id=$1`, tier).Scan(&available); err != nil {
				t.Fatal(err)
			}
			if err := eventDB.QueryRow(ctx, `SELECT COALESCE(booking_id,reserved_by_booking_id),status FROM seats WHERE id=$1`, seat).Scan(&owner, &seatState); err != nil {
				t.Fatal(err)
			}
			if err := bookingDB.QueryRow(ctx, `SELECT status FROM bookings WHERE id=$1`, booking).Scan(&bookingState); err != nil {
				t.Fatal(err)
			}
			if err := paymentDB.QueryRow(ctx, `SELECT refund_state FROM payments WHERE id=$1`, paymentID).Scan(&refundState); err != nil {
				t.Fatal(err)
			}
			if scenario == "terminal_after_seat_reallocated" {
				if owner != other || seatState != "reserved" || available != 1 || bookingState != "CANCELLED" || refundState != "pending" || got.Status == domain.SAGA_ROLLED_BACK {
					t.Fatalf("terminal A harmed B or lost refund: %s/%s/%d/%s/%s", owner, seatState, available, bookingState, refundState)
				}
			} else {
				var outbox int
				if err := bookingDB.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_key=$1`, booking.String()).Scan(&outbox); err != nil {
					t.Fatal(err)
				}
				if got.Status != domain.SAGA_COMPLETED || owner != booking || seatState != "booked" || available != 0 || bookingState != "CONFIRMED" || refundState != "not_required" || outbox != 1 {
					t.Fatalf("forward guarded recovery incorrect: %s/%s/%d/%s/%s/outbox%d", got.Status, seatState, available, bookingState, refundState, outbox)
				}
			}
			for i, step := range got.Steps {
				if step.ID != v.Steps[i].ID {
					t.Fatal("guarded RPC replay replaced identity")
				}
			}
		})
	}
}
