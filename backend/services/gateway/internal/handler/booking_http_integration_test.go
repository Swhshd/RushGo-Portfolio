package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	bookingfixture "github.com/ticketbox/booking/testfixture"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type bookingHTTPEvent struct {
	eventv1.EventServiceClient
	event, tier, seat uuid.UUID
}

func (e bookingHTTPEvent) CheckEventSaleEligibility(context.Context, *eventv1.CheckEventSaleEligibilityRequest, ...grpc.CallOption) (*eventv1.CheckEventSaleEligibilityResponse, error) {
	return &eventv1.CheckEventSaleEligibilityResponse{State: eventv1.EventSaleEligibilityState_EVENT_SALE_ELIGIBILITY_STATE_ELIGIBLE}, nil
}

func (e bookingHTTPEvent) GetEvent(context.Context, *eventv1.GetEventRequest, ...grpc.CallOption) (*eventv1.EventDetail, error) {
	return &eventv1.EventDetail{Id: e.event.String()}, nil
}
func (e bookingHTTPEvent) GetSeatsBySeatIds(context.Context, *eventv1.GetSeatsBySeatIdsRequest, ...grpc.CallOption) (*eventv1.GetSeatsResponse, error) {
	return &eventv1.GetSeatsResponse{Seats: []*eventv1.Seat{{Id: e.seat.String(), EventId: e.event.String(), TicketTierId: e.tier.String(), Status: "available"}}}, nil
}
func (e bookingHTTPEvent) GetTicketAvailability(context.Context, *eventv1.GetTicketAvailabilityRequest, ...grpc.CallOption) (*eventv1.TicketAvailabilityResponse, error) {
	return &eventv1.TicketAvailabilityResponse{TierId: e.tier.String(), PriceCents: 725, AccountingReady: true}, nil
}

type bookingHTTPSaga struct {
	sagav1.SagaOrchestratorServiceClient
	mu    sync.Mutex
	ready bool
	id    string
}

func (s *bookingHTTPSaga) StartOrderSaga(context.Context, *sagav1.StartOrderSagaRequest, ...grpc.CallOption) (*sagav1.OrderSagaResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	secret := ""
	if s.ready {
		secret = "original_http_secret"
	}
	return &sagav1.OrderSagaResponse{SagaId: s.id, PaymentIntentClientSecret: secret, Status: "PROCESSING"}, nil
}

func TestBookingHTTPResponseLossRecoversOriginalRequestIntegration(t *testing.T) {
	u, err := url.Parse(os.Getenv("BOOKING_TEST_DATABASE_URL"))
	if err != nil || u.Host != "127.0.0.1:25435" || u.Path != "/ticketbox_booking_test" {
		t.Fatal("BLOCKED: isolated Booking PG required")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	schema := "booking_http_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public; CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	if out, err := exec.Command("migrate", "-path", "../../../booking/migrations", "-database", u.String(), "up").CombinedOutput(); err != nil {
		t.Fatalf("actual migrations: %s", out)
	}
	p, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	e := bookingHTTPEvent{event: uuid.New(), tier: uuid.New(), seat: uuid.New()}
	saga := &bookingHTTPSaga{id: uuid.NewString()}
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	bookingv1.RegisterBookingServiceServer(server, bookingfixture.RequestReplayServer(p, e, saga))
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() { server.Stop(); _ = lis.Close() })
	conn, err := grpc.NewClient("passthrough:///booking-http-fixture", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	h := NewBookingHandler(bookingv1.NewBookingServiceClient(conn))
	user := uuid.NewString()
	r := gin.New()
	r.POST("/api/bookings", func(c *gin.Context) { c.Set("user_id", user); h.CreateBooking(c) })
	httpServer := httptest.NewServer(r)
	defer httpServer.Close()
	payload := fmt.Sprintf(`{"event_id":%q,"items":[{"ticket_tier_id":%q,"quantity":1,"seat_ids":[%q]}]}`, e.event, e.tier, e.seat)
	post := func() (*http.Response, map[string]any) {
		t.Helper()
		req, err := http.NewRequest("POST", httpServer.URL+"/api/bookings", strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "lost_http_response")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return resp, body
	}
	first, body := post()
	if first.StatusCode != 202 || first.Header.Get("Retry-After") != "1" || body["paymentIntentClientSecret"] != "" {
		t.Fatal("pending dispatch HTTP contract", first.StatusCode, body)
	}
	id := body["id"]
	// Discard the successful response from the caller's view, then resend the
	// same authenticated HTTP request through the actual gRPC/PG boundaries.
	saga.mu.Lock()
	saga.ready = true
	saga.mu.Unlock()
	second, body := post()
	if second.StatusCode != 201 || body["id"] != id || body["paymentIntentClientSecret"] != "original_http_secret" || body["total_amount_cents"] != float64(725) {
		t.Fatal("response loss duplicated/repriced Booking", second.StatusCode, body)
	}
	var count int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM bookings WHERE user_id=$1 AND request_id='lost_http_response'`, user).Scan(&count); err != nil || count != 1 {
		t.Fatal("HTTP replay created duplicate", count, err)
	}
}
