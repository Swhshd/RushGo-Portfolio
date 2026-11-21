package handler_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/ticketbox/gateway/internal/router"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

const financialOwner = "11111111-1111-4111-8111-111111111111"
const financialOther = "22222222-2222-4222-8222-222222222222"
const financialBooking = "33333333-3333-4333-8333-333333333333"
const financialPayment = "44444444-4444-4444-8444-444444444444"

type financialAuthClient struct{ userv1.UserServiceClient }

func (financialAuthClient) ValidateToken(_ context.Context, req *userv1.ValidateTokenRequest, _ ...grpc.CallOption) (*userv1.ValidateTokenResponse, error) {
	return &userv1.ValidateTokenResponse{Valid: req.AccessToken == "fixture-valid", UserId: financialOwner, Email: "fixture@example.test", Role: "user"}, nil
}

type financialBookingClient struct {
	bookingv1.BookingServiceClient
	owner  string
	amount int64
	phase  string
}

func (b *financialBookingClient) GetLifecycleDisposition(context.Context, *bookingv1.LifecycleRequest, ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error) {
	return &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: financialBooking, UserId: b.owner, TotalAmountCents: b.amount, Status: "PENDING"}, Generation: 1, Phase: b.phase, Currency: "usd"}, nil
}

type financialPaymentClient struct {
	paymentv1.PaymentServiceClient
	owner              string
	creates, mutations int
	searchUser         string
	createState        string
	createRequest      *paymentv1.CreatePaymentRequest
}

func (p *financialPaymentClient) CreatePayment(_ context.Context, req *paymentv1.CreatePaymentRequest, _ ...grpc.CallOption) (*paymentv1.CreatePaymentResponse, error) {
	p.creates++
	p.createRequest = req
	return &paymentv1.CreatePaymentResponse{Id: financialPayment, Status: "pending", CreateState: p.createState, PaymentIntentClientSecret: "fixture_secret", RetryAfterSeconds: 1}, nil
}
func (p *financialPaymentClient) GetPaymentById(context.Context, *paymentv1.GetPaymentByIdReq, ...grpc.CallOption) (*paymentv1.PaymentEntry, error) {
	return &paymentv1.PaymentEntry{Id: financialPayment, UserId: p.owner, BookingId: financialBooking, Price: 725, Currency: "usd", Status: "success", ProviderOutcome: "collected", RefundState: "pending", RefundAmountCents: 725, BookingDisposition: "EXPIRED"}, nil
}
func (p *financialPaymentClient) GetPayments(_ context.Context, req *paymentv1.GetPaymentsReqeust, _ ...grpc.CallOption) (*paymentv1.PaymentList, error) {
	p.searchUser = req.UserId
	return &paymentv1.PaymentList{Payments: []*paymentv1.PaymentEntry{{Id: financialPayment, UserId: p.owner, ProviderOutcome: "collected", RefundState: "pending"}}}, nil
}
func (p *financialPaymentClient) UpdatePayment(context.Context, *paymentv1.UpdatePaymentRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	p.mutations++
	return &emptypb.Empty{}, nil
}
func (p *financialPaymentClient) DeletePayment(context.Context, *paymentv1.DeletePaymentRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	p.mutations++
	return &emptypb.Empty{}, nil
}

func TestFinancialCRUDCannotOverrideOwnerAmountOrOutcome(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:26379", MaxRetries: -1, DialTimeout: time.Second})
	defer rdb.Close()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatal("BLOCKED: isolated reliability Redis unavailable", err)
	}
	p := &financialPaymentClient{owner: financialOwner, createState: "create_ready"}
	b := &financialBookingClient{owner: financialOwner, amount: 725, phase: "OPEN"}
	r := router.SetupRouter(financialAuthClient{}, nil, b, p, nil, rdb, nil)
	request := func(method, path, body, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	body := `{"booking_id":"` + financialBooking + `","price":725,"currency":"usd","payment_method":"card"}`
	for _, token := range []string{"", "fixture-invalid"} {
		if w := request("POST", "/api/payments", body, token); w.Code != 401 {
			t.Error("missing/invalid JWT accepted", w.Code)
		}
	}
	for _, tc := range []struct {
		name, body string
		owner      string
		amount     int64
		code       int
	}{
		{"wrong_owner", body, financialOther, 725, 403},
		{"wrong_amount", strings.Replace(body, ":725", ":724", 1), financialOwner, 725, 409},
		{"wrong_currency", strings.Replace(body, "usd", "eur", 1), financialOwner, 725, 409},
		{"client_overflow", strings.Replace(body, ":725", ":2147483648", 1), financialOwner, 725, 400},
		{"server_overflow", body, financialOwner, 2147483648, 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b.owner, b.amount = tc.owner, tc.amount
			before := p.creates
			w := request("POST", "/api/payments", tc.body, "fixture-valid")
			if w.Code != tc.code || p.creates != before {
				t.Errorf("financial authority bypass code=%d creates=%d body=%s", w.Code, p.creates, w.Body.String())
			}
		})
	}
	b.owner, b.amount = financialOwner, 725
	p.owner = financialOther
	for _, tc := range []struct{ method, path, body string }{{"GET", "/api/payments/" + financialPayment, ""}, {"DELETE", "/api/payments/" + financialPayment, ""}, {"POST", "/api/payments/search", `{"user_id":"` + financialOther + `"}`}} {
		w := request(tc.method, tc.path, tc.body, "fixture-valid")
		if w.Code != 403 || strings.Contains(w.Body.String(), financialPayment) {
			t.Error("other user's financial record exposed/mutated", w.Code, w.Body.String())
		}
	}
	p.owner = financialOwner
	for _, method := range []string{"PATCH", "DELETE"} {
		w := request(method, "/api/payments/"+financialPayment, `{"id":"`+financialPayment+`","status":"fail","order_id":"`+financialBooking+`","transaction_id":"`+financialBooking+`"}`, "fixture-valid")
		if w.Code != 409 || !strings.Contains(w.Body.String(), "financial_state_protected") {
			t.Error("protected mutation accepted", w.Code, w.Body.String())
		}
	}
	if p.mutations != 0 {
		t.Error("public financial mutation reached Payment service")
	}
	if w := request("GET", "/api/payments/"+financialPayment, "", "fixture-valid"); w.Code != 200 || !strings.Contains(w.Body.String(), `"refund_state":"pending"`) || !strings.Contains(w.Body.String(), `"booking_disposition":"EXPIRED"`) {
		t.Error("authoritative financial read missing", w.Body.String())
	}
	w := request("POST", "/api/payments/search", `{"status":"success"}`, "fixture-valid")
	if w.Code != 200 || p.searchUser != financialOwner {
		t.Error("list not scoped to authenticated user", w.Code)
	}
	if w = request("POST", "/api/payments", body, "fixture-valid"); w.Code != http.StatusCreated || p.creates != 1 {
		t.Error("own authoritative create unavailable", w.Code, w.Body.String())
	}
	if p.createRequest.UserId != financialOwner || p.createRequest.BookingId != financialBooking || p.createRequest.Price != 725 || p.createRequest.Currency != "usd" || p.createRequest.PaymentGeneration != 1 {
		t.Fatal("create lost authority snapshot")
	}
	p.createState = "create_unknown"
	if w = request("POST", "/api/payments", body, "fixture-valid"); w.Code != 202 || w.Header().Get("Retry-After") != "1" {
		t.Fatal("unknown create did not retain retry contract", w.Code, w.Body.String())
	}
	b.phase = "TERMINAL"
	p.createState = "create_ready"
	if w = request("POST", "/api/payments", body, "fixture-valid"); w.Code != 200 || strings.Contains(w.Body.String(), "fixture_secret") {
		t.Fatal("terminal recovery disclosed payable secret", w.Code, w.Body.String())
	}
}
