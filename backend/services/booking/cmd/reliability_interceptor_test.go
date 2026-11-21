package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"github.com/ticketbox/pkg/reliabilityhook"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const bookingInterceptorTestToken = "booking-interceptor-tests-token-0001"

func TestBookingSagaDialOptionsInstallReliabilityAckGate(t *testing.T) {
	const (
		bookingID = "11111111-1111-4111-8111-111111111111"
		windowID  = "booking.saga_ack_lost"
	)
	method := sagav1.SagaOrchestratorService_StartOrderSaga_FullMethodName
	hook, directory, key, nonce := newBookingInterceptorHook(t, windowID, bookingID, method)
	_, target := startBookingInterceptorServer(t, func(server *grpc.Server) {
		sagav1.RegisterSagaOrchestratorServiceServer(server, bookingInterceptorSagaServer{})
	})

	conn, err := grpc.NewClient(target, bookingSagaClientDialOptions(hook,
		grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := sagav1.NewSagaOrchestratorServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan struct {
		response *sagav1.OrderSagaResponse
		err      error
	}, 1)
	go func() {
		response, err := client.StartOrderSaga(ctx, &sagav1.StartOrderSagaRequest{BookingId: bookingID})
		result <- struct {
			response *sagav1.OrderSagaResponse
			err      error
		}{response: response, err: err}
	}()

	barrier := waitBookingInterceptorJSON[reliabilityhook.Barrier](t, filepath.Join(directory, "barrier-"+key+".json"))
	if barrier.WindowID != windowID || barrier.OperationID != bookingID || barrier.Checkpoint != method || barrier.Nonce != nonce {
		t.Fatalf("Saga client ACK interceptor wrote unexpected barrier: %+v", barrier)
	}
	writeBookingInterceptorDecision(t, directory, key, nonce)
	select {
	case got := <-result:
		if got.err != nil || got.response == nil || got.response.SagaId != "22222222-2222-4222-8222-222222222222" {
			t.Fatalf("StartOrderSaga result after ACK decision = (%+v, %v)", got.response, got.err)
		}
	case <-ctx.Done():
		t.Fatalf("StartOrderSaga remained blocked after continue decision: %v", ctx.Err())
	}
}

func TestBookingServerOptionsInstallTerminalAckGateAfterHandler(t *testing.T) {
	const bookingID = "33333333-3333-4333-8333-333333333333"
	method := bookingv1.BookingService_CancelBooking_FullMethodName
	hook, directory, key, nonce := newBookingInterceptorHook(t, "booking.terminal_ack_lost", bookingID, method)
	stub := &bookingInterceptorBookingServer{}
	_, target := startBookingInterceptorServer(t, func(server *grpc.Server) {
		bookingv1.RegisterBookingServiceServer(server, stub)
	}, grpc.ChainUnaryInterceptor(bookingUnaryServerInterceptors(hook, zap.NewNop())...))

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := bookingv1.NewBookingServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan struct {
		response *bookingv1.BookingDetail
		err      error
	}, 1)
	go func() {
		response, err := client.CancelBooking(ctx, &bookingv1.CancelBookingRequest{BookingId: bookingID})
		result <- struct {
			response *bookingv1.BookingDetail
			err      error
		}{response: response, err: err}
	}()

	barrier := waitBookingInterceptorJSON[reliabilityhook.Barrier](t, filepath.Join(directory, "barrier-"+key+".json"))
	if barrier.WindowID != "booking.terminal_ack_lost" || barrier.OperationID != bookingID || barrier.Checkpoint != method {
		t.Fatalf("Booking server ACK interceptor wrote unexpected barrier: %+v", barrier)
	}
	if got := stub.calls.Load(); got != 1 {
		t.Fatalf("CancelBooking handler calls before ACK barrier = %d, want 1", got)
	}
	writeBookingInterceptorDecision(t, directory, key, nonce)
	select {
	case got := <-result:
		if got.err != nil || got.response == nil || got.response.Status != "CANCELLED" {
			t.Fatalf("CancelBooking result after ACK decision = (%+v, %v)", got.response, got.err)
		}
	case <-ctx.Done():
		t.Fatalf("CancelBooking remained blocked after continue decision: %v", ctx.Err())
	}
}

type bookingInterceptorSagaServer struct {
	sagav1.UnimplementedSagaOrchestratorServiceServer
}

func (bookingInterceptorSagaServer) StartOrderSaga(context.Context, *sagav1.StartOrderSagaRequest) (*sagav1.OrderSagaResponse, error) {
	return &sagav1.OrderSagaResponse{SagaId: "22222222-2222-4222-8222-222222222222", Status: "RUNNING"}, nil
}

type bookingInterceptorBookingServer struct {
	bookingv1.UnimplementedBookingServiceServer
	calls atomic.Int32
}

func (s *bookingInterceptorBookingServer) CancelBooking(_ context.Context, request *bookingv1.CancelBookingRequest) (*bookingv1.BookingDetail, error) {
	s.calls.Add(1)
	return &bookingv1.BookingDetail{Id: request.BookingId, Status: "CANCELLED"}, nil
}

func newBookingInterceptorHook(t *testing.T, windowID, operationID, checkpoint string) (*reliabilityhook.Controller, string, string, string) {
	t.Helper()
	directory := t.TempDir()
	const nonce = "booking-interceptor-nonce"
	hook, err := reliabilityhook.New(reliabilityhook.Config{Enabled: true, Token: bookingInterceptorTestToken, Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	command := reliabilityhook.Command{WindowID: windowID, OperationID: operationID, Checkpoint: checkpoint,
		Action: reliabilityhook.ActionAckGate, Token: bookingInterceptorTestToken, Nonce: nonce}
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "command.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return hook, directory, bookingInterceptorBoundaryKey(windowID, operationID, checkpoint), nonce
}

func bookingInterceptorBoundaryKey(windowID, operationID, checkpoint string) string {
	digest := sha256.Sum256([]byte(windowID + "\x00" + operationID + "\x00" + checkpoint))
	return hex.EncodeToString(digest[:16])
}

func writeBookingInterceptorDecision(t *testing.T, directory, key, nonce string) {
	t.Helper()
	decision := reliabilityhook.ControlDecision{Token: bookingInterceptorTestToken, Nonce: nonce,
		Decision: reliabilityhook.DecisionContinue}
	raw, err := json.Marshal(decision)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "decision-"+key+".json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func waitBookingInterceptorJSON[T any](t *testing.T, path string) T {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if raw, err := os.ReadFile(path); err == nil {
			var value T
			if err := json.Unmarshal(raw, &value); err != nil {
				t.Fatal(err)
			}
			return value
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for reliability hook file %q", path)
	var zero T
	return zero
}

func startBookingInterceptorServer(t *testing.T, register func(*grpc.Server), options ...grpc.ServerOption) (net.Listener, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(options...)
	register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener, listener.Addr().String()
}
