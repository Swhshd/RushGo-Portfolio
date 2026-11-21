package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"github.com/ticketbox/pkg/reliabilityhook"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const sagaInterceptorTestToken = "saga-interceptor-tests-token-0001"

func TestSagaLegacyServerOptionsObserveStartRequestBeforeHandler(t *testing.T) {
	const (
		bookingID = "44444444-4444-4444-8444-444444444444"
		windowID  = "booking.saga_ack_lost"
		nonce     = "saga-interceptor-nonce"
	)
	method := sagav1.SagaOrchestratorService_StartOrderSaga_FullMethodName
	directory := t.TempDir()
	hook, err := reliabilityhook.New(reliabilityhook.Config{Enabled: true, Token: sagaInterceptorTestToken, Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	command := reliabilityhook.Command{WindowID: windowID, OperationID: bookingID, Checkpoint: method,
		Action: reliabilityhook.ActionAckGate, Token: sagaInterceptorTestToken, Nonce: nonce}
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "command.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	key := sagaInterceptorBoundaryKey(windowID, bookingID, method)
	receiptPath := filepath.Join(directory, "request-"+key+".json")
	serverImplementation := &sagaObserverReceiptServer{
		receiptPath: receiptPath,
		receiptSeen: make(chan reliabilityhook.RequestReceipt, 1),
		release:     make(chan struct{}),
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(sagaLegacyUnaryServerInterceptors(hook, zap.NewNop())...))
	sagav1.RegisterSagaOrchestratorServiceServer(grpcServer, serverImplementation)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := sagav1.NewSagaOrchestratorServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	call := make(chan error, 1)
	go func() {
		response, err := client.StartOrderSaga(ctx, &sagav1.StartOrderSagaRequest{BookingId: bookingID})
		if err == nil && (response == nil || response.SagaId != "55555555-5555-4555-8555-555555555555") {
			err = errors.New("StartOrderSaga returned an unexpected response")
		}
		call <- err
	}()

	select {
	case receipt := <-serverImplementation.receiptSeen:
		if receipt.WindowID != windowID || receipt.OperationID != bookingID || receipt.Checkpoint != method || receipt.Nonce != nonce || receipt.ReceivedAt == "" {
			t.Fatalf("Saga handler did not observe the exact pre-handler request receipt: %+v", receipt)
		}
	case <-ctx.Done():
		t.Fatalf("Saga StartOrderSaga handler did not observe the request receipt: %v", ctx.Err())
	}
	close(serverImplementation.release)
	select {
	case err := <-call:
		if err != nil {
			t.Fatalf("StartOrderSaga failed after the receipt check: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("StartOrderSaga did not finish after releasing the handler: %v", ctx.Err())
	}
	if _, err := client.VerifyTicketIssuanceSaga(ctx, &sagav1.VerifyTicketIssuanceSagaRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("legacy Saga listener Ticket Authority method status = %v, want Unauthenticated", status.Code(err))
	}
}

type sagaObserverReceiptServer struct {
	sagav1.UnimplementedSagaOrchestratorServiceServer
	receiptPath string
	receiptSeen chan reliabilityhook.RequestReceipt
	release     chan struct{}
}

func (s *sagaObserverReceiptServer) StartOrderSaga(ctx context.Context, _ *sagav1.StartOrderSagaRequest) (*sagav1.OrderSagaResponse, error) {
	raw, err := os.ReadFile(s.receiptPath)
	if err != nil {
		return nil, err
	}
	var receipt reliabilityhook.RequestReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return nil, err
	}
	s.receiptSeen <- receipt
	select {
	case <-s.release:
		return &sagav1.OrderSagaResponse{SagaId: "55555555-5555-4555-8555-555555555555", Status: "RUNNING"}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func sagaInterceptorBoundaryKey(windowID, operationID, checkpoint string) string {
	digest := sha256.Sum256([]byte(windowID + "\x00" + operationID + "\x00" + checkpoint))
	return hex.EncodeToString(digest[:16])
}
