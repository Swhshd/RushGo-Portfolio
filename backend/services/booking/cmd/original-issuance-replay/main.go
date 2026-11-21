// A bounded replay of the original envelope through the existing handler and
// service. Original execution is a separate, explicit single-order maintenance scope.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ticketbox/booking/internal/domain"
	bookingkafka "github.com/ticketbox/booking/internal/kafka"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	"github.com/ticketbox/pkg/grpcsecurity"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/originalissuance"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"regexp"
)

var originalEnvelopeBytes []byte

func recoveryScope(scope, attempt string) (bool, error) {
	if scope == "isolated_snapshot_copy" && attempt == "" {
		return false, nil
	}
	if scope == "guarded_original_recovery" {
		id, err := uuid.Parse(attempt)
		if err == nil && id != uuid.Nil && id.String() == attempt {
			return true, nil
		}
	}
	return false, errors.New("explicit_issuance_scope_required")
}

type guardedStore struct {
	repository.TicketIssuanceRepository
}

func (s guardedStore) ApplyTicketIssuance(ctx context.Context, e domain.TicketIssuanceEvent, c domain.TicketIssuanceCandidate, d domain.TicketIssuanceDecision, a domain.TicketAuthorityEvidence) error {
	if !d.Eligible {
		return &domain.IssuanceFailure{Stage: "eligibility_denied", Reason: d.ReasonCode, Cause: errors.New("denied_no_write")}
	}
	return s.TicketIssuanceRepository.ApplyTicketIssuance(ctx, e, c, d, a)
}

func connect(service string) (*grpc.ClientConn, error) {
	address := os.Getenv("ORIGINAL_" + service + "_ADDRESS")
	host, _, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		return nil, errors.New("authority_address_scope")
	}
	var creds credentials.TransportCredentials
	if service == "payment" {
		var test bool
		creds, test, err = grpcsecurity.ClientCredentialsFromEnv(service, grpcsecurity.ServiceIdentityBooking)
		if test {
			return nil, errors.New("plaintext_forbidden")
		}
	} else {
		creds, err = grpcsecurity.TicketAuthorityClientCredentialsFromEnv(service)
	}
	if err != nil {
		return nil, errors.New("authority_tls_configuration")
	}
	return grpc.NewClient(address, grpc.WithTransportCredentials(creds))
}

func main() {
	ctx, cancel := originalissuance.Context()
	defer cancel()
	out := map[string]any{"runId": originalissuance.Run, "sourceCommit": originalissuance.BuildSource, "originalOrderTouched": false, "financialWrites": 0, "kafkaOffsetsChanged": false, "scope": "isolated_snapshot_copy"}
	stage := "scope"
	err := func() error {
		recovery, scopeErr := recoveryScope(os.Getenv("RUSHGO_ISSUANCE_SCOPE"), os.Getenv("RUSHGO_ORIGINAL_RECOVERY_ATTEMPT"))
		if scopeErr != nil {
			return scopeErr
		}
		if recovery {
			out["scope"] = "guarded_original_recovery"
			out["attemptId"] = os.Getenv("RUSHGO_ORIGINAL_RECOVERY_ATTEMPT")
			out["issuanceOutcome"] = "NOT_ATTEMPTED"
			out["automaticReplayAllowed"] = false
		}
		action := os.Getenv("RUSHGO_ISSUANCE_ACTION")
		verifyOnly := action == "verify_only" && recovery
		if action != "" && !verifyOnly {
			return errors.New("invalid_issuance_action")
		}
		stage = "original_message"
		report, err := inspectArchive(os.Getenv("ORIGINAL_KAFKA_SNAPSHOT"))
		if err != nil {
			return err
		}
		if report["status"] != "MESSAGE_CHECK_PASS" {
			return errors.New("original_message_invalid")
		}
		stage = "database"
		pool, err := originalissuance.Open(ctx, "booking", verifyOnly)
		if err != nil {
			return err
		}
		defer pool.Close()
		repo := repository.NewPostgresBookingRepository(pool)
		candidate, err := repo.LoadTicketIssuanceCandidate(ctx, originalEvent)
		if err != nil {
			return err
		}
		if !validOriginalCandidate(candidate) {
			return errors.New("original_order_shape_mismatch")
		}
		if verifyOnly {
			stage = "verify_only"
			out["issuanceOutcome"] = "UNKNOWN"
			if err := verifyIssued(ctx, pool, candidate, out); err != nil {
				return err
			}
			out["issuanceOutcome"] = "COMMIT_VERIFIED"
			return nil
		}
		stage = "before_state"
		var tickets, receipts, requests int
		err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM tickets WHERE booking_id=$1),(SELECT count(*) FROM ticket_issuance_receipts WHERE booking_id=$1),(SELECT count(*) FROM booking_refund_requests WHERE booking_id=$1)`, originalEvent.BookingID).Scan(&tickets, &receipts, &requests)
		if err != nil {
			return err
		}
		if tickets != 0 || receipts != 0 || requests != 0 {
			return errors.New("not_original_unissued_state")
		}
		stage = "authority_clients"
		e, err := connect("event")
		if err != nil {
			return err
		}
		defer e.Close()
		s, err := connect("saga")
		if err != nil {
			return err
		}
		defer s.Close()
		p, err := connect("payment")
		if err != nil {
			return err
		}
		defer p.Close()
		authority := service.NewGRPCTicketAuthority(eventv1.NewEventServiceClient(e), sagav1.NewSagaOrchestratorServiceClient(s), paymentv1.NewPaymentServiceClient(p))
		svc := service.NewTicketIssuanceService(guardedStore{repo}, authority)
		var envelope pkgkafka.Event
		if json.Unmarshal(originalEnvelopeBytes, &envelope) != nil {
			return errors.New("envelope_invalid")
		}
		logger, _ := zap.NewProduction()
		defer logger.Sync()
		handler := bookingkafka.TicketIssuanceHandler(svc, logger)
		stage = "consumer_handler"
		if recovery {
			out["originalOrderTouched"] = true // May write; ambiguous failures remain UNKNOWN.
			out["issuanceOutcome"] = "UNKNOWN"
		}
		if err = handler(ctx, envelope); err != nil {
			return err
		}
		verify := func() error {
			return verifyIssued(ctx, pool, candidate, out)
		}
		stage = "verify_commit"
		if err = verify(); err != nil {
			return err
		}
		if recovery {
			out["issuanceOutcome"] = "COMMIT_VERIFIED"
			out["duplicateReplay"] = "NOT_RUN_ORIGINAL"
			return nil
		}
		stage = "duplicate_handler"
		if err = handler(ctx, envelope); err != nil {
			return err
		}
		stage = "verify_duplicate"
		if err = verify(); err != nil {
			return err
		}
		out["duplicateReplay"] = "PASS"
		return nil
	}()
	out["stage"] = stage
	out["status"] = "PASS"
	if err != nil {
		out["status"] = "FAIL"
		out["category"] = "validation_or_dependency"
		var failure *domain.IssuanceFailure
		if errors.As(err, &failure) {
			out["stage"] = failure.Stage
			if regexp.MustCompile(`^[a-z_]{1,80}$`).MatchString(failure.Reason) {
				out["reason"] = failure.Reason
			}
		}
		var pg *pgconn.PgError
		if errors.As(err, &pg) {
			out["category"] = "postgres"
			out["sqlState"] = pg.Code
		}
		if st, ok := status.FromError(err); ok {
			out["category"] = "grpc"
			out["grpcCode"] = st.Code().String()
		}
	}
	_ = json.NewEncoder(os.Stdout).Encode(out)
	if err != nil {
		os.Exit(1)
	}
}
