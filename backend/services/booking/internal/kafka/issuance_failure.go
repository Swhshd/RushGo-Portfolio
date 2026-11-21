package kafka

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ticketbox/booking/internal/domain"
	"go.uber.org/zap"
	"google.golang.org/grpc/status"
	"regexp"
	"runtime/debug"
)

var issuanceSQLState = regexp.MustCompile(`^[A-Z0-9]{5}$`)
var issuanceRevision = func() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				return setting.Value
			}
		}
	}
	return "unknown"
}()

func logIssuanceFailure(logger *zap.Logger, event ticketIssuanceRequestedV1, err error) {
	stage, category := "processor", "unclassified"
	var failure *domain.IssuanceFailure
	if errors.As(err, &failure) {
		stage = failure.Stage
	}
	fields := []zap.Field{zap.String("stage", stage), zap.String("source_commit", issuanceRevision),
		zap.String("issuance_id", event.TicketIssuanceID.String()), zap.String("booking_id", event.BookingID.String()),
		zap.String("saga_id", event.SagaID.String()), zap.String("payment_id", event.PaymentID.String()), zap.Int64("generation", event.Generation)}
	if failure != nil && failure.Reason != "" {
		reason := "unclassified"
		switch failure.Reason {
		case "missing_response", "outside_approved_test_scope", "outside_one_payment_scope", "provider_evidence_unavailable",
			"local_payment_unavailable", "local_financial_identity_unverified", "local_refund_state_unverified",
			"local_financial_state_unverified", "provider_read_unavailable", "provider_evidence_unknown", "provider_evidence_incomplete":
			reason = failure.Reason
		}
		fields = append(fields, zap.String("reason", reason))
	}
	var pg *pgconn.PgError
	switch {
	case errors.As(err, &pg):
		category = "postgres"
		if issuanceSQLState.MatchString(pg.Code) {
			fields = append(fields, zap.String("sqlstate", pg.Code))
		}
	case errors.Is(err, context.DeadlineExceeded):
		category = "deadline"
	case errors.Is(err, context.Canceled):
		category = "canceled"
	default:
		if st, ok := status.FromError(err); ok {
			category = "grpc"
			fields = append(fields, zap.String("grpc_code", st.Code().String()))
		}
	}
	fields = append(fields, zap.String("category", category))
	// Never log err.Error(), SQL detail, full payload, DSN or provider responses.
	logger.Error("Ticket issuance processing failed", fields...)
}
