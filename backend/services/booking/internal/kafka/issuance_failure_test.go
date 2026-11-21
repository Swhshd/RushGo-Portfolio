package kafka

import (
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ticketbox/booking/internal/domain"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"testing"
)

func TestIssuanceLogRetainsSQLStateWithoutErrorPayload(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	cause := &pgconn.PgError{Code: "23505", Message: "sk_test_PRIVATE", Detail: "postgres://private"}
	err := domain.WrapIssuanceFailure("apply_transaction", cause)
	logIssuanceFailure(zap.New(core), ticketIssuanceRequestedV1{}, err)
	if !errors.Is(err, cause) || logs.Len() != 1 {
		t.Fatal("cause or log missing")
	}
	fields := logs.All()[0].ContextMap()
	if fields["sqlstate"] != "23505" || fields["stage"] != "apply_transaction" {
		t.Fatal(fields)
	}
	raw, _ := json.Marshal(fields)
	for _, value := range []string{"sk_test_PRIVATE", "postgres://private"} {
		for i := 0; i+len(value) <= len(raw); i++ {
			if string(raw[i:i+len(value)]) == value {
				t.Fatal("secret leaked")
			}
		}
	}
}
