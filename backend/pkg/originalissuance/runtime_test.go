package originalissuance

import (
	"context"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"testing"
)

func TestTargetOnlyRejectsForeignIdentityBeforeHandler(t *testing.T) {
	for _, r := range []*paymentv1.AdmissionFinancialEligibilityRequest{
		{BookingId: "other", PaymentId: Payment, Generation: 1},
		{BookingId: Booking, PaymentId: "other", Generation: 1},
		{BookingId: Booking, PaymentId: Payment, Generation: 2},
	} {
		called := false
		_, err := TargetOnly(context.Background(), r, nil, func(context.Context, any) (any, error) { called = true; return nil, nil })
		if err == nil || called {
			t.Fatal("foreign identity reached handler")
		}
	}
	called := false
	_, err := TargetOnly(context.Background(), &paymentv1.AdmissionFinancialEligibilityRequest{BookingId: Booking, PaymentId: Payment, Generation: 1}, nil, func(context.Context, any) (any, error) { called = true; return nil, nil })
	if err != nil || !called {
		t.Fatal("original identity refused")
	}
}

func TestOpenRejectsNetworkOrForeignDatabaseBeforeConnect(t *testing.T) {
	t.Setenv("RUSHGO_ORIGINAL_ISSUANCE_RUN", Run)
	for _, dsn := range []string{
		"host=127.0.0.1 user=r4_booking_4b7a98d635d5 dbname=r4_booking_4b7a98d635d5",
		"host=/tmp user=postgres dbname=r4_booking_4b7a98d635d5",
		"host=/tmp user=other dbname=other",
	} {
		t.Setenv("DATABASE_URL", dsn)
		p, err := Open(context.Background(), "booking", false)
		if err == nil || p != nil || err.Error() != "database_scope_mismatch" {
			t.Fatal("foreign database not rejected before connection")
		}
	}
}
