package repository

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestGetPaymentsByBookingIDsScansNullableIntentAndTransactionIDs(t *testing.T) {
	databaseURL := os.Getenv("PAYMENT_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("PAYMENT_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	adminPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to payment integration database: %v", err)
	}
	schema := "payment_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminPool.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		adminPool.Close()
		t.Fatalf("create isolated payment schema: %v", err)
	}
	t.Cleanup(func() {
		_, dropErr := adminPool.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		adminPool.Close()
		if dropErr != nil {
			t.Errorf("drop isolated payment schema: %v", dropErr)
		}
	})

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse payment integration database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect to isolated payment schema: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `CREATE TABLE payments (
		id UUID PRIMARY KEY,
		user_id UUID NOT NULL,
		booking_id UUID,
		order_id UUID,
		status TEXT,
		price BIGINT NOT NULL,
		currency TEXT NOT NULL,
		transaction_id UUID,
		payment_intent_id VARCHAR(250),
		payment_method TEXT,
		created_at TIMESTAMPTZ NOT NULL
	)`); err != nil {
		t.Fatalf("create isolated payments table: %v", err)
	}
	bookingID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO payments (id,user_id,booking_id,order_id,status,price,currency,transaction_id,payment_intent_id,payment_method,created_at)
		VALUES ($1,$2,$3,$4,'pending',500,'usd',NULL,NULL,'card',$5)`, uuid.New(), uuid.New(), bookingID, uuid.New(), time.Now()); err != nil {
		t.Fatalf("insert pending payment with nullable identifiers: %v", err)
	}

	payments, err := NewPostgresPaymentRepository(pool).GetPaymentsByBookingIds(ctx, []uuid.UUID{bookingID})
	if err != nil {
		t.Fatalf("get payments by booking IDs: %v", err)
	}
	if len(payments) != 1 || payments[0].PaymentIntentId != "" || payments[0].Transaction_id != nil {
		t.Fatalf("payments = %#v, want one payment with empty intent and nil transaction ID", payments)
	}
}
