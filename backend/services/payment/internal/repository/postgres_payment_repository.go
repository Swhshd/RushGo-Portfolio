package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
)

var RecordNotFound = errors.New("payment record not found")

type PostgresPaymentRepository struct {
	pool                 *pgxpool.Pool
	beginRefundPreSendTx func(context.Context) (refundPreSendTx, error)
}

func NewPostgresPaymentRepository(pool *pgxpool.Pool) *PostgresPaymentRepository {
	return &PostgresPaymentRepository{
		pool: pool,
		beginRefundPreSendTx: func(ctx context.Context) (refundPreSendTx, error) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				return nil, err
			}
			return tx, nil
		},
	}
}

const paymentColumns = `p.id,p.user_id,p.booking_id,p.order_id,p.status,p.price,p.currency,
	p.transaction_id,p.payment_intent_id,p.payment_method,p.created_at`

// Scan optional values into pointers: an absent UUID is not the zero UUID.
func paymentScanTargets(p *domain.Payment, status, intent, method **string) []any {
	return []any{&p.ID, &p.UserId, &p.BookingId, &p.OrderId, status, &p.Price, &p.Currency,
		&p.Transaction_id, intent, method, &p.CreatedAt}
}

func finishPaymentScan(p *domain.Payment, status, intent, method *string) {
	if status != nil {
		p.Status = domain.PaymentStatus(*status)
	}
	if intent != nil {
		p.PaymentIntentId = *intent
	}
	if method != nil {
		p.PaymentMethod = *method
	}
}

func scanPayment(row pgx.Row) (*domain.Payment, error) {
	p := &domain.Payment{}
	var status, intent, method *string
	if err := row.Scan(paymentScanTargets(p, &status, &intent, &method)...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, RecordNotFound
		}
		return nil, fmt.Errorf("scan payment: %w", err)
	}
	finishPaymentScan(p, status, intent, method)
	return p, nil
}

func collectPayments(rows pgx.Rows) ([]domain.Payment, error) {
	defer rows.Close()
	payments := []domain.Payment{}
	for rows.Next() {
		p, err := scanPayment(rows)
		if err != nil {
			return nil, err
		}
		payments = append(payments, *p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate payments: %w", err)
	}
	return payments, nil
}

func (p *PostgresPaymentRepository) CreatePayment(ctx context.Context, payment *domain.Payment) error {
	return ErrFinancialStateProtected
}

func (p *PostgresPaymentRepository) GetPaymentByID(ctx context.Context, id uuid.UUID) (*domain.Payment, error) {
	return scanPayment(p.pool.QueryRow(ctx, `SELECT `+paymentColumns+` FROM payments p WHERE p.id=$1`, id))
}

func (p *PostgresPaymentRepository) GetPaymentByPaymentIntentId(ctx context.Context, intentID string) (*domain.Payment, error) {
	return scanPayment(p.pool.QueryRow(ctx, `SELECT `+paymentColumns+` FROM payments p WHERE p.payment_intent_id=$1`, intentID))
}

func (p *PostgresPaymentRepository) GetListPaymentsByCondition(ctx context.Context, req *domain.GetPaymentsByCondition) ([]domain.Payment, error) {
	if req == nil {
		req = &domain.GetPaymentsByCondition{}
	}
	rows, err := p.pool.Query(ctx, `SELECT `+paymentColumns+` FROM payments p
		WHERE ($1::text='' OR p.status::text=$1) AND ($2::uuid IS NULL OR p.user_id=$2)
		AND ($3::uuid IS NULL OR p.booking_id=$3) AND ($4::bigint=0 OR p.price=$4)
		AND ($5::text='' OR p.currency=$5) AND ($6::text='' OR p.payment_method=$6) ORDER BY p.id`,
		req.Status, req.UserId, req.BookingId, req.Price, req.Currency, req.PaymentMethod)
	if err != nil {
		return nil, err
	}
	return collectPayments(rows)
}

func (p *PostgresPaymentRepository) UpdatePaymentStatus(ctx context.Context, id uuid.UUID, status domain.PaymentStatus) error {
	return p.rejectFinancialMutation(ctx, id)
}

func (p *PostgresPaymentRepository) UpdatePaymentStatusByPaymentIntentId(ctx context.Context, intentID string, status domain.PaymentStatus) error {
	if _, err := p.GetPaymentByPaymentIntentId(ctx, intentID); err != nil {
		return err
	}
	return ErrFinancialStateProtected
}

func (p *PostgresPaymentRepository) UpdatePayment(ctx context.Context, payment *domain.Payment) error {
	if payment == nil {
		return ErrInvalidFinancialIdentity
	}
	return p.rejectFinancialMutation(ctx, payment.ID)
}

func (p *PostgresPaymentRepository) SoftDeletePayment(ctx context.Context, id uuid.UUID) error {
	return p.rejectFinancialMutation(ctx, id)
}

func (p *PostgresPaymentRepository) DeletePayment(ctx context.Context, id uuid.UUID) error {
	return p.rejectFinancialMutation(ctx, id)
}

func (p *PostgresPaymentRepository) rejectFinancialMutation(ctx context.Context, id uuid.UUID) error {
	if _, err := p.GetPaymentByID(ctx, id); err != nil {
		return err
	}
	return ErrFinancialStateProtected
}

func (p *PostgresPaymentRepository) GetPaymentsByBookingIds(ctx context.Context, bookingIDs []uuid.UUID) ([]domain.Payment, error) {
	rows, err := p.pool.Query(ctx, `SELECT `+paymentColumns+` FROM payments p WHERE p.booking_id=ANY($1) ORDER BY p.id`, bookingIDs)
	if err != nil {
		return nil, err
	}
	return collectPayments(rows)
}
