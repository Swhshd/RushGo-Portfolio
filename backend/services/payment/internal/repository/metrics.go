package repository

import (
	"context"
	"github.com/ticketbox/payment/internal/domain"
	"time"
)

type MetricsRepository interface {
	MoneyMetrics(context.Context, time.Time, time.Time) (*domain.MoneyMetrics, error)
}

func (r *PostgresPaymentRepository) MoneyMetrics(ctx context.Context, from, to time.Time) (*domain.MoneyMetrics, error) {
	if !from.Before(to) || to.Sub(from) > 31*24*time.Hour {
		return nil, domain.ErrMetricsBounds
	}
	// This is an observation of current Payment records created in the window,
	// not a settlement ledger or a claim about external provider balances.
	rows, err := r.pool.Query(ctx, `WITH scope AS MATERIALIZED (
 SELECT *,CASE WHEN livemode IS TRUE THEN 'live' WHEN livemode IS FALSE THEN 'test' ELSE 'unknown' END mode
 FROM payments WHERE created_at>=$1 AND created_at<$2 ORDER BY created_at,id LIMIT 10001
 ), facts AS (
 SELECT *, managed AND provider_outcome='collected' AND status='success' AND provider_observed_at IS NOT NULL collected_known,
 managed AND refund_state='succeeded' AND refund_provider_status='succeeded' AND refund_provider_id IS NOT NULL AND refund_provider_id<>'' AND refund_completed_at IS NOT NULL AND refund_currency=currency AND refund_amount_cents BETWEEN 1 AND price refund_known
 FROM scope
 ) SELECT COALESCE(currency,'unknown'),mode,count(*),
 count(*) FILTER(WHERE NOT(COALESCE(managed AND provider_outcome IN ('collected','not_collected') AND provider_observed_at IS NOT NULL AND (provider_outcome<>'collected' OR collected_known) AND (refund_state='not_required' OR (refund_known AND collected_known)),false))),
 count(*) FILTER(WHERE refund_known),
 COALESCE(sum(price) FILTER(WHERE collected_known),0),
 COALESCE(sum(refund_amount_cents) FILTER(WHERE refund_known),0),clock_timestamp()
 FROM facts GROUP BY currency,mode ORDER BY currency,mode`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := &domain.MoneyMetrics{Groups: []domain.MoneyMetric{}, ObservedAt: time.Now().UTC()}
	var total int64
	for rows.Next() {
		var v domain.MoneyMetric
		if err = rows.Scan(&v.Currency, &v.Mode, &v.Payments, &v.UnknownPayments, &v.ConfirmedRefunds, &v.CollectedKnownMinor, &v.RefundedKnownMinor, &result.ObservedAt); err != nil {
			return nil, err
		}
		total += v.Payments
		result.Groups = append(result.Groups, v)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if total > 10000 {
		return nil, domain.ErrMetricsBounds
	}
	return result, nil
}
