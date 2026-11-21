package repository

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
)

type OperationsRepository interface {
	ListOperationsBookings(context.Context, domain.OperationsQuery) ([]domain.OperationsBooking, bool, error)
	ListStaffWork(context.Context, uuid.UUID, bool, int, int) ([]domain.StaffWork, bool, error)
}

func (r *PostgresBookingRepository) ListOperationsBookings(ctx context.Context, q domain.OperationsQuery) ([]domain.OperationsBooking, bool, error) {
	if q.Limit < 1 || q.Limit > 100 || q.Offset < 0 || q.Offset > 10000 {
		return nil, false, errors.New("invalid operations bounds")
	}
	rows, err := r.pool.Query(ctx, `WITH page AS (SELECT id,event_id,status,created_at,COALESCE(lifecycle_generation,0) generation FROM bookings WHERE created_at>=$1 AND created_at<$2 ORDER BY created_at DESC,id DESC LIMIT $3 OFFSET $4)
 SELECT b.id,b.event_id,b.status,b.created_at,b.generation,
 (SELECT count(*) FROM tickets t WHERE t.booking_id=b.id),COALESCE((SELECT sum(quantity) FROM booking_items i WHERE i.booking_id=b.id),0),
 EXISTS(SELECT 1 FROM ticket_issuance_receipts t WHERE t.booking_id=b.id AND t.outcome='denied'),
 COALESCE(r.status,''),COALESCE(r.ticket_revocation_state,''),r.decided_at,
 EXISTS(SELECT 1 FROM outbox o WHERE NOT published AND o.event_key=b.id::text AND o.created_at<clock_timestamp()-interval '5 minutes'),
 EXISTS(SELECT 1 FROM ticket_admission_intents a WHERE a.booking_id=b.id AND a.state='PENDING' AND a.created_at<clock_timestamp()-interval '5 minutes')
 FROM page b LEFT JOIN LATERAL(SELECT status,ticket_revocation_state,decided_at FROM booking_refund_requests WHERE booking_id=b.id ORDER BY requested_at DESC,request_id DESC LIMIT 1) r ON true
 ORDER BY b.created_at DESC,b.id DESC`, q.From, q.To, q.Limit+1, q.Offset)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	items := []domain.OperationsBooking{}
	for rows.Next() {
		var v domain.OperationsBooking
		if err = rows.Scan(&v.BookingID, &v.EventID, &v.Status, &v.CreatedAt, &v.Generation, &v.TicketCount, &v.ExpectedTickets, &v.IssuanceDenied, &v.RefundStatus, &v.RevocationState, &v.RefundDecidedAt, &v.OutboxStale, &v.AdmissionStale); err != nil {
			return nil, false, err
		}
		items = append(items, v)
	}
	if err = rows.Err(); err != nil {
		return nil, false, err
	}
	more := len(items) > q.Limit
	if more {
		items = items[:q.Limit]
	}
	return items, more, nil
}
func (r *PostgresBookingRepository) ListStaffWork(ctx context.Context, user uuid.UUID, admin bool, limit, offset int) ([]domain.StaffWork, bool, error) {
	if user == uuid.Nil || limit < 1 || limit > 100 || offset < 0 || offset > 10000 {
		return nil, false, errors.New("invalid worklist bounds")
	}
	rows, err := r.pool.Query(ctx, `WITH access AS (
 SELECT event_id,'scanner' kind FROM event_scanner_assignments WHERE staff_user_id=$1 AND active
 UNION SELECT a.event_id,'approver' kind FROM event_refund_approvers a WHERE a.staff_user_id=$1 AND a.active
 UNION SELECT DISTINCT r.event_id,'admin' kind FROM booking_refund_requests r WHERE $2 AND NOT EXISTS(SELECT 1 FROM event_refund_approvers p WHERE p.event_id=r.event_id AND p.staff_user_id=$1 AND p.active)
 ) SELECT a.event_id,a.kind,CASE WHEN a.kind='scanner' THEN 0 ELSE (SELECT count(*) FROM booking_refund_requests r WHERE r.event_id=a.event_id AND r.status IN ('REQUESTED','WAITING_CHECKIN_RESOLUTION') AND r.buyer_user_id<>$1) END
 FROM access a ORDER BY a.event_id,a.kind LIMIT $3 OFFSET $4`, user, admin, limit+1, offset)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	items := []domain.StaffWork{}
	for rows.Next() {
		var v domain.StaffWork
		if err = rows.Scan(&v.EventID, &v.Access, &v.PendingRefunds); err != nil {
			return nil, false, err
		}
		items = append(items, v)
	}
	if err = rows.Err(); err != nil {
		return nil, false, err
	}
	more := len(items) > limit
	if more {
		items = items[:limit]
	}
	return items, more, nil
}
