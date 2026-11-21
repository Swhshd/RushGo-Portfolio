package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"

	"github.com/ticketbox/event/internal/admission"
	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/event/internal/repository"
	"github.com/ticketbox/pkg/observability"
)

func TestReservationAdmissionConflictStopsBeforePostgres(t *testing.T) {
	repo := &seatRepositoryStub{}
	admissionStub := &admissionStub{acquireErr: admission.ErrConflict}
	svc := NewEventServiceWithAdmission(nil, nil, repo, zap.NewNop(), admissionStub, admission.ModeRedisAdmission)
	eventID := uuid.New()
	seatID := uuid.New()
	owner := uuid.New()

	outcome, err := svc.ReservedOrCompensateBatchSeats(context.Background(), &ReservedOrCompensateBatchSeatsReq{
		EventID:               eventID,
		SeatIds:               []uuid.UUID{seatID},
		ReservedByBookingId:   owner,
		Action:                domain.RESERVED_SEAT,
		ReservedTimeInMinutes: 10,
	})
	if !errors.Is(err, repository.ErrSeatNotAvailable) {
		t.Fatalf("error = %v, want ErrSeatNotAvailable", err)
	}
	if repo.reservationCalls != 0 {
		t.Fatalf("repository calls = %d, want 0", repo.reservationCalls)
	}
	if outcome.RedisStatus != serviceRedisStatusConflict {
		t.Fatalf("redis status = %q, want conflict", outcome.RedisStatus)
	}
}

func TestReservationAdmissionInfrastructureErrorFallsBackToPostgres(t *testing.T) {
	repo := &seatRepositoryStub{}
	admissionStub := &admissionStub{acquireErr: errors.New("redis unavailable")}
	svc := NewEventServiceWithAdmission(nil, nil, repo, zap.NewNop(), admissionStub, admission.ModeRedisAdmission)

	outcome, err := svc.ReservedOrCompensateBatchSeats(context.Background(), &ReservedOrCompensateBatchSeatsReq{
		EventID:               uuid.New(),
		SeatIds:               []uuid.UUID{uuid.New()},
		ReservedByBookingId:   uuid.New(),
		Action:                domain.RESERVED_SEAT,
		ReservedTimeInMinutes: 10,
	})
	if err != nil || !outcome.Success {
		t.Fatalf("fallback result = %#v, %v; want success", outcome, err)
	}
	if repo.reservationCalls != 1 {
		t.Fatalf("repository calls = %d, want 1", repo.reservationCalls)
	}
	if !outcome.RedisFallback || outcome.RedisStatus != serviceRedisStatusInfrastructureError {
		t.Fatalf("fallback outcome = %#v", outcome)
	}
}

func TestReservationAdmissionMetricsRecordOneRedisOutcomeAndOnePostgresAttempt(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics, err := observability.NewMetrics(provider)
	if err != nil {
		t.Fatalf("NewMetrics() error = %v", err)
	}
	repo := &seatRepositoryStub{}
	svc := NewEventServiceWithAdmissionAndRealtimePublisherAndMetrics(
		nil, nil, repo, zap.NewNop(), &admissionStub{}, admission.ModeRedisAdmission, nil, metrics,
	)
	_, err = svc.ReservedOrCompensateBatchSeats(context.Background(), &ReservedOrCompensateBatchSeatsReq{
		EventID: uuid.New(), SeatIds: []uuid.UUID{uuid.New()}, ReservedByBookingId: uuid.New(), Action: domain.RESERVED_SEAT,
	})
	if err != nil {
		t.Fatalf("reservation = %v", err)
	}

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if got := metricSumByName(collected, "rushgo.seat_admission.attempts"); got != 1 {
		t.Fatalf("seat admission attempts = %d, want 1", got)
	}
	if got := metricSumByName(collected, "rushgo.seat_postgres_reservation.attempts"); got != 1 {
		t.Fatalf("postgres attempts = %d, want 1", got)
	}
}

func metricSumByName(metrics metricdata.ResourceMetrics, name string) int64 {
	var total int64
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			if sum, ok := metric.Data.(metricdata.Sum[int64]); ok {
				for _, point := range sum.DataPoints {
					total += point.Value
				}
			}
		}
	}
	return total
}

func TestReservationOutcomeRecordsRedisAdmissionLatency(t *testing.T) {
	repo := &seatRepositoryStub{}
	admissionStub := &admissionStub{acquireDuration: 2 * time.Millisecond}
	svc := NewEventServiceWithAdmission(nil, nil, repo, zap.NewNop(), admissionStub, admission.ModeRedisAdmission)

	outcome, err := svc.ReservedOrCompensateBatchSeats(context.Background(), &ReservedOrCompensateBatchSeatsReq{
		EventID:               uuid.New(),
		SeatIds:               []uuid.UUID{uuid.New()},
		ReservedByBookingId:   uuid.New(),
		Action:                domain.RESERVED_SEAT,
		ReservedTimeInMinutes: 1,
	})
	if err != nil {
		t.Fatalf("reservation: %v", err)
	}
	if outcome.RedisLatencyNs <= 0 {
		t.Fatalf("Redis latency = %d ns, want positive measurement", outcome.RedisLatencyNs)
	}
}

func TestReservationDatabaseErrorCleansRedisOwnerAfterAdmission(t *testing.T) {
	repo := &seatRepositoryStub{reservationErr: errors.New("database conflict")}
	admissionStub := &admissionStub{}
	svc := NewEventServiceWithAdmission(nil, nil, repo, zap.NewNop(), admissionStub, admission.ModeRedisAdmission)
	eventID := uuid.New()
	seatID := uuid.New()
	owner := uuid.New()

	_, err := svc.ReservedOrCompensateBatchSeats(context.Background(), &ReservedOrCompensateBatchSeatsReq{
		EventID:               eventID,
		SeatIds:               []uuid.UUID{seatID},
		ReservedByBookingId:   owner,
		Action:                domain.RESERVED_SEAT,
		ReservedTimeInMinutes: 10,
	})
	if err == nil {
		t.Fatal("expected database error")
	}
	if admissionStub.releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", admissionStub.releaseCalls)
	}
}

const (
	serviceRedisStatusConflict            = RedisStatusConflict
	serviceRedisStatusInfrastructureError = RedisStatusInfrastructureErr
)

func TestCompensationMutatesDatabaseBeforeRedisCleanup(t *testing.T) {
	order := make([]string, 0, 2)
	repo := &seatRepositoryStub{onReservation: func() { order = append(order, "db") }}
	admissionStub := &admissionStub{onRelease: func() { order = append(order, "redis") }}
	svc := NewEventServiceWithAdmission(nil, nil, repo, zap.NewNop(), admissionStub, admission.ModeRedisAdmission)

	if _, err := svc.ReservedOrCompensateBatchSeats(context.Background(), &ReservedOrCompensateBatchSeatsReq{
		EventID:             uuid.New(),
		SeatIds:             []uuid.UUID{uuid.New()},
		ReservedByBookingId: uuid.New(),
		Action:              domain.COMPENSATE_SEAT,
	}); err != nil {
		t.Fatalf("compensation: %v", err)
	}
	if len(order) < 2 || order[0] != "db" || order[1] != "redis" {
		t.Fatalf("operation order = %#v, want database mutation before Redis admission cleanup", order)
	}
}

func TestNoOpBatchStatusDoesNotCleanUpAdmission(t *testing.T) {
	eventID := uuid.New()
	seatID := uuid.New()
	owner := uuid.New()
	repo := &seatRepositoryStub{}
	admissionStub := &admissionStub{}
	svc := NewEventServiceWithAdmission(nil, nil, repo, zap.NewNop(), admissionStub, admission.ModeRedisAdmission)

	if err := svc.UpdateBatchSeatStatus(context.Background(), eventID, []uuid.UUID{seatID}, domain.SeatStatusAvailable, &owner); err != nil {
		t.Fatalf("no-op batch status: %v", err)
	}
	if admissionStub.releaseCalls != 0 {
		t.Fatalf("release calls = %d, want 0 for an empty changed-seat result", admissionStub.releaseCalls)
	}
}

func TestExpiredDatabaseCleanupRemovesMatchingRedisAdmission(t *testing.T) {
	eventID := uuid.New()
	bookingID := uuid.New()
	seatID := uuid.New()
	repo := &seatRepositoryStub{expiredResult: &repository.ReservedExpiredSeatsResult{
		BookingIdSeatIdsMap: map[uuid.UUID][]uuid.UUID{bookingID: {seatID}},
		EventBookingSeatIDs: map[uuid.UUID]map[uuid.UUID][]uuid.UUID{
			eventID: {bookingID: {seatID}},
		},
	}}
	admissionStub := &admissionStub{}
	svc := NewEventServiceWithAdmission(nil, nil, repo, zap.NewNop(), admissionStub, admission.ModeRedisAdmission)

	if _, err := svc.UndoReservedExpiredSeats(context.Background()); err != nil {
		t.Fatalf("expired cleanup: %v", err)
	}
	if admissionStub.releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", admissionStub.releaseCalls)
	}
}

type admissionStub struct {
	acquireErr      error
	releaseErr      error
	releaseCalls    int
	onRelease       func()
	acquireDuration time.Duration
}

func (a *admissionStub) Acquire(context.Context, uuid.UUID, []uuid.UUID, uuid.UUID, int32) (admission.AcquireResult, error) {
	if a.acquireDuration > 0 {
		time.Sleep(a.acquireDuration)
	}
	if a.acquireErr != nil {
		return admission.AcquireResult{}, a.acquireErr
	}
	return admission.AcquireResult{Created: 1}, nil
}

func (a *admissionStub) Release(context.Context, uuid.UUID, []uuid.UUID, uuid.UUID) (admission.ReleaseResult, error) {
	a.releaseCalls++
	if a.onRelease != nil {
		a.onRelease()
	}
	return admission.ReleaseResult{Deleted: 1}, a.releaseErr
}

type seatRepositoryStub struct {
	reservationCalls  int
	reservationErr    error
	onReservation     func()
	reservationResult repository.SeatMutationResult
	expiredResult     *repository.ReservedExpiredSeatsResult
	lifecycleCalls    int
	lifecycleResult   *domain.SeatLifecycleResult
}

func (s *seatRepositoryStub) ApplyLifecycle(context.Context, domain.SeatLifecycleRequest) (*domain.SeatLifecycleResult, error) {
	s.lifecycleCalls++
	if s.reservationErr != nil {
		return nil, s.reservationErr
	}
	if s.lifecycleResult == nil {
		return &domain.SeatLifecycleResult{State: "held"}, nil
	}
	return s.lifecycleResult, nil
}
func (s *seatRepositoryStub) GetSeatAuthorities(context.Context, []uuid.UUID) ([]domain.SeatAuthority, error) {
	return nil, nil
}

func (s *seatRepositoryStub) ApplySaleAccounting(context.Context, domain.SeatLifecycleRequest) (*domain.SeatLifecycleResult, error) {
	return nil, repository.ErrLifecycleConflict
}

func (s *seatRepositoryStub) Create(context.Context, *domain.Seat) error        { return nil }
func (s *seatRepositoryStub) CreateBatch(context.Context, []*domain.Seat) error { return nil }
func (s *seatRepositoryStub) GetByID(context.Context, uuid.UUID) (*domain.Seat, error) {
	return nil, nil
}
func (s *seatRepositoryStub) GetByEventID(context.Context, uuid.UUID, *uuid.UUID) ([]*domain.Seat, error) {
	return nil, nil
}
func (s *seatRepositoryStub) GetByTierID(context.Context, uuid.UUID) ([]*domain.Seat, error) {
	return nil, nil
}
func (s *seatRepositoryStub) UpdateStatus(context.Context, uuid.UUID, domain.SeatStatus, *uuid.UUID) (repository.SeatMutationResult, error) {
	return repository.SeatMutationResult{Success: true}, nil
}
func (s *seatRepositoryStub) UpdateStatusBatch(context.Context, []uuid.UUID, domain.SeatStatus, *uuid.UUID) (repository.SeatMutationResult, error) {
	return repository.SeatMutationResult{Success: true}, nil
}
func (s *seatRepositoryStub) GetAvailableSeats(context.Context, uuid.UUID) ([]*domain.Seat, error) {
	return nil, nil
}
func (s *seatRepositoryStub) GetAvailableSeatsByTier(context.Context, uuid.UUID) ([]*domain.Seat, error) {
	return nil, nil
}
func (s *seatRepositoryStub) GetSeatsBySeatIds(context.Context, []uuid.UUID) ([]domain.Seat, error) {
	return nil, nil
}
func (s *seatRepositoryStub) ReservedOrCompensateBatchSeats(context.Context, *repository.ReservedOrCompensateBatchSeats) (repository.SeatMutationResult, error) {
	s.reservationCalls++
	if s.onReservation != nil {
		s.onReservation()
	}
	if s.reservationErr != nil {
		return repository.SeatMutationResult{}, s.reservationErr
	}
	if !s.reservationResult.Success {
		s.reservationResult.Success = true
	}
	return s.reservationResult, nil
}
func (s *seatRepositoryStub) UndoReservedExpiredSeats(context.Context) (*repository.ReservedExpiredSeatsResult, error) {
	return s.expiredResult, nil
}
func (s *seatRepositoryStub) GetReservedExpiredSeats(context.Context) (*repository.ReservedExpiredSeatsResult, error) {
	return nil, nil
}
