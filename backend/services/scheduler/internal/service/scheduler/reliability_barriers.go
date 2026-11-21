package service_scheduler

import (
	"context"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/ticketbox/pkg/reliabilityhook"
	"github.com/ticketbox/scheduler/internal/domain"
)

func (s *SchedulerService) awaitUnitOfWorkBoundary(
	ctx context.Context, updated *domain.SchedulerConfig, expected int, eventID uuid.UUID,
) error {
	if s == nil || s.reliabilityHook == nil || !s.reliabilityHook.Enabled() {
		return nil
	}
	if updated == nil || updated.Id == uuid.Nil || updated.Version != expected+1 || eventID == uuid.Nil {
		return fmt.Errorf("invalid Scheduler unit-of-work barrier identity")
	}
	_, _, err := s.reliabilityHook.Await(ctx, reliabilityhook.Boundary{
		WindowID: "scheduler.uow_second_operation_rollback", OperationID: eventID.String(),
		Checkpoint: "scheduler_uow_first_mutation_before_outbound_insert",
		Action:     reliabilityhook.ActionPause,
		Facts: map[string]string{
			"scheduler_config_id": updated.Id.String(),
			"expected_version":    strconv.Itoa(expected),
			"staged_version":      strconv.Itoa(updated.Version),
			"outbound_event_id":   eventID.String(),
		},
	})
	return err
}
