package service

import (
	"context"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/ticketbox/pkg/reliabilityhook"
	"github.com/ticketbox/saga/internal/domain"
)

func (s *SagaService) reliabilityStepBoundary(claim domain.SagaClaim, index int, phase string) (reliabilityhook.Boundary, error) {
	if claim.Saga == nil || claim.Saga.BookingID == uuid.Nil || claim.Saga.ID == uuid.Nil ||
		claim.Token == uuid.Nil || claim.Fence < 1 || index < 0 || index >= len(claim.Saga.Steps) {
		return reliabilityhook.Boundary{}, fmt.Errorf("invalid Saga reliability barrier identity")
	}
	if phase != "before_effect" && phase != "effect_before_result" {
		return reliabilityhook.Boundary{}, fmt.Errorf("invalid Saga reliability barrier phase")
	}
	direction := claim.Direction
	if direction == "compensate" {
		direction = "compensation"
	}
	if (direction != "forward" && direction != "compensation") || (direction == "compensation" && index > 3) {
		return reliabilityhook.Boundary{}, fmt.Errorf("invalid Saga reliability barrier direction")
	}
	step := claim.Saga.Steps[index]
	if step.ID == uuid.Nil || int(step.Order) != index || step.Name == "" {
		return reliabilityhook.Boundary{}, fmt.Errorf("invalid Saga reliability barrier step")
	}
	stepStatus := domain.SAGA_STEP_EXECUTING
	if direction == "compensation" {
		stepStatus = domain.SAGA_STEP_COMPENSATING
	}
	windowID := fmt.Sprintf("saga.%s.S%d.%s", direction, index, phase)
	return reliabilityhook.Boundary{
		WindowID: windowID, OperationID: claim.Saga.BookingID.String(),
		Checkpoint: windowID, Action: reliabilityhook.ActionPause,
		Facts: map[string]string{
			"booking_id":  claim.Saga.BookingID.String(),
			"saga_id":     claim.Saga.ID.String(),
			"step_id":     step.ID.String(),
			"step_name":   step.Name,
			"direction":   direction,
			"phase":       phase,
			"claim_fence": strconv.FormatInt(claim.Fence, 10),
			"step_status": string(stepStatus),
		},
	}, nil
}

func (s *SagaService) awaitReliabilityStepBoundary(ctx context.Context, claim domain.SagaClaim, index int, phase string) error {
	if s == nil || s.reliabilityHook == nil || !s.reliabilityHook.Enabled() {
		return nil
	}
	boundary, err := s.reliabilityStepBoundary(claim, index, phase)
	if err != nil {
		return err
	}
	_, _, err = s.reliabilityHook.Await(ctx, boundary)
	return err
}
