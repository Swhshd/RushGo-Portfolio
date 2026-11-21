package grpc

import (
	"context"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/grpcsecurity"
	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
)

type AdmissionServer struct {
	admissionv1.UnimplementedPaymentAdmissionCoordinatorServer
	coordinator admission.Coordinator
}

func NewAdmissionServer(c admission.Coordinator) *AdmissionServer {
	return &AdmissionServer{coordinator: c}
}
func (s *AdmissionServer) PrepareAdmission(ctx context.Context, req *admissionv1.Identity) (*admissionv1.Permit, error) {
	if err := grpcsecurity.RequireAdmissionOwner(ctx, grpcsecurity.ServiceIdentityBooking); err != nil {
		return nil, err
	}
	i, err := admission.IdentityFromProto(req)
	if err != nil {
		return nil, admission.RPCError(err)
	}
	if s.coordinator == nil {
		return nil, admission.RPCError(admission.ErrUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, admission.RPCDeadline)
	defer cancel()
	p, err := s.coordinator.PrepareAdmission(ctx, i)
	if err != nil {
		return nil, admission.RPCError(err)
	}
	return admission.PermitToProto(p), nil
}
func (s *AdmissionServer) GetAdmission(ctx context.Context, req *admissionv1.Identity) (*admissionv1.Permit, error) {
	if err := grpcsecurity.RequireAdmissionOwner(ctx, grpcsecurity.ServiceIdentityBooking); err != nil {
		return nil, err
	}
	i, err := admission.IdentityFromProto(req)
	if err != nil {
		return nil, admission.RPCError(err)
	}
	if s.coordinator == nil {
		return nil, admission.RPCError(admission.ErrUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, admission.RPCDeadline)
	defer cancel()
	p, err := s.coordinator.GetAdmission(ctx, i)
	if err != nil {
		return nil, admission.RPCError(err)
	}
	return admission.PermitToProto(p), nil
}
func (s *AdmissionServer) ReconcileAdmission(ctx context.Context, req *admissionv1.ResolutionRequest) (*admissionv1.Permit, error) {
	if err := grpcsecurity.RequireAdmissionOwner(ctx, grpcsecurity.ServiceIdentityBooking); err != nil {
		return nil, err
	}
	i, token, fence, err := admission.ResolutionFromProto(req, false)
	if err != nil {
		return nil, admission.RPCError(err)
	}
	if req.AbortPending {
		return nil, admission.RPCError(admission.ErrInvalid)
	}
	if s.coordinator == nil {
		return nil, admission.RPCError(admission.ErrUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, admission.RPCDeadline)
	defer cancel()
	p, err := s.coordinator.ReconcileAdmission(ctx, i, token, fence)
	if err != nil {
		return nil, admission.RPCError(err)
	}
	return admission.PermitToProto(p), nil
}
