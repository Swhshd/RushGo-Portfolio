package grpc

import (
	"context"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/grpcsecurity"
	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
)

type AdmissionServer struct {
	admissionv1.UnimplementedBookingAdmissionAuthorityServer
	resolver admission.OutcomeResolver
}

func NewAdmissionServer(r admission.OutcomeResolver) *AdmissionServer {
	return &AdmissionServer{resolver: r}
}
func (s *AdmissionServer) ResolveAdmission(ctx context.Context, req *admissionv1.ResolutionRequest) (*admissionv1.Outcome, error) {
	if err := grpcsecurity.RequireAdmissionOwner(ctx, grpcsecurity.ServiceIdentityPayment); err != nil {
		return nil, err
	}
	i, token, fence, err := admission.ResolutionFromProto(req, true)
	if err != nil {
		return nil, admission.RPCError(err)
	}
	if s.resolver == nil {
		return nil, admission.RPCError(admission.ErrUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, admission.RPCDeadline)
	defer cancel()
	outcome, err := s.resolver.ResolveAdmission(ctx, i, token, fence, req.AbortPending)
	if err != nil {
		return nil, admission.RPCError(err)
	}
	return admission.OutcomeToProto(outcome), nil
}
