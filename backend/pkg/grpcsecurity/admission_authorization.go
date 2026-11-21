package grpcsecurity

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RequireAdmissionOwner is enforced inside every admission handler. Metadata,
// plaintext test modes, and connection-level allowlists cannot substitute for
// the verified peer certificate and exact owner-service method authority.
func RequireAdmissionOwner(ctx context.Context, expected string) error {
	caller, ok := verifiedMTLSServiceIdentity(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "verified admission caller required")
	}
	if (expected != ServiceIdentityBooking && expected != ServiceIdentityPayment && expected != ServiceIdentitySaga) || caller != expected {
		return status.Error(codes.PermissionDenied, "admission caller is not authorized")
	}
	return nil
}
