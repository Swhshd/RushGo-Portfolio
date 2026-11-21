package grpcsecurity

import (
	"context"

	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	userReflectionV1Method      = "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"
	userReflectionV1AlphaMethod = "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"
)

// UserServiceMethodAuthorizationInterceptor limits each verified mTLS service
// identity to the User RPC methods it needs. Connection-level mTLS allowlists
// remain enforced by ServerCredentialsFromEnv.
func UserServiceMethodAuthorizationInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info == nil {
			return nil, status.Error(codes.PermissionDenied, "User gRPC method is not authorized for caller identity")
		}
		if err := authorizeUserServiceMethod(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// UserServiceStreamAuthorizationInterceptor applies the same caller-method
// policy to server-streaming methods such as gRPC reflection.
func UserServiceStreamAuthorizationInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info == nil || stream == nil {
			return status.Error(codes.PermissionDenied, "User gRPC stream method is not authorized for caller identity")
		}
		if err := authorizeUserServiceMethod(stream.Context(), info.FullMethod); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

func authorizeUserServiceMethod(ctx context.Context, method string) error {
	caller, ok := verifiedMTLSServiceIdentity(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "verified User gRPC caller identity required")
	}
	if !userServiceMethodAllowed(caller, method) {
		return status.Error(codes.PermissionDenied, "User gRPC method is not authorized for caller identity")
	}
	return nil
}

func verifiedMTLSServiceIdentity(ctx context.Context) (string, bool) {
	remotePeer, ok := peer.FromContext(ctx)
	if !ok {
		return "", false
	}

	var tlsInfo credentials.TLSInfo
	switch authInfo := remotePeer.AuthInfo.(type) {
	case credentials.TLSInfo:
		tlsInfo = authInfo
	case *credentials.TLSInfo:
		if authInfo == nil {
			return "", false
		}
		tlsInfo = *authInfo
	default:
		return "", false
	}
	if len(tlsInfo.State.PeerCertificates) == 0 || len(tlsInfo.State.VerifiedChains) == 0 {
		return "", false
	}

	caller := ""
	for _, identity := range tlsInfo.State.PeerCertificates[0].URIs {
		if identity == nil {
			continue
		}
		candidate := identity.String()
		if !validServiceIdentity(candidate) {
			continue
		}
		if caller != "" && caller != candidate {
			return "", false
		}
		caller = candidate
	}
	return caller, caller != ""
}

func userServiceMethodAllowed(caller, method string) bool {
	if caller == ServiceIdentityBooking {
		return method == userv1.UserService_ValidateToken_FullMethodName
	}
	if caller == ServiceIdentityNotification {
		return method == userv1.UserService_ValidateToken_FullMethodName || method == userv1.UserService_GetNotificationRecipient_FullMethodName
	}
	if caller != ServiceIdentityGateway {
		return false
	}
	// Keep the existing grpcurl-based Gateway diagnostics while preventing
	// Booking credentials from querying reflection metadata.
	switch method {
	case userReflectionV1Method,
		userv1.UserService_ChangePassword_FullMethodName,
		userv1.UserService_RequestEmailChange_FullMethodName,
		userv1.UserService_CompleteEmailChange_FullMethodName,
		userReflectionV1AlphaMethod,
		userv1.UserService_Register_FullMethodName,
		userv1.UserService_RequestRegistrationCode_FullMethodName,
		userv1.UserService_RequestPasswordReset_FullMethodName,
		userv1.UserService_ResetPassword_FullMethodName,
		userv1.UserService_Login_FullMethodName,
		userv1.UserService_RefreshToken_FullMethodName,
		userv1.UserService_Logout_FullMethodName,
		userv1.UserService_GetProfile_FullMethodName,
		userv1.UserService_UpdateProfile_FullMethodName,
		userv1.UserService_ValidateToken_FullMethodName:
		return true
	default:
		return false
	}
}
