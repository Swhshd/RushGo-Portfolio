package grpcsecurity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"

	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestUserServiceMethodAuthorization(t *testing.T) {
	gatewayMethods := []string{userv1.UserService_ChangePassword_FullMethodName, userv1.UserService_RequestEmailChange_FullMethodName, userv1.UserService_CompleteEmailChange_FullMethodName,
		userv1.UserService_Register_FullMethodName,
		userv1.UserService_RequestRegistrationCode_FullMethodName,
		userv1.UserService_RequestPasswordReset_FullMethodName,
		userv1.UserService_ResetPassword_FullMethodName,
		userv1.UserService_Login_FullMethodName,
		userv1.UserService_RefreshToken_FullMethodName,
		userv1.UserService_Logout_FullMethodName,
		userv1.UserService_GetProfile_FullMethodName,
		userv1.UserService_UpdateProfile_FullMethodName,
		userv1.UserService_ValidateToken_FullMethodName,
	}
	for _, method := range gatewayMethods {
		t.Run("gateway/"+method, func(t *testing.T) {
			assertUserMethodAuthorization(t, ServiceIdentityGateway, method, codes.OK, true)
		})
	}

	t.Run("booking/validate-token", func(t *testing.T) {
		assertUserMethodAuthorization(t, ServiceIdentityBooking, userv1.UserService_ValidateToken_FullMethodName, codes.OK, true)
	})
	t.Run("notification/validate-token", func(t *testing.T) {
		assertUserMethodAuthorization(t, ServiceIdentityNotification, userv1.UserService_ValidateToken_FullMethodName, codes.OK, true)
	})
	t.Run("notification/get-profile-denied", func(t *testing.T) {
		assertUserMethodAuthorization(t, ServiceIdentityNotification, userv1.UserService_GetProfile_FullMethodName, codes.PermissionDenied, false)
	})
	t.Run("notification/update-profile-denied", func(t *testing.T) {
		assertUserMethodAuthorization(t, ServiceIdentityNotification, userv1.UserService_UpdateProfile_FullMethodName, codes.PermissionDenied, false)
	})
	t.Run("booking/get-profile-denied", func(t *testing.T) {
		assertUserMethodAuthorization(t, ServiceIdentityBooking, userv1.UserService_GetProfile_FullMethodName, codes.PermissionDenied, false)
	})
	t.Run("booking/update-profile-denied", func(t *testing.T) {
		assertUserMethodAuthorization(t, ServiceIdentityBooking, userv1.UserService_UpdateProfile_FullMethodName, codes.PermissionDenied, false)
	})
	t.Run("booking/unknown-method-denied", func(t *testing.T) {
		assertUserMethodAuthorization(t, ServiceIdentityBooking, "/user.v1.UserService/Unknown", codes.PermissionDenied, false)
	})
	t.Run("untrusted-service-denied", func(t *testing.T) {
		assertUserMethodAuthorization(t, ServiceIdentitySaga, userv1.UserService_ValidateToken_FullMethodName, codes.PermissionDenied, false)
	})
	t.Run("missing-tls-peer-fails-closed", func(t *testing.T) {
		assertUserMethodAuthorization(t, "", userv1.UserService_ValidateToken_FullMethodName, codes.Unauthenticated, false)
	})
	t.Run("unverified-tls-peer-fails-closed", func(t *testing.T) {
		ctx := userIdentityContext(ServiceIdentityGateway, false)
		assertUserAuthorizationResult(t, ctx, userv1.UserService_ValidateToken_FullMethodName, codes.Unauthenticated, false)
	})
}

func TestUserServiceStreamMethodAuthorization(t *testing.T) {
	for _, method := range []string{userReflectionV1Method, userReflectionV1AlphaMethod} {
		t.Run("gateway/"+method, func(t *testing.T) {
			assertUserStreamMethodAuthorization(t, ServiceIdentityGateway, method, codes.OK, true)
		})
		t.Run("booking-denied/"+method, func(t *testing.T) {
			assertUserStreamMethodAuthorization(t, ServiceIdentityBooking, method, codes.PermissionDenied, false)
		})
	}
	t.Run("missing-tls-peer-fails-closed", func(t *testing.T) {
		assertUserStreamMethodAuthorization(t, "", userReflectionV1Method, codes.Unauthenticated, false)
	})
}

func assertUserMethodAuthorization(t *testing.T, identity, method string, wantCode codes.Code, wantHandler bool) {
	t.Helper()
	ctx := context.Background()
	if identity != "" {
		ctx = userIdentityContext(identity, true)
	}
	assertUserAuthorizationResult(t, ctx, method, wantCode, wantHandler)
}

func assertUserAuthorizationResult(t *testing.T, ctx context.Context, method string, wantCode codes.Code, wantHandler bool) {
	t.Helper()
	called := false
	interceptor := UserServiceMethodAuthorizationInterceptor()
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
		called = true
		return nil, nil
	})
	if got := status.Code(err); got != wantCode {
		t.Fatalf("interceptor status = %s, want %s (err=%v)", got, wantCode, err)
	}
	if called != wantHandler {
		t.Fatalf("handler called = %t, want %t", called, wantHandler)
	}
}

func assertUserStreamMethodAuthorization(t *testing.T, identity, method string, wantCode codes.Code, wantHandler bool) {
	t.Helper()
	ctx := context.Background()
	if identity != "" {
		ctx = userIdentityContext(identity, true)
	}
	called := false
	interceptor := UserServiceStreamAuthorizationInterceptor()
	err := interceptor(nil, &userAuthorizationServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: method}, func(any, grpc.ServerStream) error {
		called = true
		return nil
	})
	if got := status.Code(err); got != wantCode {
		t.Fatalf("stream interceptor status = %s, want %s (err=%v)", got, wantCode, err)
	}
	if called != wantHandler {
		t.Fatalf("stream handler called = %t, want %t", called, wantHandler)
	}
}

type userAuthorizationServerStream struct {
	ctx context.Context
}

func (s *userAuthorizationServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *userAuthorizationServerStream) SendHeader(metadata.MD) error { return nil }
func (s *userAuthorizationServerStream) SetTrailer(metadata.MD)       {}
func (s *userAuthorizationServerStream) Context() context.Context     { return s.ctx }
func (s *userAuthorizationServerStream) SendMsg(any) error            { return nil }
func (s *userAuthorizationServerStream) RecvMsg(any) error            { return nil }

func userIdentityContext(identity string, verified bool) context.Context {
	uri, _ := url.Parse(identity)
	cert := &x509.Certificate{URIs: []*url.URL{uri}}
	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	if verified {
		state.VerifiedChains = [][]*x509.Certificate{{cert}}
	}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
}

func TestNotificationRecipientMethodAuthorization(t *testing.T) {
	for _, identity := range []string{ServiceIdentityGateway, ServiceIdentityBooking, ServiceIdentityNotification, ServiceIdentitySaga} {
		code := codes.PermissionDenied
		if identity == ServiceIdentityNotification {
			code = codes.OK
		}
		assertUserMethodAuthorization(t, identity, userv1.UserService_GetNotificationRecipient_FullMethodName, code, code == codes.OK)
	}
}
