package grpcsecurity

import (
	"context"
	"fmt"
	"os"
	"strings"

	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// TicketAuthorityServerCredentialsFromEnv always returns mutual-TLS
// credentials for the dedicated Event/Saga ticket-authority listener. It
// deliberately bypasses the build-tagged TEST_ONLY plaintext exception.
func TicketAuthorityServerCredentialsFromEnv(service string) (credentials.TransportCredentials, error) {
	if service != ServiceEvent && service != ServiceSaga {
		return nil, fmt.Errorf("unsupported Ticket authority gRPC server %q", service)
	}
	certPEM, err := readRequiredEnvFile("RUSHGO_GRPC_SERVER_CERT_FILE")
	if err != nil {
		return nil, err
	}
	keyPEM, err := readRequiredEnvFile("RUSHGO_GRPC_SERVER_KEY_FILE")
	if err != nil {
		return nil, err
	}
	caPEM, err := readRequiredEnvFile("RUSHGO_GRPC_CLIENT_CA_FILE")
	if err != nil {
		return nil, err
	}
	callers := []string{ServiceIdentityBooking}
	if service == ServiceEvent {
		// Saga recovery may read seat eligibility; the method interceptor still
		// denies it all refund permit mutations.
		callers = append(callers, ServiceIdentitySaga)
	}
	return NewServerCredentials(certPEM, keyPEM, caPEM, callers)
}

// TicketAuthorityClientCredentialsFromEnv only issues Booking mTLS
// credentials for the dedicated Event/Saga eligibility endpoints. It never
// consults TEST_ONLY plaintext settings.
func TicketAuthorityClientCredentialsFromEnv(target string) (credentials.TransportCredentials, error) {
	return ticketAuthorityClientCredentialsFromEnv(target, ServiceIdentityBooking)
}

// EventRecoveryClientCredentialsFromEnv uses the Saga identity, never Booking
// impersonation or a plaintext fallback, for the recovery eligibility read.
func EventRecoveryClientCredentialsFromEnv() (credentials.TransportCredentials, error) {
	return ticketAuthorityClientCredentialsFromEnv(ServiceEvent, ServiceIdentitySaga)
}

func ticketAuthorityClientCredentialsFromEnv(target, identity string) (credentials.TransportCredentials, error) {
	if target != ServiceEvent && target != ServiceSaga {
		return nil, fmt.Errorf("unsupported Ticket authority gRPC target %q", target)
	}
	certPEM, err := readRequiredEnvFile("RUSHGO_GRPC_CLIENT_CERT_FILE")
	if err != nil {
		return nil, err
	}
	keyPEM, err := readRequiredEnvFile("RUSHGO_GRPC_CLIENT_KEY_FILE")
	if err != nil {
		return nil, err
	}
	caPEM, err := readRequiredEnvFile("RUSHGO_GRPC_SERVER_CA_FILE")
	if err != nil {
		return nil, err
	}
	serverNameEnv := "RUSHGO_" + strings.ToUpper(target) + "_GRPC_SERVER_NAME"
	serverName := strings.TrimSpace(os.Getenv(serverNameEnv))
	if serverName == "" {
		return nil, fmt.Errorf("required gRPC TLS setting %s is empty", serverNameEnv)
	}
	return NewClientCredentials(certPEM, keyPEM, caPEM, serverName, identity)
}

// TicketAuthorityMethodAuthorizationInterceptor binds a dedicated authority
// listener to one exact RPC and one verified Booking certificate identity.
func TicketAuthorityMethodAuthorizationInterceptor(allowedMethod string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info == nil {
			return nil, status.Error(codes.Unauthenticated, "verified Ticket authority caller identity required")
		}
		caller, ok := verifiedMTLSServiceIdentity(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "verified Ticket authority caller identity required")
		}
		if caller != ServiceIdentityBooking || allowedMethod == "" || info.FullMethod != allowedMethod {
			return nil, status.Error(codes.PermissionDenied, "Ticket authority method is not authorized for caller identity")
		}
		return handler(ctx, req)
	}
}

// RejectTicketAuthorityMethodsOnLegacyListener closes the former plaintext
// path while leaving other Event and Saga RPCs available to existing clients.
func RejectTicketAuthorityMethodsOnLegacyListener() grpc.UnaryServerInterceptor {
	blocked := map[string]struct{}{
		eventv1.EventService_VerifyBookingTicketEligibility_FullMethodName:     {},
		eventv1.EventService_AcquireUnusedRefundTimePermit_FullMethodName:      {},
		eventv1.EventService_ReleaseUnusedRefundTimePermit_FullMethodName:      {},
		sagav1.SagaOrchestratorService_VerifyTicketIssuanceSaga_FullMethodName: {},
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info != nil {
			if _, ok := blocked[info.FullMethod]; ok {
				return nil, status.Error(codes.Unauthenticated, "Ticket authority RPC requires its dedicated mTLS listener")
			}
		}
		return handler(ctx, req)
	}
}

// ServiceDescWithOnlyMethod creates a private descriptor exposing exactly one
// unary method. The original generated descriptor remains unchanged.
func ServiceDescWithOnlyMethod(desc grpc.ServiceDesc, fullMethod string) (grpc.ServiceDesc, error) {
	prefix := "/" + desc.ServiceName + "/"
	if !strings.HasPrefix(fullMethod, prefix) || len(fullMethod) == len(prefix) {
		return grpc.ServiceDesc{}, fmt.Errorf("method %q does not belong to gRPC service %q", fullMethod, desc.ServiceName)
	}
	methodName := strings.TrimPrefix(fullMethod, prefix)
	if strings.Contains(methodName, "/") {
		return grpc.ServiceDesc{}, fmt.Errorf("method %q is not a unary method name", fullMethod)
	}
	for _, method := range desc.Methods {
		if method.MethodName == methodName {
			restricted := desc
			restricted.Methods = []grpc.MethodDesc{method}
			restricted.Streams = nil
			return restricted, nil
		}
	}
	return grpc.ServiceDesc{}, fmt.Errorf("method %q is not registered on gRPC service %q", methodName, desc.ServiceName)
}

func RegisterServiceWithOnlyMethod(registrar grpc.ServiceRegistrar, desc grpc.ServiceDesc, implementation any, fullMethod string) error {
	restricted, err := ServiceDescWithOnlyMethod(desc, fullMethod)
	if err != nil {
		return err
	}
	registrar.RegisterService(&restricted, implementation)
	return nil
}

// TicketAuthorityMethodsAuthorizationInterceptor restricts a dedicated listener to
// the explicitly selected methods and verified Booking peer. Saga may only
// read Event seat eligibility; it cannot mutate refund time permits.
func TicketAuthorityMethodsAuthorizationInterceptor(methods ...string) grpc.UnaryServerInterceptor {
	allowed := make(map[string]bool, len(methods))
	for _, method := range methods {
		if method != "" {
			allowed[method] = true
		}
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info == nil {
			return nil, status.Error(codes.Unauthenticated, "verified authority caller required")
		}
		caller, ok := verifiedMTLSServiceIdentity(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "verified authority caller required")
		}
		callerAllowed := caller == ServiceIdentityBooking ||
			(caller == ServiceIdentitySaga && info.FullMethod == eventv1.EventService_VerifyBookingTicketEligibility_FullMethodName)
		if !callerAllowed || !allowed[info.FullMethod] {
			return nil, status.Error(codes.PermissionDenied, "authority method not permitted")
		}
		return handler(ctx, req)
	}
}
func RegisterServiceWithOnlyMethods(registrar grpc.ServiceRegistrar, desc grpc.ServiceDesc, implementation any, methods ...string) error {
	if len(methods) == 0 {
		return fmt.Errorf("at least one authority method required")
	}
	restricted := desc
	restricted.Methods = nil
	restricted.Streams = nil
	seen := map[string]bool{}
	for _, method := range methods {
		if seen[method] {
			return fmt.Errorf("duplicate authority method")
		}
		seen[method] = true
		one, err := ServiceDescWithOnlyMethod(desc, method)
		if err != nil {
			return err
		}
		restricted.Methods = append(restricted.Methods, one.Methods...)
	}
	registrar.RegisterService(&restricted, implementation)
	return nil
}
