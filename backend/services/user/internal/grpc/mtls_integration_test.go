package grpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/pkg/grpcsecurity"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"github.com/ticketbox/user/internal/domain"
	"github.com/ticketbox/user/internal/service"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type userTestCA struct {
	certificate *x509.Certificate
	privateKey  *ecdsa.PrivateKey
	pem         []byte
}

func newUserTestCA(t *testing.T) userTestCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "RushGo isolated User mTLS test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return userTestCA{certificate: certificate, privateKey: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca userTestCA) leaf(t *testing.T, dnsName, identity string, usage x509.ExtKeyUsage, notBefore, notAfter time.Time) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: identity},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{usage},
	}
	if dnsName != "" {
		template.DNSNames = []string{dnsName}
	}
	if identity != "" {
		uri, err := url.Parse(identity)
		if err != nil {
			t.Fatal(err)
		}
		template.URIs = []*url.URL{uri}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, &key.PublicKey, ca.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func TestUserValidateTokenOverMutualTLS(t *testing.T) {
	ca := newUserTestCA(t)
	now := time.Now()
	serverCert, serverKey := ca.leaf(t, "user-service", grpcsecurity.ServiceIdentityUser, x509.ExtKeyUsageServerAuth, now.Add(-time.Minute), now.Add(time.Hour))
	serverCred, err := grpcsecurity.NewServerCredentials(serverCert, serverKey, ca.pem, []string{grpcsecurity.ServiceIdentityGateway, grpcsecurity.ServiceIdentityBooking})
	if err != nil {
		t.Fatal(err)
	}
	manager := service.NewJWTManager("isolated-user-jwt-secret", 15)
	userID := uuid.New()
	validToken, err := manager.GenerateAccessToken(userID, "buyer@rushgo.test", "user")
	if err != nil {
		t.Fatal(err)
	}
	serviceImpl := service.NewAuthService(&currentRoleUserRepositoryStub{user: &domain.User{ID: userID, Email: "buyer@rushgo.test", Role: "user"}}, nil, manager, 7, zap.NewNop(), service.AuthDependencies{})
	userServer := NewUserServer(serviceImpl, nil, zap.NewNop())

	for _, caller := range []string{grpcsecurity.ServiceIdentityGateway, grpcsecurity.ServiceIdentityBooking} {
		t.Run(caller, func(t *testing.T) {
			clientCert, clientKey := ca.leaf(t, "", caller, x509.ExtKeyUsageClientAuth, now.Add(-time.Minute), now.Add(time.Hour))
			clientCred, err := grpcsecurity.NewClientCredentials(clientCert, clientKey, ca.pem, "user-service", caller)
			if err != nil {
				t.Fatal(err)
			}
			response, err := invokeUserValidateToken(t, userServer, serverCred, clientCred, validToken, true)
			if err != nil || !response.GetValid() || response.GetUserId() != userID.String() || response.GetRole() != "user" {
				t.Fatalf("mTLS ValidateToken response = (%+v, %v), want signed user identity", response, err)
			}
			response, err = invokeUserValidateToken(t, userServer, serverCred, clientCred, "malformed-token", false)
			if err != nil || response.GetValid() {
				t.Fatalf("invalid JWT response = (%+v, %v), want valid=false", response, err)
			}
			reflectionErr := invokeUserReflectionMethod(t, userServer, serverCred, clientCred)
			wantReflectionCode := codes.OK
			if caller == grpcsecurity.ServiceIdentityBooking {
				wantReflectionCode = codes.PermissionDenied
			}
			if got := status.Code(reflectionErr); got != wantReflectionCode {
				t.Errorf("User reflection status = %s, want %s (err=%v)", got, wantReflectionCode, reflectionErr)
			}
			if caller == grpcsecurity.ServiceIdentityBooking {
				for _, method := range []string{
					userv1.UserService_GetProfile_FullMethodName,
					userv1.UserService_UpdateProfile_FullMethodName,
				} {
					if err := invokeUserProfileMethod(t, userServer, serverCred, clientCred, method); status.Code(err) != codes.PermissionDenied {
						t.Errorf("Booking %s status = %s, want %s (err=%v)", method, status.Code(err), codes.PermissionDenied, err)
					}
				}
			}
		})
	}

	t.Run("unapproved service identity rejected", func(t *testing.T) {
		caller := grpcsecurity.ServiceIdentitySaga
		clientCert, clientKey := ca.leaf(t, "", caller, x509.ExtKeyUsageClientAuth, now.Add(-time.Minute), now.Add(time.Hour))
		clientCred, err := grpcsecurity.NewClientCredentials(clientCert, clientKey, ca.pem, "user-service", caller)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := invokeUserValidateToken(t, userServer, serverCred, clientCred, validToken, false); err == nil {
			t.Fatal("User service accepted a valid-CA caller outside the Gateway/Booking allowlist")
		}
	})

	t.Run("plaintext rejected", func(t *testing.T) {
		if _, err := invokeUserValidateToken(t, userServer, serverCred, insecure.NewCredentials(), validToken, false); err == nil {
			t.Fatal("User ValidateToken succeeded over plaintext")
		}
	})

	t.Run("wrong service name rejected", func(t *testing.T) {
		clientCert, clientKey := ca.leaf(t, "", grpcsecurity.ServiceIdentityGateway, x509.ExtKeyUsageClientAuth, now.Add(-time.Minute), now.Add(time.Hour))
		clientCred, err := grpcsecurity.NewClientCredentials(clientCert, clientKey, ca.pem, "booking-service", grpcsecurity.ServiceIdentityGateway)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := invokeUserValidateToken(t, userServer, serverCred, clientCred, validToken, false); err == nil {
			t.Fatal("client accepted User service under a different DNS identity")
		}
	})
}

func invokeUserValidateToken(t *testing.T, implementation *UserServer, serverCred, clientCred credentials.TransportCredentials, token string, spoofRole bool) (*userv1.ValidateTokenResponse, error) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.Creds(serverCred), grpc.ChainUnaryInterceptor(grpcsecurity.UserServiceMethodAuthorizationInterceptor()))
	userv1.RegisterUserServiceServer(server, implementation)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///user-mtls-test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(clientCred))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if spoofRole {
		ctx = metadata.AppendToOutgoingContext(ctx, "role", "admin", "user_id", uuid.NewString())
	}
	return userv1.NewUserServiceClient(conn).ValidateToken(ctx, &userv1.ValidateTokenRequest{AccessToken: token})
}

func invokeUserProfileMethod(t *testing.T, implementation *UserServer, serverCred, clientCred credentials.TransportCredentials, method string) error {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.Creds(serverCred), grpc.ChainUnaryInterceptor(grpcsecurity.UserServiceMethodAuthorizationInterceptor()))
	userv1.RegisterUserServiceServer(server, implementation)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///user-mtls-test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(clientCred))
	if err != nil {
		return err
	}
	defer conn.Close()
	switch method {
	case userv1.UserService_GetProfile_FullMethodName:
		return conn.Invoke(ctx, method, &userv1.GetProfileRequest{UserId: uuid.NewString()}, &userv1.UserProfile{})
	case userv1.UserService_UpdateProfile_FullMethodName:
		return conn.Invoke(ctx, method, &userv1.UpdateProfileRequest{UserId: uuid.NewString(), Name: "unauthorized"}, &userv1.UserProfile{})
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported User profile method %q", method)
	}
}

func invokeUserReflectionMethod(t *testing.T, implementation *UserServer, serverCred, clientCred credentials.TransportCredentials) error {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(
		grpc.Creds(serverCred),
		grpc.ChainUnaryInterceptor(grpcsecurity.UserServiceMethodAuthorizationInterceptor()),
		grpc.ChainStreamInterceptor(grpcsecurity.UserServiceStreamAuthorizationInterceptor()),
	)
	userv1.RegisterUserServiceServer(server, implementation)
	reflection.Register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///user-mtls-test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(clientCred))
	if err != nil {
		return err
	}
	defer conn.Close()
	stream, err := reflectionv1.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&reflectionv1.ServerReflectionRequest{
		MessageRequest: &reflectionv1.ServerReflectionRequest_ListServices{ListServices: ""},
	}); err != nil {
		return err
	}
	_, err = stream.Recv()
	return err
}
