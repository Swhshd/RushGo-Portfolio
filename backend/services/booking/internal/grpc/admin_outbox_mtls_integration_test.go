package grpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	adminOutboxJWTTestSecret  = "isolated-admin-outbox-jwt-test-secret"
	adminOutboxTestBookingDNS = "booking-outbox.test"
	adminOutboxTestUserDNS    = "user-outbox.test"
)

type adminOutboxTLSCA struct {
	certificate *x509.Certificate
	privateKey  *ecdsa.PrivateKey
	pem         []byte
}

type adminOutboxTLSLeaf struct {
	certificate []byte
	privateKey  []byte
}

func newAdminOutboxTLSCA(t *testing.T, commonName string) adminOutboxTLSCA {
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
		Subject:               pkix.Name{CommonName: commonName},
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
	return adminOutboxTLSCA{certificate: certificate, privateKey: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca adminOutboxTLSCA) leaf(t *testing.T, dnsName, identity string, usage x509.ExtKeyUsage) adminOutboxTLSLeaf {
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
		Subject:               pkix.Name{CommonName: identity},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{usage},
	}
	if dnsName != "" {
		template.DNSNames = []string{dnsName}
	}
	if identity != "" {
		parsed, err := url.Parse(identity)
		if err != nil {
			t.Fatal(err)
		}
		template.URIs = []*url.URL{parsed}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, &key.PublicKey, ca.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return adminOutboxTLSLeaf{
		certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		privateKey:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	}
}

type adminOutboxJWTClaims struct {
	Issuer    string `json:"iss"`
	ExpiresAt int64  `json:"exp"`
	IssuedAt  int64  `json:"iat"`
	UserID    string `json:"user_id"`
	Email     string `json:"email"`
	Role      string `json:"role"`
}

// adminOutboxJWTUserFixture is a local UserService RPC fixture that verifies
// the same HS256 ticket claims used by the existing JWTManager. It performs
// real signature validation; it never trusts caller-supplied role metadata.
type adminOutboxJWTUserFixture struct {
	userv1.UnimplementedUserServiceServer
	secret              []byte
	calls               int
	currentRoleCalls    int
	currentProfileRole  string
	currentProfileID    string
	currentProfileError error
}

func (s *adminOutboxJWTUserFixture) ValidateToken(_ context.Context, req *userv1.ValidateTokenRequest) (*userv1.ValidateTokenResponse, error) {
	s.calls++
	claims, err := validateAdminOutboxJWT(req.GetAccessToken(), s.secret)
	if err != nil {
		return &userv1.ValidateTokenResponse{Valid: false}, nil
	}
	if adminOutboxValidateTokenRequestsCurrentRole(req) {
		s.currentRoleCalls++
		if s.currentProfileError != nil {
			return nil, s.currentProfileError
		}
		role := s.currentProfileRole
		if role == "" {
			role = claims.Role
		}
		userID := s.currentProfileID
		if userID == "" {
			userID = claims.UserID
		}
		return &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Email: claims.Email, Role: role, CurrentRoleVerified: true}, nil
	}
	return &userv1.ValidateTokenResponse{Valid: true, UserId: claims.UserID, Email: claims.Email, Role: claims.Role}, nil
}

func adminOutboxValidateTokenRequestsCurrentRole(req *userv1.ValidateTokenRequest) bool {
	if req == nil {
		return false
	}
	field := req.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name("require_current_role"))
	return field != nil && req.ProtoReflect().Get(field).Bool()
}

func validateAdminOutboxJWT(token string, secret []byte) (*adminOutboxJWTClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, status.Error(codes.Unauthenticated, "invalid test JWT")
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, err
	}
	var header struct {
		Algorithm string `json:"alg"`
		Type      string `json:"typ"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil || header.Algorithm != "HS256" || header.Type != "JWT" {
		return nil, status.Error(codes.Unauthenticated, "invalid test JWT header")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return nil, status.Error(codes.Unauthenticated, "invalid test JWT signature")
	}
	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}
	claims := &adminOutboxJWTClaims{}
	if err := json.Unmarshal(claimsJSON, claims); err != nil {
		return nil, err
	}
	if claims.Issuer != "ticketbox" || claims.ExpiresAt <= time.Now().Unix() || claims.UserID == "" || claims.Role == "" {
		return nil, status.Error(codes.Unauthenticated, "invalid test JWT claims")
	}
	if id, err := uuid.Parse(claims.UserID); err != nil || id == uuid.Nil {
		return nil, status.Error(codes.Unauthenticated, "invalid test JWT user")
	}
	return claims, nil
}

func signAdminOutboxJWT(role string, expires time.Time) string {
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	claims, _ := json.Marshal(adminOutboxJWTClaims{
		Issuer: "ticketbox", ExpiresAt: expires.Unix(), IssuedAt: time.Now().Unix(),
		UserID: ticketAuthUserID, Email: "synthetic-admin@rushgo.test", Role: role,
	})
	encodedHeader := base64.RawURLEncoding.EncodeToString(header)
	encodedClaims := base64.RawURLEncoding.EncodeToString(claims)
	message := encodedHeader + "." + encodedClaims
	mac := hmac.New(sha256.New, []byte(adminOutboxJWTTestSecret))
	_, _ = mac.Write([]byte(message))
	return message + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func tamperAdminOutboxJWTSignature(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[2] == "" {
		return token
	}
	replacement := byte('A')
	if parts[2][0] == replacement {
		replacement = 'B'
	}
	parts[2] = string(replacement) + parts[2][1:]
	return strings.Join(parts, ".")
}

func tamperAdminOutboxJWTRole(token, role string) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return token
	}
	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return token
	}
	var claims map[string]any
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		return token
	}
	claims["role"] = role
	updatedClaims, err := json.Marshal(claims)
	if err != nil {
		return token
	}
	parts[1] = base64.RawURLEncoding.EncodeToString(updatedClaims)
	return strings.Join(parts, ".")
}

type adminOutboxTLSIntegration struct {
	bookingListener *bufconn.Listener
	bookingUser     *adminOutboxJWTUserFixture
	reader          *adminOutboxRPCProjectionReader
	userConn        *grpc.ClientConn
	bookingCA       adminOutboxTLSCA
	userCA          adminOutboxTLSCA
	bookingServer   adminOutboxTLSLeaf
	userServer      adminOutboxTLSLeaf
	gateway         adminOutboxTLSLeaf
	saga            adminOutboxTLSLeaf
	wrongCAClient   adminOutboxTLSLeaf
}

func newAdminOutboxTLSIntegration(t *testing.T) *adminOutboxTLSIntegration {
	t.Helper()
	bookingCA := newAdminOutboxTLSCA(t, "isolated Booking RPC test CA")
	userCA := newAdminOutboxTLSCA(t, "isolated User RPC test CA")
	wrongCA := newAdminOutboxTLSCA(t, "untrusted client test CA")
	bookingServerLeaf := bookingCA.leaf(t, adminOutboxTestBookingDNS, grpcsecurity.ServiceIdentityBooking, x509.ExtKeyUsageServerAuth)
	userServerLeaf := userCA.leaf(t, adminOutboxTestUserDNS, grpcsecurity.ServiceIdentityUser, x509.ExtKeyUsageServerAuth)
	gatewayLeaf := bookingCA.leaf(t, "", grpcsecurity.ServiceIdentityGateway, x509.ExtKeyUsageClientAuth)
	sagaLeaf := bookingCA.leaf(t, "", grpcsecurity.ServiceIdentitySaga, x509.ExtKeyUsageClientAuth)
	bookingClientToUser := userCA.leaf(t, "", grpcsecurity.ServiceIdentityBooking, x509.ExtKeyUsageClientAuth)
	wrongCAClient := wrongCA.leaf(t, "", grpcsecurity.ServiceIdentityGateway, x509.ExtKeyUsageClientAuth)

	userServerCredentials, err := grpcsecurity.NewServerCredentials(userServerLeaf.certificate, userServerLeaf.privateKey, userCA.pem, []string{grpcsecurity.ServiceIdentityBooking})
	if err != nil {
		t.Fatal(err)
	}
	userListener := bufconn.Listen(1 << 20)
	userServer := grpc.NewServer(grpc.Creds(userServerCredentials), grpc.ChainUnaryInterceptor(grpcsecurity.UserServiceMethodAuthorizationInterceptor()))
	userFixture := &adminOutboxJWTUserFixture{secret: []byte(adminOutboxJWTTestSecret)}
	userv1.RegisterUserServiceServer(userServer, userFixture)
	go func() { _ = userServer.Serve(userListener) }()
	t.Cleanup(func() { userServer.Stop(); _ = userListener.Close() })

	bookingToUserCredentials, err := grpcsecurity.NewClientCredentials(bookingClientToUser.certificate, bookingClientToUser.privateKey, userCA.pem, adminOutboxTestUserDNS, grpcsecurity.ServiceIdentityBooking)
	if err != nil {
		t.Fatal(err)
	}
	userConn, err := grpc.NewClient("passthrough:///admin-outbox-user-test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return userListener.Dial() }), grpc.WithTransportCredentials(bookingToUserCredentials))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = userConn.Close() })

	bookingServerCredentials, err := grpcsecurity.NewServerCredentials(bookingServerLeaf.certificate, bookingServerLeaf.privateKey, bookingCA.pem, []string{grpcsecurity.ServiceIdentityGateway, grpcsecurity.ServiceIdentitySaga})
	if err != nil {
		t.Fatal(err)
	}
	bookingListener := bufconn.Listen(1 << 20)
	bookingGRPCServer := grpc.NewServer(grpc.Creds(bookingServerCredentials))
	reader := &adminOutboxRPCProjectionReader{rows: []repository.AdminOutboxProjection{{
		ID: uuid.MustParse("80000000-0000-4000-8000-000000000001"), EventType: "BookingConfirmed", Published: true,
		CreatedAt: time.Date(2026, 10, 31, 12, 0, 0, 123000000, time.UTC),
	}}}
	bookingImplementation := NewBookingServer(nil, zap.NewNop())
	bookingImplementation.SetAdminOutboxService(service.NewAdminOutboxService(reader))
	bookingImplementation.SetAdminOutboxSecure(true)
	userClient := userv1.NewUserServiceClient(userConn)
	ticketAuthenticator := NewTicketPrincipalAuthenticator(userClient, &fakeTicketRevocationChecker{})
	bookingImplementation.SetTicketPrincipalAuthenticator(ticketAuthenticator)
	bookingImplementation.SetAdminOutboxPrincipalAuthenticator(NewAdminOutboxPrincipalAuthenticator(ticketAuthenticator, userClient))
	bookingv1.RegisterBookingServiceServer(bookingGRPCServer, bookingImplementation)
	go func() { _ = bookingGRPCServer.Serve(bookingListener) }()
	t.Cleanup(func() { bookingGRPCServer.Stop(); _ = bookingListener.Close() })

	return &adminOutboxTLSIntegration{
		bookingListener: bookingListener, bookingUser: userFixture, reader: reader, userConn: userConn,
		bookingCA: bookingCA, userCA: userCA, bookingServer: bookingServerLeaf, userServer: userServerLeaf,
		gateway: gatewayLeaf, saga: sagaLeaf, wrongCAClient: wrongCAClient,
	}
}

func (fixture *adminOutboxTLSIntegration) clientCredentials(t *testing.T, leaf adminOutboxTLSLeaf, identity string) credentials.TransportCredentials {
	t.Helper()
	creds, err := grpcsecurity.NewClientCredentials(leaf.certificate, leaf.privateKey, fixture.bookingCA.pem, adminOutboxTestBookingDNS, identity)
	if err != nil {
		t.Fatal(err)
	}
	return creds
}

func (fixture *adminOutboxTLSIntegration) invoke(t *testing.T, creds credentials.TransportCredentials, token string, spoofAdminRole bool) (*bookingv1.ListAdminOutboxEventsResponse, error) {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///admin-outbox-booking-test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return fixture.bookingListener.Dial() }), grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}
	if spoofAdminRole {
		ctx = metadata.AppendToOutgoingContext(ctx, "role", "admin", "user_id", uuid.NewString())
	}
	return bookingv1.NewBookingServiceClient(conn).ListAdminOutboxEvents(ctx, adminOutboxRPCRequest())
}

func TestAdminOutboxRealMutualTLSAndSignedJWTBoundary(t *testing.T) {
	fixture := newAdminOutboxTLSIntegration(t)
	validAdmin := signAdminOutboxJWT("admin", time.Now().Add(5*time.Minute))
	expiredAdmin := signAdminOutboxJWT("admin", time.Now().Add(-time.Minute))
	validUser := signAdminOutboxJWT("user", time.Now().Add(5*time.Minute))
	badSignature := tamperAdminOutboxJWTSignature(validAdmin)
	forgedAdminRole := tamperAdminOutboxJWTRole(validUser, "admin")

	for _, tc := range []struct {
		name                string
		credentials         credentials.TransportCredentials
		token               string
		spoofAdmin          bool
		currentProfileRole  string
		currentProfileID    string
		currentProfileError error
		wantCode            codes.Code
		wantReaderAdd       int
		wantUserAdd         int
		wantCurrentRoleAdd  int
	}{
		{name: "Gateway certificate and signed Admin JWT with current role", credentials: fixture.clientCredentials(t, fixture.gateway, grpcsecurity.ServiceIdentityGateway), token: validAdmin, wantCode: codes.OK, wantReaderAdd: 1, wantUserAdd: 2, wantCurrentRoleAdd: 1},
		{name: "trusted Saga certificate is denied on Admin method", credentials: fixture.clientCredentials(t, fixture.saga, grpcsecurity.ServiceIdentitySaga), token: validAdmin, wantCode: codes.PermissionDenied},
		{name: "tampered signature", credentials: fixture.clientCredentials(t, fixture.gateway, grpcsecurity.ServiceIdentityGateway), token: badSignature, wantCode: codes.Unauthenticated, wantUserAdd: 1},
		{name: "expired signed token", credentials: fixture.clientCredentials(t, fixture.gateway, grpcsecurity.ServiceIdentityGateway), token: expiredAdmin, wantCode: codes.Unauthenticated, wantUserAdd: 1},
		{name: "unsigned JWT role elevation", credentials: fixture.clientCredentials(t, fixture.gateway, grpcsecurity.ServiceIdentityGateway), token: forgedAdminRole, wantCode: codes.Unauthenticated, wantUserAdd: 1},
		{name: "valid user cannot elevate role through metadata", credentials: fixture.clientCredentials(t, fixture.gateway, grpcsecurity.ServiceIdentityGateway), token: validUser, spoofAdmin: true, wantCode: codes.PermissionDenied, wantUserAdd: 1},
		{name: "old Admin JWT denied after live demotion", credentials: fixture.clientCredentials(t, fixture.gateway, grpcsecurity.ServiceIdentityGateway), token: validAdmin, currentProfileRole: "user", wantCode: codes.PermissionDenied, wantUserAdd: 2, wantCurrentRoleAdd: 1},
		{name: "current identity mismatch is denied", credentials: fixture.clientCredentials(t, fixture.gateway, grpcsecurity.ServiceIdentityGateway), token: validAdmin, currentProfileRole: "admin", currentProfileID: ticketAuthOtherID, wantCode: codes.PermissionDenied, wantUserAdd: 2, wantCurrentRoleAdd: 1},
		{name: "current-role service outage is unavailable", credentials: fixture.clientCredentials(t, fixture.gateway, grpcsecurity.ServiceIdentityGateway), token: validAdmin, currentProfileError: status.Error(codes.Unavailable, "private current-role failure"), wantCode: codes.Unavailable, wantUserAdd: 2, wantCurrentRoleAdd: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			readsBefore, userCallsBefore := fixture.reader.calls, fixture.bookingUser.calls
			currentRoleCallsBefore := fixture.bookingUser.currentRoleCalls
			fixture.bookingUser.currentProfileRole = tc.currentProfileRole
			fixture.bookingUser.currentProfileID = tc.currentProfileID
			fixture.bookingUser.currentProfileError = tc.currentProfileError
			response, err := fixture.invoke(t, tc.credentials, tc.token, tc.spoofAdmin)
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("real mTLS/JWT RPC status = %s, want %s (response=%+v err=%v)", got, tc.wantCode, response, err)
			}
			if fixture.reader.calls-readsBefore != tc.wantReaderAdd {
				t.Fatalf("projection reader calls increased by %d, want %d", fixture.reader.calls-readsBefore, tc.wantReaderAdd)
			}
			if fixture.bookingUser.calls-userCallsBefore != tc.wantUserAdd {
				t.Fatalf("real UserService ValidateToken calls increased by %d, want %d", fixture.bookingUser.calls-userCallsBefore, tc.wantUserAdd)
			}
			if fixture.bookingUser.currentRoleCalls-currentRoleCallsBefore != tc.wantCurrentRoleAdd {
				t.Fatalf("real UserService current-role ValidateToken calls increased by %d, want %d", fixture.bookingUser.currentRoleCalls-currentRoleCallsBefore, tc.wantCurrentRoleAdd)
			}
			if tc.wantCode == codes.OK {
				if response == nil || len(response.GetRecords()) != 1 || response.GetRecords()[0].GetEventType() != "BookingConfirmed" {
					t.Fatalf("authorized response = %+v", response)
				}
			}
		})
	}
}

func TestAdminOutboxRealMutualTLSRejectsMissingAndUntrustedClientCertificates(t *testing.T) {
	fixture := newAdminOutboxTLSIntegration(t)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(fixture.bookingCA.pem) {
		t.Fatal("failed to prepare isolated Booking server trust roots")
	}
	noCertificate := credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: adminOutboxTestBookingDNS, MinVersion: tls.VersionTLS12})
	wrongCA, err := grpcsecurity.NewClientCredentials(fixture.wrongCAClient.certificate, fixture.wrongCAClient.privateKey, fixture.bookingCA.pem, adminOutboxTestBookingDNS, grpcsecurity.ServiceIdentityGateway)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		creds credentials.TransportCredentials
	}{
		{name: "no client certificate", creds: noCertificate},
		{name: "client certificate signed by wrong CA", creds: wrongCA},
	} {
		t.Run(tc.name, func(t *testing.T) {
			readsBefore, userCallsBefore := fixture.reader.calls, fixture.bookingUser.calls
			_, err := fixture.invoke(t, tc.creds, signAdminOutboxJWT("admin", time.Now().Add(5*time.Minute)), false)
			if err == nil {
				t.Fatal("Booking RPC succeeded without a trusted verified client certificate")
			}
			if got := status.Code(err); got != codes.Unavailable {
				t.Fatalf("untrusted client certificate RPC status = %s, want transport handshake failure (err=%v)", got, err)
			}
			if fixture.reader.calls != readsBefore || fixture.bookingUser.calls != userCallsBefore {
				t.Fatalf("failed TLS handshake reached authorization/data dependencies: reader=%d->%d user=%d->%d", readsBefore, fixture.reader.calls, userCallsBefore, fixture.bookingUser.calls)
			}
		})
	}
}
