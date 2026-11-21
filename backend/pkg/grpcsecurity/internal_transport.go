package grpcsecurity

import (
	"errors"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Ordinary internal RPCs use mTLS in external environments. Dedicated ticket
// authority listeners retain their narrower existing policies and credentials.
var internalCallers = map[string][]string{
	"event":     {ServiceIdentityGateway, ServiceIdentityBooking, ServiceIdentityPayment, ServiceIdentitySaga, ServiceIdentityScheduler},
	"saga":      {ServiceIdentityBooking, ServiceIdentityScheduler},
	"scheduler": {ServiceIdentityGateway},
}

func externalTransport() (bool, error) {
	switch os.Getenv("RUSHGO_ENV") {
	case "", "local", "isolated_demo":
		return false, nil
	case "stripe_test", "preproduction", "production":
		if os.Getenv(TestOnlyInsecureEnv) != "" {
			return true, errors.New("external RPC forbids TEST_ONLY plaintext")
		}
		return true, nil
	default:
		return false, errors.New("invalid RPC environment")
	}
}

func InternalServerOptions(service string) ([]grpc.ServerOption, error) {
	secure, err := externalTransport()
	if err != nil {
		return nil, err
	}
	if !secure {
		return nil, nil
	}
	allowed, ok := internalCallers[service]
	if !ok {
		return nil, errors.New("unknown internal RPC server")
	}
	cert, err := readRequiredEnvFile("RUSHGO_GRPC_SERVER_CERT_FILE")
	if err != nil {
		return nil, err
	}
	key, err := readRequiredEnvFile("RUSHGO_GRPC_SERVER_KEY_FILE")
	if err != nil {
		return nil, err
	}
	ca, err := readRequiredEnvFile("RUSHGO_GRPC_CLIENT_CA_FILE")
	if err != nil {
		return nil, err
	}
	transport, err := NewServerCredentials(cert, key, ca, allowed)
	if err != nil {
		return nil, err
	}
	return []grpc.ServerOption{grpc.Creds(transport)}, nil
}

func InternalClientCredentials(target, identity string) (credentials.TransportCredentials, error) {
	secure, err := externalTransport()
	if err != nil {
		return nil, err
	}
	if !secure {
		return insecure.NewCredentials(), nil
	}
	allowed, ok := internalCallers[target]
	if !ok {
		return nil, errors.New("unknown internal RPC target")
	}
	authorized := false
	for _, candidate := range allowed {
		if identity == candidate {
			authorized = true
		}
	}
	if !authorized {
		return nil, errors.New("internal RPC client identity not authorized")
	}
	cert, err := readRequiredEnvFile("RUSHGO_GRPC_CLIENT_CERT_FILE")
	if err != nil {
		return nil, err
	}
	key, err := readRequiredEnvFile("RUSHGO_GRPC_CLIENT_KEY_FILE")
	if err != nil {
		return nil, err
	}
	ca, err := readRequiredEnvFile("RUSHGO_GRPC_SERVER_CA_FILE")
	if err != nil {
		return nil, err
	}
	return NewClientCredentials(cert, key, ca, os.Getenv("RUSHGO_"+strings.ToUpper(target)+"_GRPC_SERVER_NAME"), identity)
}
