package grpcsecurity

import (
	"crypto/x509"
	"github.com/ticketbox/pkg/config"
	"os"
	"path/filepath"
	"testing"
)

func TestExternalInternalRPCRequiresCertificates(t *testing.T) {
	t.Setenv("RUSHGO_ENV", "preproduction")
	t.Setenv(TestOnlyInsecureEnv, "")
	t.Setenv("RUSHGO_GRPC_CLIENT_CERT_FILE", "")
	t.Setenv("RUSHGO_GRPC_SERVER_CERT_FILE", "")
	if _, err := InternalClientCredentials("event", ServiceIdentityGateway); err == nil {
		t.Fatal("plaintext fallback")
	}
	if _, err := InternalServerOptions("event"); err == nil {
		t.Fatal("plaintext listener fallback")
	}
	t.Setenv(TestOnlyInsecureEnv, testOnlyInsecureAck)
	if _, err := InternalServerOptions("event"); err == nil {
		t.Fatal("test bypass")
	}
}

func TestInternalTLSAcceptsAuthorizedServiceAndRejectsWrongName(t *testing.T) {
	t.Setenv("RUSHGO_ENV", "preproduction")
	t.Setenv(TestOnlyInsecureEnv, "")
	ca := newTestPKI(t, "TEST_ONLY internal CA")
	before, after := validWindow()
	server, key := ca.leaf(t, "event", "event.internal", "", before, after, x509.ExtKeyUsageServerAuth)
	client, clientKey := ca.leaf(t, "gateway", "", ServiceIdentityGateway, before, after, x509.ExtKeyUsageClientAuth)
	dir := t.TempDir()
	for name, data := range map[string][]byte{"RUSHGO_GRPC_CLIENT_CERT_FILE": client, "RUSHGO_GRPC_CLIENT_KEY_FILE": clientKey, "RUSHGO_GRPC_SERVER_CA_FILE": ca.caPEM} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv(name, path)
	}
	t.Setenv("RUSHGO_EVENT_GRPC_SERVER_NAME", "event.internal")
	sc, err := NewServerCredentials(server, key, ca.caPEM, internalCallers["event"])
	if err != nil {
		t.Fatal(err)
	}
	cc, err := InternalClientCredentials("event", ServiceIdentityGateway)
	if err != nil {
		t.Fatal(err)
	}
	if err = testRPC(t, sc, cc); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUSHGO_KAFKA_CA_FILE", os.Getenv("RUSHGO_GRPC_SERVER_CA_FILE"))
	t.Setenv("RUSHGO_KAFKA_CERT_FILE", os.Getenv("RUSHGO_GRPC_CLIENT_CERT_FILE"))
	t.Setenv("RUSHGO_KAFKA_KEY_FILE", os.Getenv("RUSHGO_GRPC_CLIENT_KEY_FILE"))
	kafkaTLS, kafkaErr := config.KafkaTLSFromEnv()
	if kafkaErr != nil || kafkaTLS == nil || kafkaTLS.InsecureSkipVerify || len(kafkaTLS.Certificates) != 1 || kafkaTLS.RootCAs == nil {
		t.Fatal("Kafka TLS did not retain verified client/server identity")
	}
	t.Setenv("RUSHGO_KAFKA_KEY_FILE", "")
	if _, err := config.KafkaTLSFromEnv(); err == nil {
		t.Fatal("Kafka external plaintext fallback")
	}
	t.Setenv("RUSHGO_EVENT_GRPC_SERVER_NAME", "wrong.internal")
	cc, err = InternalClientCredentials("event", ServiceIdentityGateway)
	if err != nil {
		t.Fatal(err)
	}
	if testRPC(t, sc, cc) == nil {
		t.Fatal("wrong server SAN accepted")
	}
	if _, err = InternalClientCredentials("scheduler", ServiceIdentityPayment); err == nil {
		t.Fatal("unapproved caller accepted")
	}
}
