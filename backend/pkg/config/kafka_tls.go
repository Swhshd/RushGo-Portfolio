package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"runtime"
)

// KafkaTLSFromEnv retains local fixtures while external modes require a private
// CA and client certificate. Broker DNS SANs are verified by kafka-go's dialer.
func KafkaTLSFromEnv() (*tls.Config, error) {
	switch os.Getenv("RUSHGO_ENV") {
	case "", "local", "isolated_demo":
		return nil, nil
	case "stripe_test", "preproduction", "production":
	default:
		return nil, errors.New("invalid Kafka environment")
	}
	for _, name := range []string{"RUSHGO_KAFKA_CA_FILE", "RUSHGO_KAFKA_CERT_FILE", "RUSHGO_KAFKA_KEY_FILE"} {
		if os.Getenv(name) == "" {
			return nil, ErrNotConfigured
		}
	}
	if runtime.GOOS != "windows" {
		info, err := os.Lstat(os.Getenv("RUSHGO_KAFKA_KEY_FILE"))
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return nil, errors.New("Kafka key must be a private regular file")
		}
	}
	ca, err := os.ReadFile(os.Getenv("RUSHGO_KAFKA_CA_FILE"))
	if err != nil {
		return nil, errors.New("Kafka CA unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("Kafka CA invalid")
	}
	cert, err := tls.LoadX509KeyPair(os.Getenv("RUSHGO_KAFKA_CERT_FILE"), os.Getenv("RUSHGO_KAFKA_KEY_FILE"))
	if err != nil {
		return nil, errors.New("Kafka client identity unavailable")
	}
	return &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}
