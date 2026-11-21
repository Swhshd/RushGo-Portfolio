//go:build !ticket_e2e

package grpcsecurity

import (
	"google.golang.org/grpc/credentials"
)

func testOnlyCredentials(string, string) (credentials.TransportCredentials, error) {
	return nil, ErrTestOnlyInsecureUnavailable
}

func TicketE2EKafkaIdentity() (string, bool) { return "", true }

func ValidateTicketE2ERuntimeIdentity(string) error { return nil }

func ValidateTicketE2ERuntimeResources(string, TicketE2ERuntimeResources) error { return nil }
