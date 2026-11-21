//go:build reliability_component

package recoverycomponent

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestOwnerManifestBindsComponentDSNToFreshResourceIdentity(t *testing.T) {
	dsn := "postgres://component@127.0.0.1:55432/ticketbox_saga_component"
	manifest := OwnerManifest{
		ManifestVersion:        "rushgo.saga.component-pg-owner.v1",
		ResourceID:             repeatHex("a", 64),
		ContainerID:            repeatHex("a", 64),
		Database:               "ticketbox_saga_component",
		Host:                   "127.0.0.1",
		Port:                   55432,
		DSNSHA256:              digestString(dsn),
		RunID:                  "run-123",
		OwnerNonce:             "nonce-456",
		ServerSystemIdentifier: "7654321098765432100",
		DatabaseMarker:         "saga-component-marker-789",
	}
	if err := validateOwnerManifest(dsn, manifest); err != nil {
		t.Fatalf("valid owner manifest rejected: %v", err)
	}

	badHash := manifest
	badHash.DSNSHA256 = digestString("postgres://component@127.0.0.1:55433/ticketbox_saga_component")
	if err := validateOwnerManifest(dsn, badHash); err == nil {
		t.Fatal("DSN hash mismatch accepted")
	}

	badContainer := manifest
	badContainer.ContainerID = repeatHex("b", 64)
	if err := validateOwnerManifest(dsn, badContainer); err == nil {
		t.Fatal("container/resource identity mismatch accepted")
	}

	badRemote := manifest
	badRemote.Host = "203.0.113.17"
	if err := validateOwnerManifest(dsn, badRemote); err == nil {
		t.Fatal("non-local database endpoint accepted")
	}
}

func digestString(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func repeatHex(character string, count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += character
	}
	return result
}
