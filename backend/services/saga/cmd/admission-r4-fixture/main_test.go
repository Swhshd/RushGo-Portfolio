//go:build admission_r4

package main

import (
	"crypto/sha256"
	"encoding/base64"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/saga/internal/domain"
)

func TestFixtureSchemaGuardsMatchPackagedMigrations(t *testing.T) {
	for _, spec := range serviceSpecs {
		files, err := filepath.Glob(filepath.Join("..", "..", "..", spec.name, "migrations", "*.up.sql"))
		if err != nil || len(files) == 0 {
			t.Fatalf("missing %s migration sources: %v", spec.name, err)
		}
		var latest int64
		for _, file := range files {
			version, err := strconv.ParseInt(strings.SplitN(filepath.Base(file), "_", 2)[0], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			if version > latest {
				latest = version
			}
		}
		if latest != spec.migration {
			t.Errorf("%s fixture requires %d but package migrates to %d", spec.name, spec.migration, latest)
		}
	}
}

func TestDatabaseTargetGuardRequiresExactIsolatedIdentity(t *testing.T) {
	const suffix = "0123456789ab"
	valid := "postgres://r4_user_" + suffix + ":synthetic@user-db:5432/r4_user_" + suffix + "?sslmode=disable"
	if _, err := parseDatabaseTarget("user", valid, suffix); err != nil {
		t.Fatalf("valid isolated target rejected: %v", err)
	}

	for name, dsn := range map[string]string{
		"wrong host":     "postgres://r4_user_" + suffix + ":synthetic@booking-db:5432/r4_user_" + suffix,
		"host fallback":  "postgres://r4_user_" + suffix + ":synthetic@user-db,booking-db:5432/r4_user_" + suffix,
		"wrong port":     "postgres://r4_user_" + suffix + ":synthetic@user-db:5433/r4_user_" + suffix,
		"wrong database": "postgres://r4_user_" + suffix + ":synthetic@user-db:5432/ticketbox_user",
		"wrong user":     "postgres://ticketbox:synthetic@user-db:5432/r4_user_" + suffix,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseDatabaseTarget("user", dsn, suffix); err == nil {
				t.Fatal("unsafe database identity accepted")
			}
		})
	}
}

func TestFixtureRequiresExplicitIsolationAcknowledgment(t *testing.T) {
	values := map[string]string{
		"R4_RUN_ID":         "01234567-89ab-4cde-8fab-0123456789ab",
		"R4_FIXTURE_OUTPUT": "/private/fixture.json",
	}
	if _, err := loadConfig(func(key string) string { return values[key] }); err == nil {
		t.Fatal("fixture configuration accepted without the isolation acknowledgment")
	}
}

func TestRawURLSecretContainsExactly32RandomBytes(t *testing.T) {
	secret, err := randomRawURLSecret()
	if err != nil {
		t.Fatalf("generate synthetic secret: %v", err)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(secret)
	if err != nil || len(decoded) != 32 {
		t.Fatalf("secret is not a 32-byte RawURL value: length=%d err=%v", len(decoded), err)
	}
}

func TestEventSeatSetFingerprintMatchesVersionedAuthorityContract(t *testing.T) {
	eventID := uuid.MustParse("00000000-0000-4000-8000-000000000003")
	first := uuid.MustParse("00000000-0000-4000-8000-000000000004")
	second := uuid.MustParse("00000000-0000-4000-8000-000000000005")
	got, err := eventSeatSetFingerprint(eventID, []uuid.UUID{second, first})
	if err != nil {
		t.Fatalf("Event seat-set fingerprint: %v", err)
	}
	want := sha256.Sum256([]byte("v1\n" + eventID.String() + "\n" + first.String() + "," + second.String() + "\n"))
	if got != want {
		t.Fatalf("Event seat-set fingerprint=%x, want %x", got, want)
	}
}

func TestSagaSnapshotUsesCanonicalInputBoundToFixtureAuthority(t *testing.T) {
	deadline := time.Date(2026, time.November, 2, 12, 0, 0, 123456000, time.UTC)
	fixture := fixtureCase{
		BookingID:           uuid.MustParse("00000000-0000-4000-8000-000000000001"),
		UserID:              uuid.MustParse("00000000-0000-4000-8000-000000000002"),
		EventID:             uuid.MustParse("00000000-0000-4000-8000-000000000003"),
		SeatID:              uuid.MustParse("00000000-0000-4000-8000-000000000004"),
		AmountCents:         100,
		Currency:            "usd",
		BookingMode:         "pessimistic",
		ReservationDeadline: deadline,
	}

	raw, fingerprint, err := canonicalSagaSnapshot(fixture)
	if err != nil {
		t.Fatalf("canonical Saga snapshot: %v", err)
	}
	decoded, err := domain.DecodeInput(raw)
	if err != nil {
		t.Fatalf("decode canonical Saga snapshot: %v", err)
	}
	if decoded.BookingID != fixture.BookingID || decoded.UserID != fixture.UserID || decoded.EventID != fixture.EventID ||
		len(decoded.SeatIDs) != 1 || decoded.SeatIDs[0] != fixture.SeatID || decoded.AmountCents != fixture.AmountCents ||
		decoded.Currency != fixture.Currency || decoded.BookingMode != fixture.BookingMode ||
		!decoded.ReservationDeadline.Equal(deadline) || fingerprint != sha256.Sum256(raw) || decoded.Fingerprint != fingerprint {
		t.Fatalf("canonical snapshot is not bound to fixture authority: decoded=%+v fingerprint=%x", decoded, fingerprint)
	}

	fixture.SeatID = uuid.MustParse("00000000-0000-4000-8000-000000000005")
	changed, changedFingerprint, err := canonicalSagaSnapshot(fixture)
	if err != nil {
		t.Fatalf("canonical changed snapshot: %v", err)
	}
	if changedFingerprint == fingerprint || string(changed) == string(raw) {
		t.Fatal("changing the seat authority did not change the canonical Saga binding")
	}
}
