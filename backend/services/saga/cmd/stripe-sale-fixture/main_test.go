//go:build stripe_minimal_test

package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func testEnv(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestLoadConfigRequiresAcknowledgementAndExactRunDatabases(t *testing.T) {
	run := "bd09d7d0-c2b3-4d46-a148-f2a678229901"
	suffix := "bd09d7d0c2b3"
	values := map[string]string{
		"R4_FIXTURE_ACK":        ackValue,
		"R4_RUN_ID":             run,
		"R4_FIXTURE_OUTPUT":     filepath.Join(t.TempDir(), "sale.json"),
		"R4_USER_DATABASE_URL":  "postgres://r4_user_" + suffix + ":secret@user-db:5432/r4_user_" + suffix + "?sslmode=disable",
		"R4_EVENT_DATABASE_URL": "postgres://r4_event_" + suffix + ":secret@event-db:5432/r4_event_" + suffix + "?sslmode=disable",
	}
	cfg, err := loadConfig(testEnv(values))
	if err != nil {
		t.Fatalf("valid isolated config rejected: %v", err)
	}
	if cfg.suffix != suffix || filepath.Base(cfg.output) != "sale.json" {
		t.Fatalf("unexpected scope: %#v", cfg)
	}

	cases := []struct{ name, key, value string }{
		{"missing acknowledgement", "R4_FIXTURE_ACK", ""},
		{"different run id", "R4_RUN_ID", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"},
		{"different User database", "R4_USER_DATABASE_URL", "postgres://r4_user_deadbeefdead:secret@user-db:5432/r4_user_deadbeefdead?sslmode=disable"},
		{"different Event host", "R4_EVENT_DATABASE_URL", "postgres://r4_event_" + suffix + ":secret@outside:5432/r4_event_" + suffix + "?sslmode=disable"},
		{"query override", "R4_EVENT_DATABASE_URL", "postgres://r4_event_" + suffix + ":secret@event-db:5432/r4_event_" + suffix + "?sslmode=disable&options=-csearch_path=other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed := map[string]string{}
			for k, v := range values {
				changed[k] = v
			}
			changed[tc.key] = tc.value
			if _, err := loadConfig(testEnv(changed)); err == nil {
				t.Fatal("unsafe target accepted")
			}
		})
	}
}

func TestMakeSaleFixtureIsSingleHundredCentUserSale(t *testing.T) {
	run := uuid.MustParse("bd09d7d0-c2b3-4d46-a148-f2a678229901")
	fixture, err := makeSaleFixture(run, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if fixture.RunID != run.String() || fixture.AmountCents != 100 || fixture.Currency != "usd" {
		t.Fatalf("unexpected sale scope: %#v", fixture)
	}
	if !strings.HasPrefix(fixture.UserEmail, "stripe.bd09d7d0c2b3") || !strings.HasSuffix(fixture.UserEmail, "@example.test") {
		t.Fatalf("email is not run scoped: %q", fixture.UserEmail)
	}
	if len(fixture.UserPassword) < 40 {
		t.Fatal("synthetic password has insufficient entropy")
	}
}

func TestParseTargetRejectsFallbackAndExtraOptions(t *testing.T) {
	name := "r4_user_bd09d7d0c2b3"
	for _, dsn := range []string{
		"postgres://" + name + ":secret@user-db:5432/" + name + "?sslmode=disable&host=other",
		"postgres://" + name + ":secret@user-db:5432/" + name + "?sslmode=disable&application_name=test",
	} {
		if _, err := parseTarget(dsn, "user-db", name); err == nil {
			t.Fatal("unexpected DSN option accepted")
		}
	}
	if _, err := parseTarget("postgres://user:secret@user-db:5432/other?sslmode=disable", "user-db", name); err == nil {
		t.Fatal("mismatched database accepted")
	}
}

func TestSaleActorsAreDistinctAndDoNotShareCredentials(t *testing.T) {
	f, err := makeSaleFixture(uuid.New(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for _, values := range [][]string{{f.UserID, f.AdminID, f.ApproverID, f.ScannerID}, {f.UserEmail, f.AdminEmail, f.ApproverEmail, f.ScannerEmail}, {f.UserPassword, f.AdminPassword, f.ApproverPassword, f.ScannerPassword}} {
		seen := map[string]bool{}
		for _, v := range values {
			if v == "" || seen[v] {
				t.Fatal("empty or shared actor identity/credential")
			}
			seen[v] = true
		}
	}
}

func TestFixtureMigrationVersionsMatchCurrentFiles(t *testing.T) {
	for name, want := range map[string]int64{"user": userMigration, "event": eventMigration} {
		entries, err := os.ReadDir(filepath.Join("..", "..", "..", name, "migrations"))
		if err != nil {
			t.Fatal(err)
		}
		var latest int64
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".up.sql") {
				n, err := strconv.ParseInt(strings.SplitN(entry.Name(), "_", 2)[0], 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				if n > latest {
					latest = n
				}
			}
		}
		if latest != want {
			t.Fatalf("%s fixture migration %d differs from source %d", name, want, latest)
		}
	}
}
