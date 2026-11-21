//go:build stripe_minimal_test

// Command stripe-sale-fixture seeds only the synthetic User and sale catalog
// needed to create a real booking through Gateway. It never creates bookings,
// payments, Saga rows, refunds, check-ins, or provider observations.
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

const (
	ackValue        = "ISOLATED_SYNTHETIC_ONLY"
	fixtureAmount   = int64(100)
	fixtureCurrency = "usd"
	userMigration   = int64(3)
	eventMigration  = int64(8)
)

var uuidPattern = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)

type config struct {
	runID    uuid.UUID
	suffix   string
	output   string
	userDSN  string
	eventDSN string
}

type saleFixture struct {
	RunID            string `json:"run_id"`
	UserID           string `json:"user_id"`
	UserEmail        string `json:"user_email"`
	UserPassword     string `json:"user_password"`
	EventID          string `json:"event_id"`
	TierID           string `json:"tier_id"`
	SeatID           string `json:"seat_id"`
	AmountCents      int64  `json:"amount_cents"`
	Currency         string `json:"currency"`
	AdminID          string `json:"admin_id"`
	AdminEmail       string `json:"admin_email"`
	AdminPassword    string `json:"admin_password"`
	ApproverID       string `json:"approver_id"`
	ApproverEmail    string `json:"approver_email"`
	ApproverPassword string `json:"approver_password"`
	ScannerID        string `json:"scanner_id"`
	ScannerEmail     string `json:"scanner_email"`
	ScannerPassword  string `json:"scanner_password"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx, os.Getenv, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "stripe-sale-fixture:", err)
		os.Exit(1)
	}
}

func loadConfig(getenv func(string) string) (config, error) {
	var c config
	if getenv("R4_FIXTURE_ACK") != ackValue {
		return c, errors.New("R4_FIXTURE_ACK must equal ISOLATED_SYNTHETIC_ONLY")
	}
	runText := getenv("R4_RUN_ID")
	runID, err := uuid.Parse(runText)
	if err != nil || !uuidPattern.MatchString(runText) || runText != runID.String() {
		return c, errors.New("R4_RUN_ID must be a canonical lowercase UUID")
	}
	output := getenv("R4_FIXTURE_OUTPUT")
	if !filepath.IsAbs(output) || filepath.Clean(output) != output || filepath.Base(output) != "sale.json" {
		return c, errors.New("R4_FIXTURE_OUTPUT must be an absolute sale.json path")
	}
	suffix := strings.ReplaceAll(runID.String(), "-", "")[:12]
	userDSN, err := parseTarget(getenv("R4_USER_DATABASE_URL"), "user-db", "r4_user_"+suffix)
	if err != nil {
		return c, fmt.Errorf("User database target rejected: %w", err)
	}
	eventDSN, err := parseTarget(getenv("R4_EVENT_DATABASE_URL"), "event-db", "r4_event_"+suffix)
	if err != nil {
		return c, fmt.Errorf("Event database target rejected: %w", err)
	}
	return config{runID: runID, suffix: suffix, output: output, userDSN: userDSN, eventDSN: eventDSN}, nil
}

func parseTarget(raw, expectedHost, expectedName string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgres" && u.Scheme != "postgresql" || u.Hostname() != expectedHost || u.Port() != "5432" || u.Path != "/"+expectedName || u.User == nil {
		return "", errors.New("requires the run-specific database and internal database host")
	}
	user := u.User.Username()
	if user != expectedName {
		return "", errors.New("database role must match the run-specific database")
	}
	password, ok := u.User.Password()
	if !ok || password == "" || u.RawQuery != "sslmode=disable" || u.Fragment != "" {
		return "", errors.New("database URL must have credentials and only sslmode=disable")
	}
	host, _, err := net.SplitHostPort(net.JoinHostPort(u.Hostname(), u.Port()))
	if err != nil || host != expectedHost {
		return "", errors.New("database host is invalid")
	}
	return raw, nil
}

func run(ctx context.Context, getenv func(string) string, stdout io.Writer) error {
	c, err := loadConfig(getenv)
	if err != nil {
		return err
	}
	userPool, err := openPool(ctx, c.userDSN)
	if err != nil {
		return errors.New("cannot connect to isolated User database")
	}
	defer userPool.Close()
	eventPool, err := openPool(ctx, c.eventDSN)
	if err != nil {
		return errors.New("cannot connect to isolated Event database")
	}
	defer eventPool.Close()
	if err := validateEmpty(ctx, userPool, "r4_user_"+c.suffix, userMigration, []string{"users", "refresh_tokens"}); err != nil {
		return fmt.Errorf("User target refused: %w", err)
	}
	if err := validateEmpty(ctx, eventPool, "r4_event_"+c.suffix, eventMigration, []string{"events", "ticket_tiers", "seats"}); err != nil {
		return fmt.Errorf("Event target refused: %w", err)
	}
	file, err := reserveOutput(c.output)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if !keep {
			file.Close()
			_ = os.Remove(c.output)
		}
	}()
	fixture, err := makeSaleFixture(c.runID, time.Now().UTC())
	if err != nil {
		return err
	}
	userTx, err := userPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return errors.New("cannot begin isolated User fixture transaction")
	}
	defer userTx.Rollback(context.Background())
	eventTx, err := eventPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return errors.New("cannot begin isolated Event fixture transaction")
	}
	defer eventTx.Rollback(context.Background())
	if err := seedUser(ctx, userTx, fixture); err != nil {
		return err
	}
	if err := seedEvent(ctx, eventTx, fixture, time.Now().UTC()); err != nil {
		return err
	}
	if err := writeFixture(file, fixture); err != nil {
		return err
	}
	if err := userTx.Commit(ctx); err != nil {
		return errors.New("commit isolated User fixture failed")
	}
	if err := eventTx.Commit(ctx); err != nil {
		return errors.New("commit isolated Event fixture failed after User commit; use a new RunId")
	}
	keep = true
	_, err = fmt.Fprintf(stdout, "sale_fixture_ready run_id=%s user_id=%s event_id=%s tier_id=%s seat_id=%s amount_cents=100 currency=usd output=%s\n", fixture.RunID, fixture.UserID, fixture.EventID, fixture.TierID, fixture.SeatID, c.output)
	return err
}

func openPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	c, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	c.MaxConns = 2
	c.MinConns = 0
	p, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, err
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

func validateEmpty(ctx context.Context, p *pgxpool.Pool, name string, requiredVersion int64, requiredTables []string) error {
	var db, user string
	if err := p.QueryRow(ctx, "SELECT current_database(),current_user").Scan(&db, &user); err != nil {
		return errors.New("read database identity")
	}
	if db != name || user != name {
		return errors.New("database/user does not match fresh RunId")
	}
	var version int64
	var dirty bool
	if err := p.QueryRow(ctx, "SELECT version,dirty FROM schema_migrations").Scan(&version, &dirty); err != nil {
		return errors.New("fully migrated target required")
	}
	if dirty || version != requiredVersion {
		return fmt.Errorf("target must be clean at migration %d", requiredVersion)
	}
	for _, table := range requiredTables {
		var count int64
		if err := p.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil {
			return fmt.Errorf("read required table %s", table)
		}
		if count != 0 {
			return fmt.Errorf("required table %s must be empty", table)
		}
	}
	rows, err := p.Query(ctx, "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public' AND tablename<>'schema_migrations' ORDER BY tablename")
	if err != nil {
		return errors.New("inspect target tables")
	}
	seen := make(map[string]bool)
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			rows.Close()
			return errors.New("read target table list")
		}
		seen[table] = true
		var count int64
		if err := p.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil {
			rows.Close()
			return errors.New("inspect target table contents")
		}
		if count != 0 {
			rows.Close()
			return errors.New("all application tables must be empty")
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return errors.New("read target table list")
	}
	rows.Close()
	for _, table := range requiredTables {
		if !seen[table] {
			return fmt.Errorf("required migrated table %s is missing", table)
		}
	}
	return nil
}

func reserveOutput(path string) (*os.File, error) {
	info, err := os.Stat(filepath.Dir(path))
	if err != nil || !info.IsDir() {
		return nil, errors.New("private output directory must already exist")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, errors.New("private output must be new")
	}
	if err = f.Chmod(0o600); err != nil {
		f.Close()
		os.Remove(path)
		return nil, errors.New("restrict private output permissions")
	}
	return f, nil
}

func writeFixture(file *os.File, fixture saleFixture) error {
	if err := file.Chmod(0o600); err != nil {
		return errors.New("restrict private output permissions")
	}
	e := json.NewEncoder(file)
	e.SetIndent("", "  ")
	if err := e.Encode(fixture); err != nil {
		return errors.New("write private fixture")
	}
	if err := file.Sync(); err != nil {
		return errors.New("sync private fixture")
	}
	return nil
}

func makeSaleFixture(run uuid.UUID, now time.Time) (saleFixture, error) {
	var secret [32]byte
	if _, err := io.ReadFull(rand.Reader, secret[:]); err != nil {
		return saleFixture{}, errors.New("generate synthetic password")
	}
	password := base64.RawURLEncoding.EncodeToString(secret[:])
	f := saleFixture{RunID: run.String(), UserID: uuid.NewString(), UserEmail: "stripe." + strings.ReplaceAll(run.String(), "-", "") + "@example.test", UserPassword: password, EventID: uuid.NewString(), TierID: uuid.NewString(), SeatID: uuid.NewString(), AmountCents: fixtureAmount, Currency: fixtureCurrency}
	for _, actor := range []struct {
		id, email, password *string
		role                string
	}{
		{&f.AdminID, &f.AdminEmail, &f.AdminPassword, "admin"},
		{&f.ApproverID, &f.ApproverEmail, &f.ApproverPassword, "approver"},
		{&f.ScannerID, &f.ScannerEmail, &f.ScannerPassword, "scanner"},
	} {
		if _, err := io.ReadFull(rand.Reader, secret[:]); err != nil {
			return saleFixture{}, errors.New("generate actor credential")
		}
		*actor.id = uuid.NewString()
		*actor.email = actor.role + "." + strings.ReplaceAll(run.String(), "-", "") + "@example.test"
		*actor.password = base64.RawURLEncoding.EncodeToString(secret[:])
	}
	return f, nil
}

func seedUser(ctx context.Context, tx pgx.Tx, f saleFixture) error {
	for _, actor := range []struct{ id, email, password, role string }{
		{f.UserID, f.UserEmail, f.UserPassword, "user"},
		{f.AdminID, f.AdminEmail, f.AdminPassword, "admin"},
		{f.ApproverID, f.ApproverEmail, f.ApproverPassword, "user"},
		{f.ScannerID, f.ScannerEmail, f.ScannerPassword, "user"},
	} {
		hash, err := bcrypt.GenerateFromPassword([]byte(actor.password), bcrypt.DefaultCost)
		if err != nil {
			return errors.New("hash synthetic actor password")
		}
		_, err = tx.Exec(ctx, `INSERT INTO users(id,email,email_canonical,email_verified_at,password_hash,name,role) VALUES($1,$2,$2,$3,$4,'Stripe TEST synthetic',$5)`, actor.id, actor.email, time.Now().UTC(), string(hash), actor.role)
		if err != nil {
			return errors.New("insert isolated actor")
		}
	}

	return nil
}

func seedEvent(ctx context.Context, tx pgx.Tx, f saleFixture, now time.Time) error {
	eventDate := now.Add(72 * time.Hour)
	if _, err := tx.Exec(ctx, `INSERT INTO events(id,title,description,category,venue,location,date,status) VALUES($1,'Stripe TEST synthetic sale','Isolated one-sale fixture','test','Isolated venue','Isolated location',$2,'active')`, f.EventID, eventDate); err != nil {
		return errors.New("insert isolated Event")
	}
	if _, err := tx.Exec(ctx, `INSERT INTO ticket_tiers(id,event_id,name,price_cents,total_quantity,available_quantity,version,lifecycle_accounting_baseline) VALUES($1,$2,'Stripe TEST $1',100,1,1,1,'verified')`, f.TierID, f.EventID); err != nil {
		return errors.New("insert isolated $1 ticket tier")
	}
	if _, err := tx.Exec(ctx, `INSERT INTO seats(id,event_id,ticket_tier_id,status,booking_id,order_id,position) VALUES($1,$2,$3,'available',NULL,$4,'{}'::jsonb)`, f.SeatID, f.EventID, f.TierID, uuid.New()); err != nil {
		return errors.New("insert isolated available seat")
	}
	return nil
}
