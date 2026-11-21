//go:build admission_r4

package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	sagasteps "github.com/ticketbox/pkg/saga"
	"github.com/ticketbox/saga/internal/domain"
	"golang.org/x/crypto/bcrypt"
)

const (
	fixtureAck         = "ISOLATED_SYNTHETIC_ONLY"
	fixtureProvider    = "acct_r4_local"
	fixtureAmountCents = int64(100)
	fixtureCurrency    = "usd"
	fixtureGeneration  = int64(1)
)

var serviceSpecs = []serviceSpec{
	{name: "user", dsnEnv: "R4_USER_DATABASE_URL", migration: 4},
	{name: "event", dsnEnv: "R4_EVENT_DATABASE_URL", migration: 9},
	{name: "saga", dsnEnv: "R4_SAGA_DATABASE_URL", migration: 202611030001},
	{name: "booking", dsnEnv: "R4_BOOKING_DATABASE_URL", migration: 13},
}

var fixtureCaseKeys = []string{"normal", "refund_first", "admission_first", "unknown_recovery"}

type serviceSpec struct {
	name      string
	dsnEnv    string
	migration int64
}

type runConfig struct {
	RunID          uuid.UUID
	DatabaseSuffix string
	OutputPath     string
	Targets        map[string]databaseTarget
}

type databaseTarget struct {
	Service   string
	Database  string
	User      string
	DSN       string
	Migration int64
	Pool      *pgxpool.Pool
}

type fixtureCase struct {
	Key                     string    `json:"key"`
	UserID                  uuid.UUID `json:"user_id"`
	UserEmail               string    `json:"user_email"`
	UserPassword            string    `json:"user_password"`
	StaffUserID             uuid.UUID `json:"staff_user_id"`
	EventID                 uuid.UUID `json:"event_id"`
	SagaID                  uuid.UUID `json:"saga_id"`
	BookingID               uuid.UUID `json:"booking_id"`
	BookingItemID           uuid.UUID `json:"booking_item_id"`
	PaymentID               uuid.UUID `json:"payment_id"`
	PaymentIntentID         string    `json:"payment_intent_id"`
	SeatID                  uuid.UUID `json:"seat_id"`
	TierID                  uuid.UUID `json:"tier_id"`
	TicketID                uuid.UUID `json:"ticket_id"`
	TicketCredential        string    `json:"ticket_credential"`
	TicketCredentialSHA256  string    `json:"ticket_credential_sha256"`
	TicketCredentialExpires time.Time `json:"ticket_credential_expires_at"`
	AmountCents             int64     `json:"amount_cents"`
	Currency                string    `json:"currency"`
	BookingMode             string    `json:"booking_mode"`
	ProviderAccount         string    `json:"provider_account"`
	ReservationDeadline     time.Time `json:"reservation_deadline"`
}

type fixtureManifest struct {
	SchemaVersion  int           `json:"schema_version"`
	FixtureKind    string        `json:"fixture_kind"`
	RunID          uuid.UUID     `json:"run_id"`
	DatabaseSuffix string        `json:"database_suffix"`
	Provider       string        `json:"provider_account"`
	AmountCents    int64         `json:"amount_cents"`
	Currency       string        `json:"currency"`
	CreatedAt      time.Time     `json:"created_at"`
	Cases          []fixtureCase `json:"cases"`
}

func loadConfig(getenv func(string) string) (runConfig, error) {
	var config runConfig
	if getenv("R4_FIXTURE_ACK") != fixtureAck {
		return config, errors.New("R4_FIXTURE_ACK must equal ISOLATED_SYNTHETIC_ONLY")
	}
	runIDText := getenv("R4_RUN_ID")
	runID, err := uuid.Parse(runIDText)
	if err != nil || runIDText != runID.String() {
		return config, errors.New("R4_RUN_ID must be a canonical lowercase UUID")
	}
	suffix := strings.ReplaceAll(runID.String(), "-", "")[:12]
	outputPath := getenv("R4_FIXTURE_OUTPUT")
	if outputPath == "" || !filepath.IsAbs(outputPath) {
		return config, errors.New("R4_FIXTURE_OUTPUT must be an absolute private manifest path")
	}
	config.RunID, config.DatabaseSuffix, config.OutputPath = runID, suffix, outputPath
	config.Targets = make(map[string]databaseTarget, len(serviceSpecs))
	for _, spec := range serviceSpecs {
		target, err := parseDatabaseTarget(spec.name, getenv(spec.dsnEnv), suffix)
		if err != nil {
			return runConfig{}, fmt.Errorf("%s database target rejected: %w", spec.name, err)
		}
		target.Migration = spec.migration
		config.Targets[spec.name] = target
	}
	return config, nil
}

func parseDatabaseTarget(service, dsn, suffix string) (databaseTarget, error) {
	var target databaseTarget
	if dsn == "" {
		return target, errors.New("database URL is required")
	}
	var expectedHost string
	switch service {
	case "user", "event", "saga", "booking":
		expectedHost = service + "-db"
	default:
		return target, errors.New("unknown service database")
	}
	expectedIdentity := "r4_" + service + "_" + suffix
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		return target, errors.New("database URL is not a valid PostgreSQL connection string")
	}
	fallbacksSafe := true
	for _, fallback := range parsed.Fallbacks {
		if fallback.Host != expectedHost || fallback.Port != 5432 {
			fallbacksSafe = false
			break
		}
	}
	if parsed.Host != expectedHost || !fallbacksSafe || parsed.Port != 5432 || parsed.Database != expectedIdentity || parsed.User != expectedIdentity {
		return target, fmt.Errorf("requires host %s:5432 and matching database/user %s", expectedHost, expectedIdentity)
	}
	for key := range parsed.RuntimeParams {
		if key == "search_path" || key == "options" {
			return target, errors.New("database URL cannot override PostgreSQL schema search path")
		}
	}
	return databaseTarget{Service: service, Database: expectedIdentity, User: expectedIdentity, DSN: dsn}, nil
}

func reserveManifestOutput(path string) (*os.File, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("manifest output path must be absolute")
	}
	dir := filepath.Dir(path)
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil, errors.New("private manifest directory must already exist")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, errors.New("private manifest path must be new and writable")
	}
	return file, nil
}

func writeManifest(file *os.File, manifest fixtureManifest) error {
	if err := file.Chmod(0o600); err != nil {
		return errors.New("could not restrict private manifest permissions")
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(manifest); err != nil {
		return errors.New("could not write private fixture manifest")
	}
	if err := file.Sync(); err != nil {
		return errors.New("could not persist private fixture manifest")
	}
	return nil
}

func newManifest(config runConfig, cases []fixtureCase, now time.Time) fixtureManifest {
	return fixtureManifest{
		SchemaVersion: 1, FixtureKind: "admission_r4", RunID: config.RunID, DatabaseSuffix: config.DatabaseSuffix,
		Provider: fixtureProvider, AmountCents: fixtureAmountCents, Currency: fixtureCurrency,
		CreatedAt: now, Cases: cases,
	}
}

func makeFixtureCases(suffix string, now time.Time) ([]fixtureCase, error) {
	cases := make([]fixtureCase, 0, len(fixtureCaseKeys))
	for _, key := range fixtureCaseKeys {
		userPassword, err := randomRawURLSecret()
		if err != nil {
			return nil, errors.New("could not generate synthetic user password")
		}
		ticketCredential, err := randomRawURLSecret()
		if err != nil {
			return nil, errors.New("could not generate synthetic Ticket credential")
		}
		credentialDigest := sha256.Sum256([]byte(ticketCredential))
		userID, eventID, sagaID, bookingID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		paymentID, seatID, tierID, ticketID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		paymentIntentSuffix := strings.ReplaceAll(paymentID.String(), "-", "")
		cases = append(cases, fixtureCase{
			Key: key, UserID: userID, UserEmail: "r4." + suffix + "." + key + "@example.test", UserPassword: userPassword,
			StaffUserID: userID, EventID: eventID, SagaID: sagaID, BookingID: bookingID, BookingItemID: uuid.New(),
			PaymentID: paymentID, PaymentIntentID: "pi_r4_" + paymentIntentSuffix, SeatID: seatID, TierID: tierID, TicketID: ticketID,
			TicketCredential: ticketCredential, TicketCredentialSHA256: hex.EncodeToString(credentialDigest[:]),
			TicketCredentialExpires: now.Add(48 * time.Hour).Truncate(time.Microsecond),
			AmountCents:             fixtureAmountCents, Currency: fixtureCurrency, BookingMode: "pessimistic", ProviderAccount: fixtureProvider,
			ReservationDeadline: now.Add(24 * time.Hour).Truncate(time.Microsecond),
		})
	}
	return cases, nil
}

func randomRawURLSecret() (string, error) {
	var raw [32]byte
	if _, err := io.ReadFull(rand.Reader, raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func openTargets(ctx context.Context, config runConfig) ([]*databaseTarget, error) {
	targets := make([]*databaseTarget, 0, len(serviceSpecs))
	for _, spec := range serviceSpecs {
		target := config.Targets[spec.name]
		poolConfig, err := pgxpool.ParseConfig(target.DSN)
		if err != nil {
			closeTargets(targets)
			return nil, fmt.Errorf("parse isolated %s database connection", spec.name)
		}
		poolConfig.MaxConns = 2
		pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			closeTargets(targets)
			return nil, fmt.Errorf("open isolated %s database connection", spec.name)
		}
		target.Pool = pool
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			closeTargets(targets)
			return nil, fmt.Errorf("connect to isolated %s database", spec.name)
		}
		targets = append(targets, &target)
	}
	return targets, nil
}

func closeTargets(targets []*databaseTarget) {
	for _, target := range targets {
		if target != nil && target.Pool != nil {
			target.Pool.Close()
		}
	}
}

func validateTarget(ctx context.Context, target *databaseTarget) error {
	var database, user string
	if err := target.Pool.QueryRow(ctx, `SELECT current_database(), current_user`).Scan(&database, &user); err != nil {
		return fmt.Errorf("read isolated %s database identity", target.Service)
	}
	if database != target.Database || user != target.User {
		return fmt.Errorf("connected %s identity does not match its R4 database/user", target.Service)
	}
	var version int64
	var dirty bool
	if err := target.Pool.QueryRow(ctx, `SELECT version, dirty FROM public.schema_migrations`).Scan(&version, &dirty); err != nil {
		return fmt.Errorf("read %s migration version; fully migrated isolated database is required", target.Service)
	}
	if dirty || version != target.Migration {
		return fmt.Errorf("%s migration version must be clean at %d", target.Service, target.Migration)
	}
	rows, err := target.Pool.Query(ctx, `SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public' ORDER BY tablename`)
	if err != nil {
		return fmt.Errorf("list %s target tables", target.Service)
	}
	tables := make([]string, 0, 32)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("read %s target table list", target.Service)
		}
		tables = append(tables, name)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return fmt.Errorf("read %s target table list", target.Service)
	}
	seen := make(map[string]bool, len(tables))
	for _, table := range tables {
		seen[table] = true
	}
	for _, required := range requiredTables[target.Service] {
		if !seen[required] {
			return fmt.Errorf("%s database is missing migrated target table %s", target.Service, required)
		}
	}
	for _, table := range tables {
		if table == "schema_migrations" {
			continue
		}
		if target.Service == "booking" && table == "booking_admission_mode" {
			var count int64
			var inactive *bool
			if err := target.Pool.QueryRow(ctx, `SELECT count(*), bool_and(NOT active AND activated_at IS NULL) FROM public.booking_admission_mode`).Scan(&count, &inactive); err != nil {
				return errors.New("read Booking admission mode")
			}
			if count != 1 || inactive == nil || !*inactive {
				return errors.New("Booking admission mode must have its single inactive migration row")
			}
			continue
		}
		if target.Service == "booking" && table == "presentation_feature_flags" {
			var count int64
			var pristine *bool
			if err := target.Pool.QueryRow(ctx, `SELECT count(*), bool_and(key='discovery_advanced_filters' AND NOT enabled AND version=0 AND updated_at IS NULL AND actor_id IS NULL) FROM public.presentation_feature_flags`).Scan(&count, &pristine); err != nil {
				return errors.New("read initial presentation feature flag")
			}
			if count != 1 || pristine == nil || !*pristine {
				return errors.New("presentation feature flag must have its single unchanged migration row")
			}
			continue
		}
		var count int64
		query := "SELECT count(*) FROM " + pgx.Identifier{"public", table}.Sanitize()
		if err := target.Pool.QueryRow(ctx, query).Scan(&count); err != nil {
			return fmt.Errorf("check empty %s target table %s", target.Service, table)
		}
		if count != 0 {
			return fmt.Errorf("%s target table %s is not empty", target.Service, table)
		}
	}
	return nil
}

var requiredTables = map[string][]string{
	"user":    {"users"},
	"event":   {"events", "ticket_tiers", "seats", "booking_seat_lifecycles"},
	"saga":    {"sagas", "saga_steps"},
	"booking": {"bookings", "booking_items", "tickets", "ticket_credentials", "event_scanner_assignments", "booking_admission_mode"},
}

func beginTransactions(ctx context.Context, targets []*databaseTarget) (map[string]pgx.Tx, error) {
	transactions := make(map[string]pgx.Tx, len(targets))
	for _, target := range targets {
		tx, err := target.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			rollbackTransactions(transactions)
			return nil, fmt.Errorf("begin isolated %s fixture transaction", target.Service)
		}
		transactions[target.Service] = tx
	}
	return transactions, nil
}

func rollbackTransactions(transactions map[string]pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, tx := range transactions {
		if tx != nil {
			_ = tx.Rollback(ctx)
		}
	}
}

func seedUser(ctx context.Context, tx pgx.Tx, cases []fixtureCase) error {
	for _, fixture := range cases {
		passwordHash, err := bcrypt.GenerateFromPassword([]byte(fixture.UserPassword), bcrypt.DefaultCost)
		if err != nil {
			return errors.New("hash synthetic User password")
		}
		if _, err := tx.Exec(ctx, `INSERT INTO users(id,email,email_canonical,email_verified_at,password_hash,name,role)
			VALUES($1,$2,$2,clock_timestamp(),$3,$4,'user')`, fixture.UserID, fixture.UserEmail, string(passwordHash), "R4 synthetic "+fixture.Key); err != nil {
			return errors.New("seed isolated User login account")
		}
	}
	return nil
}

func seedEvent(ctx context.Context, tx pgx.Tx, cases []fixtureCase, now time.Time) error {
	for _, fixture := range cases {
		if _, err := tx.Exec(ctx, `INSERT INTO events(id,title,description,category,venue,location,date,status,publication_status,sales_status)
			VALUES($1,$2,$3,'r4_fixture','r4 isolated venue','r4 isolated location',$4,'active','published','selling')`,
			fixture.EventID, "R4 synthetic "+fixture.Key, "Isolated synthetic Admission fixture", now.Add(72*time.Hour)); err != nil {
			return errors.New("seed isolated Event")
		}
		if _, err := tx.Exec(ctx, `INSERT INTO ticket_tiers(id,event_id,name,price_cents,total_quantity,available_quantity,version,lifecycle_accounting_baseline)
			VALUES($1,$2,'R4 synthetic tier',$3,1,0,2,'verified')`, fixture.TierID, fixture.EventID, fixture.AmountCents); err != nil {
			return errors.New("seed isolated Event tier")
		}
		if _, err := tx.Exec(ctx, `INSERT INTO seats(id,event_id,ticket_tier_id,status,booking_id,order_id,position,reservation_expired_at,reserved_by_booking_id,lifecycle_generation,seat_revision)
			VALUES($1,$2,$3,'booked',$4,$5,'{}'::jsonb,NULL,NULL,1,3)`, fixture.SeatID, fixture.EventID, fixture.TierID, fixture.BookingID, uuid.New()); err != nil {
			return errors.New("seed isolated booked Event seat")
		}
		seatHash, err := eventSeatSetFingerprint(fixture.EventID, []uuid.UUID{fixture.SeatID})
		if err != nil {
			return errors.New("construct Event seat authority hash")
		}
		debits, err := json.Marshal(map[string]int32{fixture.TierID.String(): 1})
		if err != nil {
			return errors.New("construct Event lifecycle debit evidence")
		}
		if _, err := tx.Exec(ctx, `INSERT INTO booking_seat_lifecycles(
			booking_id,generation,event_id,seat_ids,seat_set_hash,reservation_deadline,state,accounting_state,last_operation_id,debits,accounted_at)
			VALUES($1,1,$2,$3,$4,$5,'booked','applied',$6,$7::jsonb,$8)`,
			fixture.BookingID, fixture.EventID, []uuid.UUID{fixture.SeatID}, seatHash[:], fixture.ReservationDeadline, uuid.New(), debits, now); err != nil {
			return errors.New("seed applied Event seat lifecycle")
		}
	}
	return nil
}

func seedSaga(ctx context.Context, tx pgx.Tx, cases []fixtureCase, now time.Time) error {
	stepNames := []sagasteps.SagaStepName{
		sagasteps.RESERVED_SEAT_STEP,
		sagasteps.CREATE_PAYMENT_INTENT_STEP,
		sagasteps.UPDATE_SEAT_BOOKED,
		sagasteps.UPDATE_BOOKING_STATUS_CONFIRMED,
		sagasteps.UPDATE_AVAILABLE_SEAT_NUMBER,
	}
	for _, fixture := range cases {
		snapshot, fingerprint, err := canonicalSagaSnapshot(fixture)
		if err != nil {
			return fmt.Errorf("construct canonical Saga input for case %s", fixture.Key)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO sagas(id,booking_id,name,status,current_step_index,payment_intent_id,payment_id,
			recovery_managed,lifecycle_generation,input_snapshot,input_fingerprint,recovery_state,direction,next_attempt_at)
			VALUES($1,$2,'ORDER_SAGA','COMPLETED',5,$3,$4,true,1,$5::jsonb,$6,'ready','forward',NULL)`,
			fixture.SagaID, fixture.BookingID, fixture.PaymentIntentID, fixture.PaymentID, snapshot, fingerprint[:]); err != nil {
			return errors.New("seed completed managed Saga")
		}
		for order, name := range stepNames {
			if _, err := tx.Exec(ctx, `INSERT INTO saga_steps(id,saga_id,name,status,"order",should_pause_for_payment,executed_at,
				recovery_managed,execute_state,compensate_state)
				VALUES($1,$2,$3,'COMPLETED',$4,$5,$6,true,'completed','pending')`,
				uuid.New(), fixture.SagaID, string(name), order, order == 1, now); err != nil {
				return errors.New("seed completed managed Saga step")
			}
		}
	}
	return nil
}

func seedBooking(ctx context.Context, tx pgx.Tx, cases []fixtureCase, now time.Time) error {
	for _, fixture := range cases {
		confirmationClaim := bookingDecisionID(fixture.BookingID, "claim_confirmation")
		confirmation := bookingDecisionID(fixture.BookingID, "confirm")
		if _, err := tx.Exec(ctx, `INSERT INTO bookings(id,user_id,event_id,status,total_amount_cents,version,managed,lifecycle_generation,
			lifecycle_phase,decision_kind,decision_reason,decision_id,confirmation_decision_id,currency,booking_mode,reservation_deadline,saga_id,
			dispatch_kind,dispatch_state,dispatch_due_at,dispatch_version)
			VALUES($1,$2,$3,'CONFIRMED',$4,3,true,1,'TERMINAL','confirm','payment_succeeded',$5,$6,$7,$8,$9,$10,'start','accepted',$11,3)`,
			fixture.BookingID, fixture.UserID, fixture.EventID, fixture.AmountCents, confirmation, confirmationClaim,
			fixture.Currency, fixture.BookingMode, fixture.ReservationDeadline, fixture.SagaID, now); err != nil {
			return errors.New("seed confirmed managed Booking")
		}
		if _, err := tx.Exec(ctx, `INSERT INTO booking_items(id,booking_id,ticket_tier_id,quantity,unit_price_cents,seat_ids)
			VALUES($1,$2,$3,1,$4,$5)`, fixture.BookingItemID, fixture.BookingID, fixture.TierID, fixture.AmountCents, []uuid.UUID{fixture.SeatID}); err != nil {
			return errors.New("seed isolated Booking item")
		}
		if _, err := tx.Exec(ctx, `INSERT INTO tickets(id,booking_id,booking_item_id,saga_id,payment_id,event_id,ticket_tier_id,seat_id,lifecycle_generation,status)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,1,'ISSUED')`, fixture.TicketID, fixture.BookingID, fixture.BookingItemID,
			fixture.SagaID, fixture.PaymentID, fixture.EventID, fixture.TierID, fixture.SeatID); err != nil {
			return errors.New("seed ISSUED Booking Ticket")
		}
		credentialDigest := sha256.Sum256([]byte(fixture.TicketCredential))
		if hex.EncodeToString(credentialDigest[:]) != fixture.TicketCredentialSHA256 {
			return fmt.Errorf("case %s Ticket credential digest changed while seeding", fixture.Key)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO ticket_credentials(ticket_id,token_digest,expires_at,credential_version)
			VALUES($1,$2,$3,1)`, fixture.TicketID, credentialDigest[:], fixture.TicketCredentialExpires); err != nil {
			return errors.New("seed isolated Ticket credential digest")
		}
		if _, err := tx.Exec(ctx, `INSERT INTO event_scanner_assignments(event_id,staff_user_id,active,granted_by,granted_at)
			VALUES($1,$2,true,$2,$3)`, fixture.EventID, fixture.StaffUserID, now); err != nil {
			return errors.New("seed active isolated Event scanner assignment")
		}
	}
	return nil
}

func canonicalSagaSnapshot(fixture fixtureCase) ([]byte, [32]byte, error) {
	input, snapshot, err := domain.CanonicalInput(domain.SagaInput{
		BookingID: fixture.BookingID, UserID: fixture.UserID, EventID: fixture.EventID, Generation: fixtureGeneration,
		SeatIDs: []uuid.UUID{fixture.SeatID}, AmountCents: fixture.AmountCents, Currency: fixture.Currency,
		BookingMode: fixture.BookingMode, ReservationDeadline: fixture.ReservationDeadline,
	})
	if err != nil {
		return nil, [32]byte{}, err
	}
	return snapshot, input.Fingerprint, nil
}

func eventSeatSetFingerprint(eventID uuid.UUID, seatIDs []uuid.UUID) ([32]byte, error) {
	// Keep the versioned payload aligned with Event's SeatSetFingerprint contract.
	if eventID == uuid.Nil || len(seatIDs) == 0 {
		return [32]byte{}, errors.New("event and seat IDs are required")
	}
	ids := make([]string, len(seatIDs))
	for index, seatID := range seatIDs {
		if seatID == uuid.Nil {
			return [32]byte{}, errors.New("empty seat ID")
		}
		ids[index] = seatID.String()
	}
	sort.Strings(ids)
	for index := 1; index < len(ids); index++ {
		if ids[index] == ids[index-1] {
			return [32]byte{}, errors.New("duplicate seat ID")
		}
	}
	return sha256.Sum256([]byte("v1\n" + eventID.String() + "\n" + strings.Join(ids, ",") + "\n")), nil
}

func bookingDecisionID(bookingID uuid.UUID, kind string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+bookingID.String()+":g1:"+kind))
}
