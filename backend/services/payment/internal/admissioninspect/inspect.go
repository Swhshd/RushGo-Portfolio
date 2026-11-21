package admissioninspect

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultTotalTimeout     = 10 * time.Second
	maximumTotalTimeout     = 15 * time.Second
	defaultStatementTimeout = 2 * time.Second
	maximumStatementTimeout = 3 * time.Second
)

// Inspect reads each selected service database in its own read-only,
// repeatable-read transaction. It never enumerates coordination identities.
func Inspect(parent context.Context, options Options) Report {
	if parent == nil {
		parent = context.Background()
	}
	totalTimeout := boundedDuration(options.TotalTimeout, defaultTotalTimeout, maximumTotalTimeout)
	statementTimeout := boundedDuration(options.StatementTimeout, defaultStatementTimeout, maximumStatementTimeout)
	ctx, cancel := context.WithTimeout(parent, totalTimeout)
	defer cancel()

	report := Report{
		Status:                      "available",
		SourceVersion:               options.SourceVersion,
		TotalTimeoutMillis:          totalTimeout.Milliseconds(),
		StatementTimeoutMillis:      statementTimeout.Milliseconds(),
		CrossDatabaseAtomicSnapshot: false,
		SnapshotNote:                "Payment and Booking are observed in independent read-only repeatable-read transactions; observed_at values are not a cross-database atomic snapshot.",
	}
	if options.Source == "" {
		options.Source = SourceBoth
	}
	if options.Source != SourceBoth && options.Source != SourcePayment && options.Source != SourceBooking {
		report.Status = "unavailable"
		report.SnapshotNote = "No database query was run because the source selection is invalid."
		return report
	}

	type result struct {
		source string
		db     DatabaseReport
	}
	selected := 0
	failed := 0
	observed := 0
	results := make(chan result, 2)
	if options.Source == SourceBoth || options.Source == SourcePayment {
		selected++
		go func() {
			results <- result{source: SourcePayment, db: inspectDatabase(ctx, options.PaymentDSN, SourcePayment, paymentAdmissionMigrationVersion, statementTimeout)}
		}()
	}
	if options.Source == SourceBoth || options.Source == SourceBooking {
		selected++
		go func() {
			results <- result{source: SourceBooking, db: inspectDatabase(ctx, options.BookingDSN, SourceBooking, bookingAdmissionMigrationVersion, statementTimeout)}
		}()
	}
	for range selected {
		result := <-results
		if result.source == SourcePayment {
			report.Payment = &result.db
		} else {
			report.Booking = &result.db
		}
		if result.db.Identity != nil {
			observed++
		}
		if result.db.Status != "available" {
			failed++
		}
	}
	if failed > 0 {
		report.Status = "unavailable"
		if observed > 0 {
			report.Status = "partial"
		}
	}
	return report
}

func inspectDatabase(ctx context.Context, dsn, source string, migrationTarget int64, statementTimeout time.Duration) DatabaseReport {
	report := DatabaseReport{
		Status:    "unavailable",
		Migration: MigrationReport{Status: "not_queried", RequiredVersion: migrationTarget},
		Mode:      ModeReport{Status: "not_queried"},
		DataScope: dataScope(source),
	}
	if dsn == "" {
		return fail(&report, "configuration", "configuration_missing", nil)
	}
	if ctx.Err() != nil {
		return fail(&report, "connect", failureCategory(ctx, ctx.Err()), ctx.Err())
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fail(&report, "connect", "configuration_invalid", err)
	}
	config.MaxConns = 1
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fail(&report, "connect", failureCategory(ctx, err), err)
	}
	defer pool.Close()

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fail(&report, "begin_read_only_transaction", failureCategory(ctx, err), err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	report.TransactionMode = "READ ONLY REPEATABLE READ"

	var timeoutSetting string
	if err := tx.QueryRow(ctx, `SELECT set_config('statement_timeout',$1,true)`, durationSetting(statementTimeout)).Scan(&timeoutSetting); err != nil {
		return fail(&report, "set_statement_timeout", failureCategory(ctx, err), err)
	}
	meta, err := readDatabaseMetadata(ctx, tx)
	if err != nil {
		return fail(&report, "database_identity", failureCategory(ctx, err), err)
	}
	report.Identity = &meta.identity
	report.ObservedAt = &meta.observedAt

	if err := readMigration(ctx, tx, meta.schemaMigrationInstalled, migrationTarget, &report.Migration); err != nil {
		report.Migration.Status = "unavailable"
		return fail(&report, "migration_version", failureCategory(ctx, err), err)
	}

	var modeActive *bool
	modeInstalled := false
	if source == SourcePayment {
		modeInstalled = meta.paymentModeInstalled
	} else {
		modeInstalled = meta.bookingModeInstalled
	}
	if !modeInstalled {
		report.Mode = ModeReport{Status: "not_installed"}
	} else {
		active, found, err := readMode(ctx, tx, source)
		if err != nil {
			report.Mode = ModeReport{Status: "unavailable"}
			return fail(&report, "admission_mode", failureCategory(ctx, err), err)
		}
		if !found {
			report.Mode = ModeReport{Status: "row_missing"}
		} else {
			report.Mode = ModeReport{Status: "available", Active: &active}
			modeActive = &active
		}
	}

	if source == SourcePayment {
		if !meta.paymentPermitsInstalled {
			report.Admission = notInstalledAdmission(false)
		} else {
			report.Admission = pendingAdmission(false)
			if err := readPaymentAdmission(ctx, tx, *report.ObservedAt, report.Admission); err != nil {
				markAdmissionUnavailable(report.Admission, failureCategory(ctx, err))
				return fail(&report, "payment_admission_aggregate", failureCategory(ctx, err), err)
			}
		}
		if modeActive == nil {
			metric := missingCount("unknown_mode")
			report.RefundQueueCandidatesWithPendingAdmission = &metric
		} else if !*modeActive {
			metric := availableCount(0)
			report.RefundQueueCandidatesWithPendingAdmission = &metric
		} else if !meta.paymentPermitsInstalled || !meta.paymentsInstalled {
			metric := missingCount("not_installed")
			report.RefundQueueCandidatesWithPendingAdmission = &metric
		} else {
			metric := CountMetric{Status: "not_queried"}
			report.RefundQueueCandidatesWithPendingAdmission = &metric
			count, err := readRefundCandidates(ctx, tx, *report.ObservedAt)
			if err != nil {
				metric = unavailableCount(failureCategory(ctx, err))
				report.RefundQueueCandidatesWithPendingAdmission = &metric
				return fail(&report, "refund_candidate_aggregate", failureCategory(ctx, err), err)
			}
			metric = availableCount(count)
			report.RefundQueueCandidatesWithPendingAdmission = &metric
		}
	} else {
		if !meta.bookingIntentsInstalled {
			report.Admission = notInstalledAdmission(true)
		} else {
			report.Admission = pendingAdmission(true)
			if err := readBookingAdmission(ctx, tx, *report.ObservedAt, report.Admission); err != nil {
				markAdmissionUnavailable(report.Admission, failureCategory(ctx, err))
				return fail(&report, "booking_admission_aggregate", failureCategory(ctx, err), err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fail(&report, "commit_read_only_transaction", failureCategory(ctx, err), err)
	}
	report.Status = "available"
	return report
}

type databaseMetadata struct {
	identity                 DatabaseIdentity
	observedAt               time.Time
	schemaMigrationInstalled bool
	paymentModeInstalled     bool
	paymentPermitsInstalled  bool
	paymentsInstalled        bool
	bookingModeInstalled     bool
	bookingIntentsInstalled  bool
}

func readDatabaseMetadata(ctx context.Context, tx pgx.Tx) (databaseMetadata, error) {
	var meta databaseMetadata
	var serverAddress *string
	var serverPort *int32
	err := tx.QueryRow(ctx, `SELECT current_database(),COALESCE(current_schema(),''),
		inet_server_addr()::text,inet_server_port(),transaction_timestamp(),
		to_regclass('schema_migrations') IS NOT NULL,
		to_regclass('payment_admission_mode') IS NOT NULL,
		to_regclass('payment_admission_permits') IS NOT NULL,
		to_regclass('payments') IS NOT NULL,
		to_regclass('booking_admission_mode') IS NOT NULL,
		to_regclass('ticket_admission_intents') IS NOT NULL`).Scan(
		&meta.identity.DatabaseName, &meta.identity.SchemaName, &serverAddress, &serverPort, &meta.observedAt,
		&meta.schemaMigrationInstalled, &meta.paymentModeInstalled, &meta.paymentPermitsInstalled,
		&meta.paymentsInstalled, &meta.bookingModeInstalled, &meta.bookingIntentsInstalled)
	if err != nil {
		return databaseMetadata{}, err
	}
	meta.identity.ServerAddress = serverAddress
	meta.identity.ServerPort = serverPort
	return meta, nil
}

func readMigration(ctx context.Context, tx pgx.Tx, installed bool, target int64, report *MigrationReport) error {
	if !installed {
		report.Status = migrationStatus(false, nil, nil, target)
		return nil
	}
	var version int64
	var dirty bool
	err := tx.QueryRow(ctx, `SELECT version,dirty FROM schema_migrations ORDER BY version DESC LIMIT 1`).Scan(&version, &dirty)
	if errors.Is(err, pgx.ErrNoRows) {
		report.Status = migrationStatus(true, nil, nil, target)
		return nil
	}
	if err != nil {
		return err
	}
	report.Version = &version
	report.Dirty = &dirty
	report.Status = migrationStatus(true, &version, &dirty, target)
	return nil
}

func readMode(ctx context.Context, tx pgx.Tx, source string) (bool, bool, error) {
	table := "payment_admission_mode"
	if source == SourceBooking {
		table = "booking_admission_mode"
	}
	var active bool
	err := tx.QueryRow(ctx, `SELECT active FROM `+table+` WHERE singleton=TRUE`).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return active, true, nil
}

func readPaymentAdmission(ctx context.Context, tx pgx.Tx, observedAt time.Time, report *AdmissionReport) error {
	var pending, terminal, unknown, due int64
	var oldest *time.Time
	var bookingUnavailable, bookingOutcomeMismatch, bookingIntentPending, outcomeUnavailable, otherError int64
	err := tx.QueryRow(ctx, `SELECT
		COUNT(*) FILTER (WHERE state='PENDING'),
		COUNT(*) FILTER (WHERE state IN ('COMMITTED','ABORTED')),
		COUNT(*) FILTER (WHERE state NOT IN ('PENDING','COMMITTED','ABORTED')),
		COUNT(*) FILTER (WHERE state='PENDING' AND next_attempt_at<=$1),
		MIN(created_at) FILTER (WHERE state='PENDING'),
		COUNT(*) FILTER (WHERE state='PENDING' AND last_error='booking_unavailable'),
		COUNT(*) FILTER (WHERE state='PENDING' AND last_error='booking_outcome_mismatch'),
		COUNT(*) FILTER (WHERE state='PENDING' AND last_error='booking_intent_pending'),
		COUNT(*) FILTER (WHERE state='PENDING' AND last_error='outcome_unavailable'),
		COUNT(*) FILTER (WHERE state='PENDING' AND last_error IS NOT NULL AND last_error NOT IN
			('booking_unavailable','booking_outcome_mismatch','booking_intent_pending','outcome_unavailable'))
		FROM payment_admission_permits`, observedAt).Scan(
		&pending, &terminal, &unknown, &due, &oldest,
		&bookingUnavailable, &bookingOutcomeMismatch, &bookingIntentPending, &outcomeUnavailable, &otherError)
	if err != nil {
		return err
	}
	report.Status = "available"
	report.PendingCount = availableCount(pending)
	report.TerminalCount = availableCount(terminal)
	report.UnknownStateCount = availableCount(unknown)
	report.RecoveryDueCount = availableCount(due)
	if oldest != nil {
		age := observedAt.Sub(*oldest).Seconds()
		report.OldestPendingCreatedAt = oldest
		report.OldestPendingAgeSeconds = &age
	}
	report.PendingErrorCategoryCounts = nonZeroCategories(map[string]int64{
		"booking_unavailable":      bookingUnavailable,
		"booking_outcome_mismatch": bookingOutcomeMismatch,
		"booking_intent_pending":   bookingIntentPending,
		"outcome_unavailable":      outcomeUnavailable,
		"other_unclassified_value": otherError,
	})
	return nil
}

func readBookingAdmission(ctx context.Context, tx pgx.Tx, observedAt time.Time, report *AdmissionReport) error {
	var pending, terminal, unknown int64
	var oldest *time.Time
	err := tx.QueryRow(ctx, `SELECT
		COUNT(*) FILTER (WHERE state='PENDING'),
		COUNT(*) FILTER (WHERE state IN ('COMMITTED','ABORTED')),
		COUNT(*) FILTER (WHERE state NOT IN ('PENDING','COMMITTED','ABORTED')),
		MIN(created_at) FILTER (WHERE state='PENDING')
		FROM ticket_admission_intents`).Scan(&pending, &terminal, &unknown, &oldest)
	if err != nil {
		return err
	}
	report.Status = "available"
	report.PendingCount = availableCount(pending)
	report.TerminalCount = availableCount(terminal)
	report.UnknownStateCount = availableCount(unknown)
	report.RecoveryDueCount = missingCount("not_persisted")
	if oldest != nil {
		age := observedAt.Sub(*oldest).Seconds()
		report.OldestPendingCreatedAt = oldest
		report.OldestPendingAgeSeconds = &age
	}
	return nil
}

func readRefundCandidates(ctx context.Context, tx pgx.Tx, observedAt time.Time) (int64, error) {
	var count int64
	err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM payments p
		WHERE p.managed
		AND p.refund_state IN ('pending','claimed','retryable','provider_pending','unknown')
		AND p.due_at<=$1
		AND (p.lease_until IS NULL OR p.lease_until<=$1)
		AND EXISTS (SELECT 1 FROM payment_admission_permits ap
			WHERE ap.payment_id=p.id AND ap.state='PENDING')`, observedAt).Scan(&count)
	return count, err
}

func pendingAdmission(booking bool) *AdmissionReport {
	recovery := CountMetric{Status: "not_queried"}
	if booking {
		recovery = missingCount("not_persisted")
	}
	return &AdmissionReport{
		Status:            "not_queried",
		PendingCount:      CountMetric{Status: "not_queried"},
		TerminalCount:     CountMetric{Status: "not_queried"},
		UnknownStateCount: CountMetric{Status: "not_queried"},
		RecoveryDueCount:  recovery,
	}
}

func notInstalledAdmission(booking bool) *AdmissionReport {
	recovery := missingCount("not_installed")
	if booking {
		recovery = missingCount("not_persisted")
	}
	return &AdmissionReport{
		Status:            "not_installed",
		PendingCount:      missingCount("not_installed"),
		TerminalCount:     missingCount("not_installed"),
		UnknownStateCount: missingCount("not_installed"),
		RecoveryDueCount:  recovery,
	}
}

func markAdmissionUnavailable(report *AdmissionReport, category string) {
	report.Status = "unavailable"
	report.PendingCount = unavailableCount(category)
	report.TerminalCount = unavailableCount(category)
	report.UnknownStateCount = unavailableCount(category)
	if report.RecoveryDueCount.Status != "not_persisted" {
		report.RecoveryDueCount = unavailableCount(category)
	}
	report.OldestPendingCreatedAt = nil
	report.OldestPendingAgeSeconds = nil
	report.PendingErrorCategoryCounts = nil
}

func dataScope(source string) []string {
	if source == SourcePayment {
		return []string{
			"payment_admission_mode: singleton active latch",
			"payment_admission_permits: state counts, due recovery count, oldest pending age, safe pending error category counts",
			"payments: due unleased managed refund queue candidates joined to pending permits; current candidates are not persisted blocker history",
			"schema_migrations: latest version and dirty flag",
			"pending last_error categories are stored values without a per-error timestamp, so their recency is unknown",
		}
	}
	return []string{
		"booking_admission_mode: singleton active latch",
		"ticket_admission_intents: state counts and oldest pending age",
		"schema_migrations: latest version and dirty flag",
	}
}

func nonZeroCategories(counts map[string]int64) map[string]int64 {
	result := make(map[string]int64)
	for category, count := range counts {
		if count > 0 {
			result[category] = count
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func fail(report *DatabaseReport, phase, category string, _ error) DatabaseReport {
	if report.Identity == nil {
		report.Status = "unavailable"
	} else {
		report.Status = "partial"
	}
	report.FailurePhase = phase
	report.FailureCategory = category
	return *report
}

func boundedDuration(value, fallback, maximum time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}
	if value < time.Millisecond {
		return time.Millisecond
	}
	if value > maximum {
		return maximum
	}
	return value
}

func durationSetting(value time.Duration) string {
	millis := value.Milliseconds()
	if millis < 1 {
		millis = 1
	}
	return fmt.Sprintf("%dms", millis)
}
