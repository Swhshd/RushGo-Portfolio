package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	bookingrepo "github.com/ticketbox/booking/internal/repository"
	bookingservice "github.com/ticketbox/booking/internal/service"
)

const (
	adminOutboxTestOwner  = "rushgo-admin-outbox-a970fdce"
	adminOutboxTestRunID  = "20261031T153529Z-a970fdce"
	adminOutboxTestDBName = "admin_outbox_a970fdce"
)

func TestAdminOutboxProjectionCapAndReadOnlyIntegration(t *testing.T) {
	adminPool, readerPool, repo := adminOutboxIntegrationFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	if _, err := readerPool.Exec(ctx, `UPDATE outbox SET published=published WHERE false`); err == nil {
		t.Fatal("outbox_reader accepted an UPDATE in its read-only test session")
	}

	for _, rowCount := range []int{4999, 5000, 5001} {
		t.Run(fmt.Sprintf("rows_%d", rowCount), func(t *testing.T) {
			if _, err := adminPool.Exec(ctx, `TRUNCATE outbox`); err != nil {
				t.Fatal(err)
			}
			if _, err := adminPool.Exec(ctx, `
				INSERT INTO outbox (id, event_type, event_key, payload, published, created_at)
				SELECT md5('admin-outbox:' || n::text)::uuid, 'BookingConfirmed', 'admin-outbox:' || n::text,
				       jsonb_build_object('synthetic', n), (n % 2 = 0), '2026-10-31T12:00:00Z'::timestamptz
			FROM generate_series(1, $1::int) AS n`, rowCount); err != nil {
				t.Fatal(err)
			}

			before := adminOutboxIntegrationSnapshot(t, ctx, adminPool)
			rows, err := repo.ReadAdminOutboxProjection(ctx)
			after := adminOutboxIntegrationSnapshot(t, ctx, adminPool)
			if after != before {
				t.Fatalf("outbox state changed during projection read: before=%s after=%s", before, after)
			}
			if rowCount <= bookingrepo.AdminOutboxProjectionCap {
				if err != nil {
					t.Fatalf("ReadAdminOutboxProjection() error = %v", err)
				}
				if len(rows) != rowCount {
					t.Fatalf("projection rows = %d, want %d", len(rows), rowCount)
				}
				return
			}
			if !errors.Is(err, bookingrepo.ErrAdminOutboxCapacityExceeded) {
				t.Fatalf("ReadAdminOutboxProjection() error = %v, want capacity exceeded", err)
			}
			if len(rows) != 0 {
				t.Fatalf("overflow returned a partial projection of %d rows", len(rows))
			}
		})
	}
}

func TestAdminOutboxRealRepositoryUsesUUIDByteKeysetForEqualTimestampsIntegration(t *testing.T) {
	adminPool, _, repo := adminOutboxIntegrationFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := adminPool.Exec(ctx, `TRUNCATE outbox`); err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 10, 31, 12, 0, 0, 123456000, time.UTC)
	ids := []string{
		"90000000-0000-4000-8000-000000000001",
		"90000000-0000-4000-8000-000000000003",
		"90000000-0000-4000-8000-000000000002",
	}
	for _, id := range ids {
		if _, err := adminPool.Exec(ctx, `INSERT INTO outbox(id,event_type,event_key,payload,published,created_at) VALUES($1,'BookingConfirmed',$2,'{}',true,$3)`, id, "admin-outbox-page:"+id, createdAt); err != nil {
			t.Fatal(err)
		}
	}
	query := bookingservice.AdminOutboxQuery{
		Source: "booking", Status: "all", From: "2026-10-31T00:00:00Z", To: "2026-11-02T00:00:00Z", Limit: 2,
	}
	outboxService := bookingservice.NewAdminOutboxService(repo)
	first, err := outboxService.List(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Records) != 2 || first.Records[0].EventID != "90000000-0000-4000-8000-000000000003" || first.Records[1].EventID != "90000000-0000-4000-8000-000000000002" {
		t.Fatalf("equal-time first page order = %#v", first.Records)
	}
	if !first.HasMore || first.NextCursor == "" {
		t.Fatalf("equal-time first page continuation = %v/%q", first.HasMore, first.NextCursor)
	}
	query.Cursor = first.NextCursor
	second, err := outboxService.List(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Records) != 1 || second.Records[0].EventID != "90000000-0000-4000-8000-000000000001" || second.HasMore || second.NextCursor != "" {
		t.Fatalf("equal-time second page = %#v, has_more=%v cursor=%q", second.Records, second.HasMore, second.NextCursor)
	}
}

func TestAdminOutboxLiveCursorReflectsConcurrentInsertAndStatusChangeIntegration(t *testing.T) {
	adminPool, _, repo := adminOutboxIntegrationFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := adminPool.Exec(ctx, `TRUNCATE outbox`); err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 10, 31, 12, 0, 0, 654321000, time.UTC)
	for n := 1; n <= 5; n++ {
		id := fmt.Sprintf("91000000-0000-4000-8000-%012d", n)
		if _, err := adminPool.Exec(ctx, `INSERT INTO outbox(id,event_type,event_key,payload,published,created_at) VALUES($1,'BookingConfirmed',$2,'{}',false,$3)`, id, "admin-outbox-live:"+id, createdAt); err != nil {
			t.Fatal(err)
		}
	}
	query := bookingservice.AdminOutboxQuery{
		Source: "booking", Status: "pending", From: "2026-10-31T00:00:00Z", To: "2026-11-02T00:00:00Z", Limit: 2,
	}
	outboxService := bookingservice.NewAdminOutboxService(repo)
	first, err := outboxService.List(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Records) != 2 || first.Records[0].EventID != "91000000-0000-4000-8000-000000000005" || first.Records[1].EventID != "91000000-0000-4000-8000-000000000004" || first.NextCursor == "" {
		t.Fatalf("initial live page = %#v has_more=%v cursor=%q", first.Records, first.HasMore, first.NextCursor)
	}
	if _, err := adminPool.Exec(ctx, `INSERT INTO outbox(id,event_type,event_key,payload,published,created_at) VALUES($1,'BookingConfirmed',$2,'{}',false,$3)`, "91000000-0000-4000-8000-000000000006", "admin-outbox-live:new", createdAt); err != nil {
		t.Fatal(err)
	}
	if _, err := adminPool.Exec(ctx, `UPDATE outbox SET published=true WHERE id=$1`, "91000000-0000-4000-8000-000000000003"); err != nil {
		t.Fatal(err)
	}
	query.Cursor = first.NextCursor
	second, err := outboxService.List(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Records) != 2 || second.Records[0].EventID != "91000000-0000-4000-8000-000000000002" || second.Records[1].EventID != "91000000-0000-4000-8000-000000000001" {
		t.Fatalf("live second page did not reflect the current filtered keyset: %#v", second.Records)
	}
	if second.HasMore || second.NextCursor != "" {
		t.Fatalf("live final page continuation = %v/%q", second.HasMore, second.NextCursor)
	}
}

func TestAdminOutboxLargeOwnedTablePlanAndBoundedOverflowIntegration(t *testing.T) {
	adminPool, readerPool, repo := adminOutboxIntegrationFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := adminPool.Exec(ctx, `TRUNCATE outbox`); err != nil {
		t.Fatal(err)
	}
	const rowCount = 50001
	if _, err := adminPool.Exec(ctx, `
		INSERT INTO outbox (id, event_type, event_key, payload, published, created_at)
		SELECT md5('admin-outbox-large:' || n::text)::uuid, 'BookingConfirmed', 'admin-outbox-large:' || n::text,
		       jsonb_build_object('synthetic', n), (n % 2 = 0), '2026-10-31T12:00:00Z'::timestamptz
		FROM generate_series(1, $1::int) AS n`, rowCount); err != nil {
		t.Fatal(err)
	}
	if _, err := adminPool.Exec(ctx, `ANALYZE outbox`); err != nil {
		t.Fatal(err)
	}
	before := adminOutboxIntegrationSnapshot(t, ctx, adminPool)
	rows, err := repo.ReadAdminOutboxProjection(ctx)
	after := adminOutboxIntegrationSnapshot(t, ctx, adminPool)
	if !errors.Is(err, bookingrepo.ErrAdminOutboxCapacityExceeded) || len(rows) != 0 {
		t.Fatalf("large projection result rows=%d err=%v, want fail-closed capacity", len(rows), err)
	}
	if before != after {
		t.Fatalf("large projection changed outbox or related tables: before=%s after=%s", before, after)
	}

	tx, err := readerPool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = '5s'`); err != nil {
		t.Fatal(err)
	}
	var rawPlan []byte
	if err := tx.QueryRow(ctx, `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
		SELECT id, event_type, published, created_at FROM outbox ORDER BY id LIMIT 5001`).Scan(&rawPlan); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var explain []map[string]any
	if err := json.Unmarshal(rawPlan, &explain); err != nil || len(explain) != 1 {
		t.Fatalf("could not decode EXPLAIN ANALYZE output: %v", err)
	}
	rootPlan, ok := explain[0]["Plan"].(map[string]any)
	if !ok || rootPlan["Node Type"] != "Limit" {
		t.Fatalf("query plan root = %#v, want Limit", explain)
	}
	actualRows, okRows := rootPlan["Actual Rows"].(float64)
	actualMs, okTime := rootPlan["Actual Total Time"].(float64)
	if !okRows || actualRows != bookingrepo.AdminOutboxProjectionCap+1 || !okTime || actualMs >= 5000 {
		t.Fatalf("query plan did not materialize exactly the 5001-row fail-closed bound within 5s: rows=%v time_ms=%v", rootPlan["Actual Rows"], rootPlan["Actual Total Time"])
	}
	if !adminOutboxPlanUsesPrimaryKeyIndex(rootPlan) {
		t.Fatalf("query plan did not use the outbox primary-key index: %s", rawPlan)
	}
	t.Logf("EXPLAIN ANALYZE returned at most %0.f materialized rows in %.3f ms under the 5s statement_timeout; plan=%s", actualRows, actualMs, rawPlan)
}

func adminOutboxPlanUsesPrimaryKeyIndex(node map[string]any) bool {
	if node["Node Type"] == "Index Scan" && node["Index Name"] == "outbox_pkey" {
		return true
	}
	children, ok := node["Plans"].([]any)
	if !ok {
		return false
	}
	for _, child := range children {
		if childNode, ok := child.(map[string]any); ok && adminOutboxPlanUsesPrimaryKeyIndex(childNode) {
			return true
		}
	}
	return false
}

type adminOutboxDockerContainerInspect struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	State struct {
		Running bool `json:"Running"`
	} `json:"State"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	HostConfig struct {
		PublishAllPorts bool `json:"PublishAllPorts"`
		PortBindings    map[string][]struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"PortBindings"`
	} `json:"HostConfig"`
	NetworkSettings struct {
		Ports map[string][]struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"Ports"`
		Networks map[string]struct {
			IPAddress string `json:"IPAddress"`
		} `json:"Networks"`
	} `json:"NetworkSettings"`
	Mounts []struct {
		Type        string `json:"Type"`
		Name        string `json:"Name"`
		Destination string `json:"Destination"`
	} `json:"Mounts"`
}

type adminOutboxDockerNetworkInspect struct {
	Name     string            `json:"Name"`
	Driver   string            `json:"Driver"`
	Labels   map[string]string `json:"Labels"`
	Internal bool              `json:"Internal"`
}

type adminOutboxDockerVolumeInspect struct {
	Name   string            `json:"Name"`
	Labels map[string]string `json:"Labels"`
}

func adminOutboxIntegrationFixture(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool, *bookingrepo.PostgresBookingRepository) {
	t.Helper()
	urlValue := os.Getenv("ADMIN_OUTBOX_TEST_DATABASE_URL")
	readonlyURL := os.Getenv("ADMIN_OUTBOX_TEST_READONLY_DATABASE_URL")
	containerID := os.Getenv("ADMIN_OUTBOX_TEST_CONTAINER_ID")
	engineID := os.Getenv("ADMIN_OUTBOX_TEST_ENGINE_ID")
	owner := os.Getenv("ADMIN_OUTBOX_TEST_OWNER")
	systemID := os.Getenv("ADMIN_OUTBOX_TEST_SYSTEM_ID")
	if urlValue == "" && readonlyURL == "" && containerID == "" && engineID == "" && owner == "" && systemID == "" {
		t.Skip("set the ADMIN_OUTBOX_TEST_* identity and database variables to run against the new owned PostgreSQL test system")
	}
	if urlValue == "" || readonlyURL == "" || containerID == "" || engineID == "" || owner == "" || systemID == "" {
		t.Fatal("BLOCKED: complete ADMIN_OUTBOX_TEST_* database and system identity is required")
	}
	if owner != adminOutboxTestOwner || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(containerID) || strings.TrimSpace(engineID) == "" || strings.TrimSpace(systemID) == "" {
		t.Fatal("BLOCKED: Admin outbox test system owner or identifiers do not match the frozen run")
	}
	adminURL, adminPort := requireAdminOutboxTestDatabaseURL(t, urlValue, "postgres", "")
	readerURL, readerPort := requireAdminOutboxTestDatabaseURL(t, readonlyURL, "outbox_reader", adminPort)
	if adminPort != readerPort {
		t.Fatal("BLOCKED: Admin and read-only URLs do not target the same loopback port")
	}
	verifyAdminOutboxDockerIdentity(t, containerID, engineID, adminPort)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	adminPool, err := pgxpool.New(ctx, adminURL.String())
	if err != nil {
		t.Fatal("BLOCKED: could not connect to the owned Admin outbox test PostgreSQL system")
	}
	readerPool, err := pgxpool.New(ctx, readerURL.String())
	if err != nil {
		adminPool.Close()
		t.Fatal("BLOCKED: could not connect with the Admin outbox read-only role")
	}
	t.Cleanup(adminPool.Close)
	t.Cleanup(readerPool.Close)

	var databaseName, adminUser, actualSystemID string
	if err := adminPool.QueryRow(ctx, `SELECT current_database(), current_user, system_identifier::text FROM pg_control_system()`).Scan(&databaseName, &adminUser, &actualSystemID); err != nil {
		t.Fatal("BLOCKED: could not verify the owned PostgreSQL database system identity")
	}
	if databaseName != adminOutboxTestDBName || adminUser != "postgres" || actualSystemID != systemID {
		t.Fatalf("BLOCKED: PostgreSQL identity mismatch (database=%q role=%q system_id_match=%v)", databaseName, adminUser, actualSystemID == systemID)
	}
	var readerUser, readOnly string
	if err := readerPool.QueryRow(ctx, `SELECT current_user, current_setting('default_transaction_read_only')`).Scan(&readerUser, &readOnly); err != nil {
		t.Fatal("BLOCKED: could not verify the read-only database role")
	}
	if readerUser != "outbox_reader" || readOnly != "on" {
		t.Fatalf("BLOCKED: reader role identity or transaction mode mismatch (role=%q default_read_only=%q)", readerUser, readOnly)
	}
	for _, column := range []string{"id", "event_type", "published", "created_at"} {
		var allowed bool
		if err := readerPool.QueryRow(ctx, `SELECT has_column_privilege(current_user, 'outbox', $1, 'SELECT')`, column).Scan(&allowed); err != nil || !allowed {
			t.Fatalf("BLOCKED: outbox_reader lacks required projection SELECT privilege on %q", column)
		}
	}
	for _, column := range []string{"event_key", "payload"} {
		var allowed bool
		if err := readerPool.QueryRow(ctx, `SELECT has_column_privilege(current_user, 'outbox', $1, 'SELECT')`, column).Scan(&allowed); err != nil || allowed {
			t.Fatalf("BLOCKED: outbox_reader has forbidden SELECT privilege on %q", column)
		}
	}
	var forbiddenPayload []byte
	if err := readerPool.QueryRow(ctx, `SELECT payload FROM outbox LIMIT 1`).Scan(&forbiddenPayload); err == nil {
		t.Fatal("BLOCKED: outbox_reader successfully selected the private payload column")
	}
	return adminPool, readerPool, bookingrepo.NewPostgresBookingRepository(readerPool)
}

func requireAdminOutboxTestDatabaseURL(t *testing.T, value, expectedUser, expectedPort string) (*url.URL, string) {
	t.Helper()
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "postgres" || parsed.Hostname() != "127.0.0.1" || parsed.Path != "/"+adminOutboxTestDBName || parsed.User == nil || parsed.User.Username() != expectedUser {
		t.Fatal("BLOCKED: Admin outbox test database URL has an unexpected scheme, host, database, or role")
	}
	if _, hasPassword := parsed.User.Password(); hasPassword {
		t.Fatal("BLOCKED: Admin outbox test system must not persist a database password")
	}
	if expectedPort != "" && parsed.Port() != expectedPort {
		t.Fatal("BLOCKED: Admin and read-only database URLs target different ports")
	}
	for _, key := range []string{"host", "hostaddr", "port", "database", "dbname", "service", "servicefile", "options"} {
		if _, exists := parsed.Query()[key]; exists {
			t.Fatal("BLOCKED: Admin outbox test database URL contains an override parameter")
		}
	}
	return parsed, parsed.Port()
}

func verifyAdminOutboxDockerIdentity(t *testing.T, containerID, engineID, hostPort string) {
	t.Helper()
	engineOutput, err := exec.Command("docker", "info", "--format", "{{.ID}}").Output()
	if err != nil || strings.TrimSpace(string(engineOutput)) != engineID {
		t.Fatal("BLOCKED: live Docker engine ID does not match ADMIN_OUTBOX_TEST_ENGINE_ID")
	}
	containerOutput, err := exec.Command("docker", "inspect", containerID).Output()
	if err != nil {
		t.Fatal("BLOCKED: live Docker container does not match ADMIN_OUTBOX_TEST_CONTAINER_ID")
	}
	var containers []adminOutboxDockerContainerInspect
	if json.Unmarshal(containerOutput, &containers) != nil || len(containers) != 1 {
		t.Fatal("BLOCKED: Admin outbox test container inspection was invalid")
	}
	container := containers[0]
	if container.ID != containerID || !container.State.Running || !strings.HasPrefix(strings.TrimPrefix(container.Name, "/"), adminOutboxTestOwner+"-postgres") ||
		container.Config.Labels["rushgo.owner"] != adminOutboxTestOwner || container.Config.Labels["rushgo.run_id"] != adminOutboxTestRunID {
		t.Fatal("BLOCKED: live PostgreSQL container owner, run, name, or running state mismatch")
	}
	if container.HostConfig.PublishAllPorts || len(container.HostConfig.PortBindings) != 1 || len(container.HostConfig.PortBindings["5432/tcp"]) != 1 {
		t.Fatal("BLOCKED: PostgreSQL container has an unexpected host port exposure")
	}
	configured := container.HostConfig.PortBindings["5432/tcp"][0]
	effective := container.NetworkSettings.Ports["5432/tcp"]
	configuredPortMatches := configured.HostPort == "" || configured.HostPort == hostPort
	if configured.HostIP != "127.0.0.1" || !configuredPortMatches || len(effective) != 1 || effective[0].HostIP != "127.0.0.1" || effective[0].HostPort != hostPort {
		t.Fatal("BLOCKED: PostgreSQL host port is not the exact loopback binding named by the database URL")
	}
	if len(container.NetworkSettings.Ports) != 1 || len(container.NetworkSettings.Networks) != 1 {
		t.Fatal("BLOCKED: PostgreSQL container has an unexpected network or published port")
	}
	var networkName string
	for name := range container.NetworkSettings.Networks {
		networkName = name
	}
	if !strings.HasPrefix(networkName, adminOutboxTestOwner+"-net") {
		t.Fatal("BLOCKED: PostgreSQL container is attached to an unexpected test network")
	}
	networkOutput, err := exec.Command("docker", "network", "inspect", networkName).Output()
	if err != nil {
		t.Fatal("BLOCKED: could not verify the owned PostgreSQL test network")
	}
	var networks []adminOutboxDockerNetworkInspect
	if json.Unmarshal(networkOutput, &networks) != nil || len(networks) != 1 || networks[0].Name != networkName || networks[0].Driver != "bridge" ||
		networks[0].Labels["rushgo.owner"] != adminOutboxTestOwner || networks[0].Labels["rushgo.run_id"] != adminOutboxTestRunID {
		t.Fatal("BLOCKED: PostgreSQL test bridge network identity mismatch")
	}

	var volumeName string
	for _, mount := range container.Mounts {
		if mount.Destination == "/var/lib/postgresql/data" {
			if mount.Type != "volume" || volumeName != "" {
				t.Fatal("BLOCKED: PostgreSQL data path is not backed by one owned named volume")
			}
			volumeName = mount.Name
		}
	}
	if !strings.HasPrefix(volumeName, adminOutboxTestOwner+"-pg") {
		t.Fatal("BLOCKED: PostgreSQL container data volume does not match the owned test system")
	}
	volumeOutput, err := exec.Command("docker", "volume", "inspect", volumeName).Output()
	if err != nil {
		t.Fatal("BLOCKED: could not verify the owned PostgreSQL data volume")
	}
	var volumes []adminOutboxDockerVolumeInspect
	if json.Unmarshal(volumeOutput, &volumes) != nil || len(volumes) != 1 || volumes[0].Name != volumeName ||
		volumes[0].Labels["rushgo.owner"] != adminOutboxTestOwner || volumes[0].Labels["rushgo.run_id"] != adminOutboxTestRunID {
		t.Fatal("BLOCKED: PostgreSQL data volume owner/run identity mismatch")
	}
	if _, err := strconv.Atoi(hostPort); err != nil {
		t.Fatal("BLOCKED: Admin outbox host port is not numeric")
	}
}

func adminOutboxIntegrationSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	tables := []string{
		"outbox", "bookings", "booking_items", "bookings_read_model",
	}
	parts := make([]string, 0, len(tables))
	for _, table := range tables {
		var fingerprint string
		query := fmt.Sprintf(`SELECT count(*)::text || ':' || md5(COALESCE(string_agg(to_jsonb(t)::text, '|' ORDER BY to_jsonb(t)::text), '')) FROM %s AS t`, table)
		if err := pool.QueryRow(ctx, query).Scan(&fingerprint); err != nil {
			t.Fatalf("snapshot %s: %v", table, err)
		}
		parts = append(parts, table+"="+fingerprint)
	}
	return strings.Join(parts, ";")
}
