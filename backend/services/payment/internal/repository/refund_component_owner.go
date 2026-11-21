package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type refundExplainOwnerManifest struct {
	ManifestVersion        int    `json:"manifest_version"`
	ResourceID             string `json:"resource_id"`
	ContainerID            string `json:"container_id"`
	Database               string `json:"database"`
	Host                   string `json:"host"`
	Port                   int    `json:"port"`
	DSNSHA256              string `json:"dsn_sha256"`
	RunID                  string `json:"run_id"`
	OwnerNonce             string `json:"owner_nonce"`
	ServerSystemIdentifier string `json:"server_system_identifier"`
	DatabaseMarker         string `json:"database_marker"`
	ServerVersion          string `json:"server_version"`
	ImageID                string `json:"image_id"`
}

// OpenRefundComponentDatabase accepts only the current ownership-manifested
// local synthetic PostgreSQL component. It performs identity queries only and
// never starts a database, mutates data, or accepts a freeform external host.
func OpenRefundComponentDatabase(ctx context.Context, dsn, manifestPath string) (*pgxpool.Pool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if dsn == "" || manifestPath == "" {
		return nil, errors.New("refund component DSN and owner manifest are required")
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, errors.New("cannot read refund component owner manifest")
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	var manifest refundExplainOwnerManifest
	if err := decoder.Decode(&manifest); err != nil {
		return nil, errors.New("refund component owner manifest is invalid")
	}
	if manifest.ManifestVersion != 1 || manifest.ResourceID == "" || manifest.ContainerID == "" ||
		manifest.Database == "" || manifest.Host == "" || manifest.Port < 1 || manifest.Port > 65535 ||
		manifest.DSNSHA256 == "" || manifest.RunID == "" || manifest.OwnerNonce == "" ||
		manifest.ServerSystemIdentifier == "" || manifest.DatabaseMarker == "" || manifest.ResourceID != manifest.ContainerID {
		return nil, errors.New("refund component owner manifest is incomplete")
	}
	expectedMarker := "codex-refund-component:v1:" + manifest.ResourceID + ":" + manifest.RunID + ":" + manifest.OwnerNonce
	if manifest.DatabaseMarker != expectedMarker {
		return nil, errors.New("refund component database marker is not owner-bound")
	}
	dsnDigest := sha256.Sum256([]byte(dsn))
	if hex.EncodeToString(dsnDigest[:]) != manifest.DSNSHA256 {
		return nil, errors.New("refund component DSN does not match its owner manifest")
	}
	config, err := refundComponentPGXConfig(dsn, manifest)
	if err != nil {
		return nil, err
	}

	connectCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(connectCtx, config)
	if err != nil {
		return nil, errors.New("cannot connect to manifested refund component database")
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			pool.Close()
		}
	}()
	if err := pool.Ping(connectCtx); err != nil {
		return nil, errors.New("manifested refund component database is unavailable")
	}
	var serverVersion int
	var database, systemIdentifier, marker string
	if err := pool.QueryRow(connectCtx, "SELECT current_setting('server_version_num')::integer").Scan(&serverVersion); err != nil {
		return nil, errors.New("cannot identify refund component PostgreSQL version")
	}
	if err := pool.QueryRow(connectCtx, "SELECT current_database()").Scan(&database); err != nil {
		return nil, errors.New("cannot identify refund component database")
	}
	if err := pool.QueryRow(connectCtx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&systemIdentifier); err != nil {
		return nil, errors.New("cannot identify refund component PostgreSQL cluster")
	}
	if err := pool.QueryRow(connectCtx, `SELECT pg_catalog.shobj_description(oid,'pg_database')
		FROM pg_catalog.pg_database WHERE datname=current_database()`).Scan(&marker); err != nil {
		return nil, errors.New("cannot identify refund component database marker")
	}
	if serverVersion < 160000 || serverVersion >= 170000 || database != manifest.Database ||
		systemIdentifier != manifest.ServerSystemIdentifier || marker != manifest.DatabaseMarker {
		return nil, errors.New("refund component database identity differs from its owner manifest")
	}
	closeOnError = false
	return pool, nil
}

func refundComponentPGXConfig(dsn string, manifest refundExplainOwnerManifest) (*pgxpool.Config, error) {
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		return nil, errors.New("refund component DSN is invalid")
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil || len(query) != 1 || len(query["sslmode"]) != 1 || query.Get("sslmode") != "disable" {
		return nil, errors.New("refund component DSN query settings are not allowed")
	}
	for _, setting := range os.Environ() {
		key, _, _ := strings.Cut(setting, "=")
		if strings.HasPrefix(strings.ToUpper(key), "PG") {
			return nil, errors.New("PostgreSQL environment overrides are not allowed")
		}
	}
	endpointIP := net.ParseIP(parsed.Hostname())
	port, portErr := strconv.Atoi(parsed.Port())
	if endpointIP == nil || !endpointIP.IsLoopback() || portErr != nil || port != manifest.Port ||
		parsed.Hostname() != manifest.Host || strings.TrimPrefix(parsed.Path, "/") != manifest.Database {
		return nil, errors.New("refund component DSN is not the manifested loopback endpoint")
	}

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil || config == nil || config.ConnConfig == nil {
		return nil, errors.New("refund component effective connection config is invalid")
	}
	conn := config.ConnConfig
	effectiveIP := net.ParseIP(conn.Host)
	if effectiveIP == nil || !effectiveIP.IsLoopback() || conn.Host != manifest.Host ||
		int(conn.Port) != manifest.Port || conn.Database != manifest.Database || len(conn.Fallbacks) != 0 {
		return nil, errors.New("refund component effective connection config is not the manifested loopback endpoint")
	}
	return config, nil
}
