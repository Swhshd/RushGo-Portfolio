//go:build reliability_component

package recoverycomponent

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"strconv"
	"strings"
)

var errInvalidOwnerManifest = errors.New("invalid Saga component PostgreSQL owner manifest")

type OwnerManifest struct {
	ManifestVersion        string `json:"manifest_version"`
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
}

func validateOwnerManifest(dsn string, manifest OwnerManifest) error {
	if dsn == "" || manifest.ManifestVersion != "rushgo.saga.component-pg-owner.v1" ||
		manifest.ResourceID == "" || manifest.ResourceID != manifest.ContainerID ||
		!isFullContainerID(manifest.ContainerID) || manifest.Database == "" || manifest.Host == "" ||
		manifest.Port < 1 || manifest.Port > 65535 || manifest.RunID == "" || manifest.OwnerNonce == "" ||
		manifest.DatabaseMarker == "" {
		return errInvalidOwnerManifest
	}
	if _, ok := new(big.Int).SetString(manifest.ServerSystemIdentifier, 10); !ok {
		return errInvalidOwnerManifest
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.User == nil || parsed.User.Username() != "component" {
		return errInvalidOwnerManifest
	}
	if _, hasPassword := parsed.User.Password(); hasPassword {
		return errInvalidOwnerManifest
	}
	host := parsed.Hostname()
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() || host != manifest.Host || parsed.Path != "/"+manifest.Database {
		return errInvalidOwnerManifest
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil || port != manifest.Port {
		return errInvalidOwnerManifest
	}
	if manifest.DSNSHA256 == "" || len(manifest.DSNSHA256) != sha256.Size*2 {
		return errInvalidOwnerManifest
	}
	if _, err := hex.DecodeString(manifest.DSNSHA256); err != nil {
		return errInvalidOwnerManifest
	}
	digest := sha256.Sum256([]byte(dsn))
	if !strings.EqualFold(manifest.DSNSHA256, hex.EncodeToString(digest[:])) {
		return fmt.Errorf("%w: dsn hash mismatch", errInvalidOwnerManifest)
	}
	return nil
}

func isFullContainerID(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
