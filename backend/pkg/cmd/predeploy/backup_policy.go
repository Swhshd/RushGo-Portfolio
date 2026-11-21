package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
)

// This validates declarations only. Neither an activation label nor a restore
// receipt supplied by an operator proves that recovery was exercised.
func verifyBackupPolicy(path string) string {
	if path == "" {
		return "NOT_CONFIGURED"
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 65536 {
		return "FAIL"
	}
	f, err := os.Open(path)
	if err != nil {
		return "FAIL"
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil || len(data) > 65536 {
		return "FAIL"
	}
	var p struct {
		Status     string          `json:"status"`
		Storage    *string         `json:"storage"`
		Encryption *string         `json:"encryptionKeyReference"`
		Retention  *int64          `json:"retentionDays"`
		RPO        *int64          `json:"rpoSeconds"`
		RTO        *int64          `json:"rtoSeconds"`
		Schedule   *string         `json:"schedule"`
		Restore    json.RawMessage `json:"lastVerifiedRestore"`
		Scope      []string        `json:"scope"`
		Note       string          `json:"note"`
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&p) != nil || d.Decode(new(any)) != io.EOF {
		return "FAIL"
	}
	want := map[string]bool{"user": false, "event": false, "booking": false, "payment": false, "saga": false, "notification": false, "scheduler": false}
	for _, name := range p.Scope {
		seen, exists := want[name]
		if !exists || seen {
			return "FAIL"
		}
		want[name] = true
	}
	for _, seen := range want {
		if !seen {
			return "FAIL"
		}
	}
	if p.Status != "NOT_ACTIVATED" && p.Status != "CONFIGURED" {
		return "FAIL"
	}
	missing := p.Status == "NOT_ACTIVATED"
	for _, value := range []*string{p.Storage, p.Encryption, p.Schedule} {
		if value == nil || strings.TrimSpace(*value) == "" {
			missing = true
		}
	}
	for _, value := range []*int64{p.Retention, p.RPO, p.RTO} {
		if value == nil {
			missing = true
		} else if *value <= 0 {
			return "FAIL"
		}
	}
	if missing {
		return "NOT_CONFIGURED"
	}
	return "PASS" // Configuration shape only; no network or recovery execution.
}
