//go:build windows

package service

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestNotificationRealPreflightPreservesDuplicateHistoryIntegration(t *testing.T) {
	p := semanticNotificationPool(t)
	ctx := context.Background()
	var schema string
	if err := p.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	for range 2 {
		if _, err := p.Exec(ctx, `INSERT INTO notifications(id,type,recipient,payload,status) VALUES($1,'booking_failure','fixture@example.com',jsonb_build_object('booking_id',$2::text),'SENT')`, uuid.New(), id); err != nil {
			t.Fatal(err)
		}
	}
	manifest := map[string]any{"version": 1, "compose_project": os.Getenv("RELIABILITY_TEST_PROJECT"), "postgres_containers": map[string]any{"notification": map[string]any{"container": os.Getenv("RELIABILITY_TEST_PG_CONTAINER"), "database": "ticketbox_notification_test", "user": "reliability_fixture", "schema": schema}}}
	raw, _ := json.Marshal(manifest)
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("python", "../../../../scripts/reliability/preflight.py", "--manifest", path, "--read-only").CombinedOutput()
	var report struct {
		Status   string `json:"status"`
		Services map[string]struct {
			Counts   map[string]int `json:"counts"`
			Mutation bool           `json:"mutation"`
			Adoption bool           `json:"automatic_adoption"`
		} `json:"services"`
	}
	if json.Unmarshal(out, &report) != nil || report.Status != "BLOCKED" || err == nil {
		t.Fatal("preflight failed to report duplicate history", string(out), err)
	}
	n := report.Services["notification"]
	if n.Counts["duplicate_semantic_groups"] != 1 || n.Counts["legacy_rows"] != 2 || n.Mutation || n.Adoption {
		t.Fatal("classification invented history", string(out))
	}
	var count int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE NOT managed AND last_error IS NULL AND status='SENT'`).Scan(&count); err != nil || count != 2 {
		t.Fatal("preflight mutated history", count, err)
	}
	if _, err := p.Exec(ctx, `DELETE FROM notifications`); err != nil {
		t.Fatal(err)
	}
	for _, entity := range []string{id, strings.ToUpper(id), "", "not-a-uuid", "00000000-0000-0000-0000-000000000000"} {
		if _, err := p.Exec(ctx, `INSERT INTO notifications(id,type,recipient,payload,status) VALUES($1,'booking_failure','fixture@example.com',jsonb_build_object('booking_id',$2::text),'SENT')`, uuid.New(), entity); err != nil {
			t.Fatal(err)
		}
	}
	out, err = exec.Command("python", "../../../../scripts/reliability/preflight.py", "--manifest", path, "--read-only").CombinedOutput()
	if json.Unmarshal(out, &report) != nil || err == nil || report.Status != "BLOCKED" {
		t.Fatal("invalid identity preflight passed", string(out), err)
	}
	n = report.Services["notification"]
	if n.Counts["duplicate_semantic_groups"] != 1 || n.Counts["unproven_identity_rows"] != 3 {
		t.Fatal("preflight failed canonical identity classification", string(out))
	}
}
