package main

import "testing"

func TestRecoveryScopeCannotFallBackFromIncompleteOriginalRequest(t *testing.T) {
	attempt := "26aadf2d-e4b0-4f21-81c1-27ac139a866d"
	for _, pair := range [][2]string{
		{"", ""}, {"guarded_original_recovery", ""},
		{"guarded_original_recovery", "00000000-0000-0000-0000-000000000000"},
		{"isolated_snapshot_copy", attempt}, {"production", attempt},
	} {
		if _, err := recoveryScope(pair[0], pair[1]); err == nil {
			t.Fatalf("unexpected accepted scope %q", pair[0])
		}
	}
	if original, err := recoveryScope("guarded_original_recovery", attempt); err != nil || !original {
		t.Fatal("explicit original attempt rejected", err)
	}
	if original, err := recoveryScope("isolated_snapshot_copy", ""); err != nil || original {
		t.Fatal("copy scope changed", err)
	}
}
