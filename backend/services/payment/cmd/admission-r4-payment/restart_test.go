//go:build admission_r4

package main

import "testing"

func TestRestartVerificationArgumentsFailClosed(t *testing.T) {
	if mode, err := restartVerificationMode(nil); mode || err != nil {
		t.Fatal("normal start refused")
	}
	if mode, err := restartVerificationMode([]string{"--verify-static-restart"}); !mode || err != nil {
		t.Fatal("read-only verification refused")
	}
	for _, args := range [][]string{{"--unknown"}, {"--verify-static-restart", "--extra"}, {"--verify-static-restart", "--verify-static-restart"}} {
		if _, err := restartVerificationMode(args); err == nil {
			t.Fatalf("unsafe arguments accepted: %v", args)
		}
	}
}
