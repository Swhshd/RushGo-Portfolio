//go:build stripe_minimal_test

package main

import "testing"

const (
	recoverySource = "89abcdef0123456789abcdef0123456789abcdef"
)

func TestSourceIdentityRequiresExactRunForReceiptRecovery(t *testing.T) {
	cfg := testStateConfig()
	cfg.RunID = approvedRecoveryRunID
	cfg.SourceCommit = approvedRecoverySource
	tests := []struct {
		name          string
		binarySource  string
		configured    string
		recoveryRunID string
		wantErr       bool
	}{
		{name: "normal same source", binarySource: approvedRecoverySource, configured: approvedRecoverySource},
		{name: "exact recovery run", binarySource: recoverySource, configured: approvedRecoverySource, recoveryRunID: approvedRecoveryRunID},
		{name: "wrong recovery run", binarySource: recoverySource, configured: approvedRecoverySource, recoveryRunID: "00000000-0000-4000-8000-000000000009", wantErr: true},
		{name: "recovery source env mismatch", binarySource: recoverySource, configured: recoverySource, recoveryRunID: approvedRecoveryRunID, wantErr: true},
		{name: "recovery same binary", binarySource: approvedRecoverySource, configured: approvedRecoverySource, recoveryRunID: approvedRecoveryRunID, wantErr: true},
		{name: "unrequested source mismatch", binarySource: recoverySource, configured: approvedRecoverySource, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := validateSourceIdentity(cfg, tt.binarySource, tt.configured, tt.recoveryRunID)
			if (gotErr != nil) != tt.wantErr {
				t.Fatalf("validateSourceIdentity() error=%v, wantErr=%v", gotErr, tt.wantErr)
			}
		})
	}
}
