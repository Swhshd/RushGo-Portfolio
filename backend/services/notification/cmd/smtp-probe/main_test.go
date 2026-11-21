package main

import "testing"

func TestProbeRequiresExactRecipientAuthorizationAndMessageIdentity(t *testing.T) {
	const id = "50bbed5c-63ae-4492-b9b5-783b89ff9ade"
	for _, args := range [][4]string{{"approved@example.test", "", id, "receipt"}, {"approved@example.test", "other@example.test", id, "receipt"}, {"approved@example.test", "approved@example.test", "", "receipt"}, {"approved@example.test", "approved@example.test", id, ""}, {"one@example.test,two@example.test", "one@example.test,two@example.test", id, "receipt"}} {
		if _, _, err := authorizeProbe(args[0], args[1], args[2], args[3]); err == nil {
			t.Fatal("unauthorized probe accepted")
		}
	}
	if _, _, err := authorizeProbe("approved@example.test", "approved@example.test", id, "new-receipt"); err != nil {
		t.Fatal(err)
	}
}
