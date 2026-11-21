package database

import "testing"

func TestDatabaseTargetOmitsPassword(t *testing.T) {
	target, err := databaseTarget("postgres://ticketbox:super-secret@db.internal:5432/ticketbox?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if target != "db.internal:5432/ticketbox" {
		t.Fatalf("target=%q, want host, port, and database only", target)
	}
}
