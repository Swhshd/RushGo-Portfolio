package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/pkg/authpolicy"
)

var errCollision = errors.New("canonical collisions require human resolution")

type identity struct {
	id        uuid.UUID
	canonical string
}

func audit(ctx context.Context, pool *pgxpool.Pool, apply bool) (int, int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx)
	if apply {
		if _, err = tx.Exec(ctx, `LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE`); err != nil {
			return 0, 0, err
		}
	}
	rows, err := tx.Query(ctx, `SELECT id,email FROM users ORDER BY id`)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	var entries []identity
	seen := map[string]bool{}
	collisions := 0
	for rows.Next() {
		var id uuid.UUID
		var email string
		if err = rows.Scan(&id, &email); err != nil {
			return 0, 0, err
		}
		canonical := authpolicy.NormalizeEmail(email)
		if seen[canonical] {
			collisions++
		}
		seen[canonical] = true
		entries = append(entries, identity{id, canonical})
	}
	if err = rows.Err(); err != nil {
		return 0, 0, err
	}
	rows.Close()
	if collisions > 0 {
		return len(entries), collisions, errCollision
	}
	if apply {
		var version int
		var dirty bool
		if err = tx.QueryRow(ctx, `SELECT version,dirty FROM schema_migrations`).Scan(&version, &dirty); err != nil {
			return len(entries), 0, err
		}
		if dirty || version < 2 {
			return len(entries), 0, errors.New("migration state unavailable")
		}
		for _, entry := range entries {
			if version >= 3 {
				var stored string
				if err = tx.QueryRow(ctx, `SELECT email_canonical FROM users WHERE id=$1`, entry.id).Scan(&stored); err != nil || stored != entry.canonical {
					return len(entries), 0, errors.New("canonical state mismatch")
				}
				continue
			}
			if _, err = tx.Exec(ctx, `UPDATE users SET email_canonical=$2 WHERE id=$1`, entry.id, entry.canonical); err != nil {
				return len(entries), 0, err
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return len(entries), 0, err
	}
	return len(entries), 0, nil
}
func run() int {
	apply := flag.Bool("apply-canonical", false, "apply after collision-free audit while auth/profile writes are paused")
	schemaState := flag.Bool("migration-state", false, "read clean schema version or FRESH; never modify schema")
	flag.Parse()
	dsn := os.Getenv("AUTH_PREFLIGHT_DATABASE_URL")
	if dsn == "" {
		fmt.Println("PREFLIGHT=UNAVAILABLE")
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Println("PREFLIGHT=UNAVAILABLE")
		return 1
	}
	defer pool.Close()
	if *schemaState {
		state, err := migrationState(ctx, pool)
		if err != nil {
			fmt.Println("MIGRATION_STATE_UNAVAILABLE")
			return 1
		}
		fmt.Println(state)
		return 0
	}
	count, collisions, err := audit(ctx, pool, *apply)
	fmt.Printf("users=%d collisions=%d\n", count, collisions)
	if errors.Is(err, errCollision) {
		fmt.Println("PREFLIGHT=CANONICAL_COLLISION_HUMAN_GATE")
		return 2
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		fmt.Println("PREFLIGHT=UNAVAILABLE")
		return 1
	}
	if err != nil {
		fmt.Println("PREFLIGHT=UNAVAILABLE")
		return 1
	}
	fmt.Println("PREFLIGHT=PASS")
	return 0
}

func migrationState(ctx context.Context, pool *pgxpool.Pool) (string, error) {
	var users, migrations bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.users') IS NOT NULL, to_regclass('public.schema_migrations') IS NOT NULL`).Scan(&users, &migrations); err != nil {
		return "", err
	}
	if !migrations {
		if users {
			return "", errors.New("unversioned existing schema requires review")
		}
		return "FRESH", nil
	}
	var version int64
	var dirty bool
	err := pool.QueryRow(ctx, `SELECT version, dirty FROM public.schema_migrations`).Scan(&version, &dirty)
	if errors.Is(err, pgx.ErrNoRows) && !users {
		return "FRESH", nil
	}
	if err != nil {
		return "", err
	}
	if dirty || !users || version < 1 {
		return "", errors.New("dirty or inconsistent migration state")
	}
	return fmt.Sprint(version), nil
}
func main() { os.Exit(run()) }
