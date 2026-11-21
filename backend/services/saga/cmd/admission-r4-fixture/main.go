//go:build admission_r4

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"
)

const commandTimeout = 45 * time.Second

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()
	if err := run(ctx, os.Getenv, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "admission-r4-fixture:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, getenv func(string) string, stdout *os.File) error {
	config, err := loadConfig(getenv)
	if err != nil {
		return err
	}
	manifestFile, err := reserveManifestOutput(config.OutputPath)
	if err != nil {
		return err
	}
	keepManifest := false
	defer func() {
		if !keepManifest {
			_ = manifestFile.Close()
			_ = os.Remove(config.OutputPath)
		}
	}()

	now := time.Now().UTC().Truncate(time.Microsecond)
	cases, err := makeFixtureCases(config.DatabaseSuffix, now)
	if err != nil {
		return err
	}
	manifest := newManifest(config, cases, now)
	targets, err := openTargets(ctx, config)
	if err != nil {
		return err
	}
	defer closeTargets(targets)
	for _, target := range targets {
		if err := validateTarget(ctx, target); err != nil {
			return err
		}
	}

	transactions, err := beginTransactions(ctx, targets)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			rollbackTransactions(transactions)
		}
	}()
	if err := seedUser(ctx, transactions["user"], cases); err != nil {
		return err
	}
	if err := seedEvent(ctx, transactions["event"], cases, manifest.CreatedAt); err != nil {
		return err
	}
	if err := seedSaga(ctx, transactions["saga"], cases, manifest.CreatedAt); err != nil {
		return err
	}
	if err := seedBooking(ctx, transactions["booking"], cases, manifest.CreatedAt); err != nil {
		return err
	}
	if err := writeManifest(manifestFile, manifest); err != nil {
		return err
	}
	if err := manifestFile.Close(); err != nil {
		return fmt.Errorf("close private manifest: %w", err)
	}
	for _, service := range []string{"user", "event", "saga", "booking"} {
		if err := transactions[service].Commit(ctx); err != nil {
			return fmt.Errorf("commit isolated %s fixture failed; earlier service commits may already exist", service)
		}
	}
	committed = true
	keepManifest = true
	keys := make([]string, 0, len(cases))
	for _, fixture := range cases {
		keys = append(keys, fixture.Key)
	}
	if _, err := fmt.Fprintf(stdout, "fixture_manifest=%s\nfixture_cases=%s\n", config.OutputPath, strings.Join(keys, ",")); err != nil {
		return fmt.Errorf("write safe fixture summary: %w", err)
	}
	return nil
}
