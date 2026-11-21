package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var fingerprintDirectories = []string{
	"pkg",
	"services/user",
	"services/event",
	"services/booking",
	"services/saga",
	"services/gateway",
	"services/payment",
	"services/scheduler",
	"scripts/ticket-e2e",
}

var ignoredFingerprintDirectories = map[string]struct{}{
	".git":            {},
	".next":           {},
	".playwright-cli": {},
	"node_modules":    {},
}

func fingerprint(root, prefix string) (string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve fingerprint root: %w", err)
	}
	var files []string
	for _, dir := range fingerprintDirectories {
		absoluteDir := filepath.Join(root, filepath.FromSlash(dir))
		if _, err := os.Stat(absoluteDir); err != nil {
			return "", fmt.Errorf("stat fingerprint input %q: %w", dir, err)
		}
		err := filepath.WalkDir(absoluteDir, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() && path != absoluteDir {
				if _, ignored := ignoredFingerprintDirectories[entry.Name()]; ignored {
					return filepath.SkipDir
				}
			}
			if entry.IsDir() {
				return nil
			}
			if !entry.Type().IsRegular() {
				return fmt.Errorf("fingerprint input %q is not a regular file", path)
			}
			files = append(files, path)
			return nil
		})
		if err != nil {
			return "", fmt.Errorf("walk fingerprint input %q: %w", dir, err)
		}
	}
	sort.Slice(files, func(i, j int) bool {
		left, _ := filepath.Rel(root, files[i])
		right, _ := filepath.Rel(root, files[j])
		return filepath.ToSlash(left) < filepath.ToSlash(right)
	})
	records := make([]string, 0, len(files))
	for _, path := range files {
		contents, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read fingerprint input %q: %w", path, err)
		}
		fileHash := sha256.Sum256(contents)
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return "", fmt.Errorf("make fingerprint input relative: %w", err)
		}
		records = append(records, prefix+"/"+filepath.ToSlash(relative)+"|"+hex.EncodeToString(fileHash[:]))
	}
	payload := strings.Join(records, "\n")
	result := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(result[:]), nil
}

func verifyFingerprint(root, prefix, expected string) error {
	if len(expected) != 64 {
		return errors.New("expected source fingerprint must be a SHA-256 hex digest")
	}
	if _, err := hex.DecodeString(expected); err != nil {
		return errors.New("expected source fingerprint must be a SHA-256 hex digest")
	}
	actual, err := fingerprint(root, prefix)
	if err != nil {
		return err
	}
	if !strings.EqualFold(actual, expected) {
		return fmt.Errorf("build context fingerprint mismatch: expected %s, got %s", expected, actual)
	}
	return nil
}

func main() {
	root := flag.String("root", ".", "backend build context root")
	prefix := flag.String("prefix", "backend", "path prefix used in fingerprint records")
	expected := flag.String("expected", "", "expected digest; when set, fail if the context differs")
	flag.Parse()
	if *expected == "" {
		actual, err := fingerprint(*root, *prefix)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(actual)
		return
	}
	if err := verifyFingerprint(*root, *prefix, *expected); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
