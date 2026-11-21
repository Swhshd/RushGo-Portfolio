//go:build stripe_minimal_test

package main

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

// acquireProcessLock uses an exclusive directory so it works with the same
// package on the Windows development host and the Linux TEST host. A crashed
// process leaves the lock behind and therefore fails closed on restart.
func acquireProcessLock(path string) (func(), error) {
	if path == "" {
		return nil, errors.New("lock path required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, errors.New("lock parent directory unavailable")
	}
	if err := os.Mkdir(path, 0700); err != nil {
		return nil, errors.New("lock already exists or is unavailable")
	}
	owner := uuid.NewString()
	ownerPath := filepath.Join(path, "owner")
	file, err := os.OpenFile(ownerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		_ = os.Remove(path)
		return nil, errors.New("lock ownership could not be recorded")
	}
	if _, err := file.WriteString(owner); err != nil {
		_ = file.Close()
		_ = os.Remove(ownerPath)
		_ = os.Remove(path)
		return nil, errors.New("lock ownership could not be recorded")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(ownerPath)
		_ = os.Remove(path)
		return nil, errors.New("lock ownership could not be synchronized")
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(ownerPath)
		_ = os.Remove(path)
		return nil, errors.New("lock ownership could not be closed")
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			data, err := os.ReadFile(ownerPath)
			if err != nil || string(data) != owner {
				return
			}
			_ = os.Remove(ownerPath)
			_ = os.Remove(path)
		})
	}
	return release, nil
}
