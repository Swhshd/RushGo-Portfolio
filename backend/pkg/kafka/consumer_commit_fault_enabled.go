//go:build ticket_e2e

package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type ticketE2ECommitFaultMarker struct {
	EventID   string `json:"event_id"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

var ticketE2ECommitFaultState struct {
	sync.Mutex
	armed     bool
	directory string
}

func ArmTicketE2ECommitFault(directory string) error {
	if !filepath.IsAbs(directory) {
		return errors.New("ticket E2E fault directory must be absolute")
	}
	info, err := os.Stat(directory)
	if err != nil || !info.IsDir() {
		return errors.New("ticket E2E fault directory is unavailable")
	}
	ticketE2ECommitFaultState.Lock()
	defer ticketE2ECommitFaultState.Unlock()
	if ticketE2ECommitFaultState.armed {
		return errors.New("ticket E2E offset fault is already armed")
	}
	markerPath := filepath.Join(directory, "ticket-consumer-before-offset-commit.json")
	if _, err := os.Stat(markerPath); err == nil || !errors.Is(err, os.ErrNotExist) {
		return errors.New("ticket E2E offset fault marker already exists or cannot be checked")
	}
	ticketE2ECommitFaultState.directory = directory
	ticketE2ECommitFaultState.armed = true
	return nil
}

// beforeOffsetCommit is compiled into C-SIM binaries only. The one-shot marker
// makes a handler-committed / broker-offset-uncommitted restart reproducible:
// the first eligible delivery blocks, and the same event is allowed through
// after the isolated Booking process restarts.
func beforeOffsetCommit(ctx context.Context, event Event, topic string, partition int, offset int64) error {
	if !strings.HasPrefix(topic, "ticket.issuance.events.") || event.Type != "TicketIssuanceRequested.v1" {
		return nil
	}
	if event.ID == "" {
		return errors.New("ticket E2E offset fault event identity is missing")
	}
	ticketE2ECommitFaultState.Lock()
	defer ticketE2ECommitFaultState.Unlock()
	if !ticketE2ECommitFaultState.armed {
		return nil
	}
	directory := ticketE2ECommitFaultState.directory
	markerPath := filepath.Join(directory, "ticket-consumer-before-offset-commit.json")
	marker := ticketE2ECommitFaultMarker{EventID: event.ID, Topic: topic, Partition: partition, Offset: offset}
	file, err := os.OpenFile(markerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		data, readErr := os.ReadFile(markerPath)
		if readErr != nil {
			return errors.New("ticket E2E offset fault marker cannot be read")
		}
		var prior ticketE2ECommitFaultMarker
		if json.Unmarshal(data, &prior) != nil || prior.EventID != marker.EventID || prior.Topic != marker.Topic || prior.Partition != marker.Partition {
			return errors.New("ticket E2E offset fault marker identity mismatch")
		}
		return nil
	}
	if err != nil {
		return errors.New("ticket E2E offset fault marker cannot be created")
	}
	if err := json.NewEncoder(file).Encode(marker); err != nil {
		_ = file.Close()
		return errors.New("ticket E2E offset fault marker cannot be written")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errors.New("ticket E2E offset fault marker cannot be synchronized")
	}
	if err := file.Close(); err != nil {
		return errors.New("ticket E2E offset fault marker cannot be closed")
	}
	ticketE2ECommitFaultState.armed = false
	<-ctx.Done()
	return ctx.Err()
}
