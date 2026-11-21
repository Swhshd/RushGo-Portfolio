// Offline inspection of one preserved TEST Kafka record. No network or Docker operations.
package main

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/ticketbox/booking/internal/domain"
	"io"
	"os"
	"path"
	"regexp"
	"time"
)

const expectedBooking = "003217d4-d350-46b4-81df-7988b42a04ad"
const expectedPayment = "5431a3c2-d2d0-4710-97e7-a00a9f4a182e"
const expectedEvent = "a1777e72-d86b-4e60-b1ac-ea6163a1bb2c"
const run = "4b7a98d6-35d5-4fa3-ad46-8ed131621a47"
const expectedHash = "e8de37b0f59d8bfb15851d1536444146d6107bb32958e6f2b6a05d6e3665308d"

var originalEvent domain.TicketIssuanceEvent
var segmentName = regexp.MustCompile(`^ticket\.issuance\.events-0/[0-9]{20}\.log$`)

func inspectValue(key, value []byte) map[string]bool {
	checks := map[string]bool{}
	originalEnvelopeBytes = append([]byte(nil), value...)
	var envelope struct {
		ID        string          `json:"id"`
		Type      string          `json:"type"`
		Timestamp time.Time       `json:"timestamp"`
		Data      json.RawMessage `json:"data"`
	}
	checks["envelopeDecodes"] = json.Unmarshal(value, &envelope) == nil
	checks["typeMatches"] = envelope.Type == "TicketIssuanceRequested.v1"
	checks["keyMatches"] = string(key) == expectedBooking
	var data struct {
		Version    int       `json:"version"`
		ID         uuid.UUID `json:"ticket_issuance_id"`
		Saga       uuid.UUID `json:"saga_id"`
		Booking    uuid.UUID `json:"booking_id"`
		Payment    uuid.UUID `json:"payment_id"`
		Event      uuid.UUID `json:"event_id"`
		Generation int64     `json:"generation"`
	}
	checks["payloadDecodes"] = json.Unmarshal(envelope.Data, &data) == nil
	checks["versionMatches"] = data.Version == 1
	outer, err := uuid.Parse(envelope.ID)
	checks["issuanceIdentityMatches"] = err == nil && outer != uuid.Nil && outer == data.ID
	checks["bookingMatches"] = data.Booking.String() == expectedBooking
	checks["paymentMatches"] = data.Payment.String() == expectedPayment
	checks["eventMatches"] = data.Event.String() == expectedEvent
	e := domain.TicketIssuanceEvent{ID: data.ID, SagaID: data.Saga, BookingID: data.Booking, PaymentID: data.Payment, EventID: data.Event, Generation: data.Generation}
	checks["domainValid"] = e.Validate() == nil
	originalEvent = e
	return checks
}
func inspectSegment(data []byte) ([]map[string]bool, error) {
	rows := []map[string]bool{}
	for len(data) > 0 {
		if len(data) < 61 {
			return nil, errors.New("segment_truncated")
		}
		size := int(binary.BigEndian.Uint32(data[8:12])) + 12
		if size < 61 || size > len(data) || size > 16*1024*1024 || data[16] != 2 {
			return nil, errors.New("batch_format_invalid")
		}
		// Original producer uses no compression. Reject unbounded decompression or control batches.
		if binary.BigEndian.Uint16(data[21:23])&0x37 != 0 {
			return nil, errors.New("batch_attributes_unsupported")
		}
		count := int32(binary.BigEndian.Uint32(data[57:61]))
		if count < 0 || count > 10000 {
			return nil, errors.New("record_count_invalid")
		}
		wire := make([]byte, 4+size)
		binary.BigEndian.PutUint32(wire, uint32(size))
		copy(wire[4:], data[:size])
		rs := protocol.RecordSet{}
		if _, err := rs.ReadFrom(bytes.NewReader(wire)); err != nil {
			return nil, errors.New("batch_crc_or_decode_failed")
		}
		seen := 0
		if rs.Records == nil {
			return nil, errors.New("record_reader_missing")
		}
		for {
			r, err := rs.Records.ReadRecord()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, errors.New("record_decode_failed")
			}
			seen++
			if seen > int(count) {
				return nil, errors.New("record_count_exceeded")
			}
			if r.Offset != 0 {
				continue
			}
			if r.Value == nil {
				return nil, errors.New("record_value_missing")
			}
			value, err := io.ReadAll(io.LimitReader(r.Value, 1024*1024+1))
			if err != nil || len(value) > 1024*1024 {
				return nil, errors.New("record_value_invalid")
			}
			var key []byte
			if r.Key != nil {
				key, err = io.ReadAll(io.LimitReader(r.Key, 1025))
				if err != nil || len(key) > 1024 {
					return nil, errors.New("record_key_invalid")
				}
			}
			rows = append(rows, inspectValue(key, value))
		}
		if seen != int(count) {
			return nil, errors.New("record_count_mismatch")
		}
		data = data[size:]
	}
	return rows, nil
}
func inspectArchive(name string) (map[string]any, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, errors.New("snapshot_unavailable")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 16*1024*1024 {
		return nil, errors.New("snapshot_size_invalid")
	}
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return nil, errors.New("snapshot_read_failed")
	}
	if hex.EncodeToString(h.Sum(nil)) != expectedHash {
		return nil, errors.New("snapshot_hash_mismatch")
	}
	if _, err = f.Seek(0, 0); err != nil {
		return nil, errors.New("snapshot_seek_failed")
	}
	tr := tar.NewReader(f)
	rows := []map[string]bool{}
	segments := 0
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.New("snapshot_tar_invalid")
		}
		if !segmentName.MatchString(path.Clean(header.Name)) {
			continue
		}
		if !header.FileInfo().Mode().IsRegular() || header.Size > 16*1024*1024 {
			return nil, errors.New("segment_type_invalid")
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			return nil, errors.New("segment_read_failed")
		}
		parsed, err := inspectSegment(b)
		if err != nil {
			return nil, err
		}
		rows = append(rows, parsed...)
		segments++
	}
	if len(rows) != 1 {
		return nil, errors.New("offset_zero_missing_or_duplicate")
	}
	passed := true
	for _, v := range rows[0] {
		passed = passed && v
	}
	result := "MESSAGE_CHECK_FAILED"
	if passed {
		result = "MESSAGE_CHECK_PASS"
	}
	return map[string]any{"runId": run, "status": result, "segments": segments, "partition": 0, "offset": 0, "checks": rows[0], "containersStarted": 0, "financialWrites": 0, "limitation": "Message format and identity only; database state and authority responses are not verified."}, nil
}
