package kafkacollector

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/segmentio/kafka-go/protocol"
)

const maxDecodedFrameBytes = 16 << 20

type tapConn struct {
	net.Conn
	collector    *Collector
	role         string
	connectionID uint64
	dialTarget   string
	peer         string

	writeMu     sync.Mutex
	readMu      sync.Mutex
	exchangeMu  sync.Mutex
	pendingMu   sync.Mutex
	pending     map[int32]pendingRequest
	writeFrames frameAccumulator
	readFrames  frameAccumulator
}

type pendingRequest struct {
	apiKey         protocol.ApiKey
	apiVersion     int16
	exchangeIndex  int
	tracked        bool
	requestDecoded bool
}

type frameAccumulator struct {
	buffer  []byte
	firstAt time.Time
	want    int64
	discard int64
}

func (t *tapConn) Write(buffer []byte) (int, error) {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	// Keep the request write and pending registration ahead of response parsing
	// on this connection, even if the peer responds before Write returns.
	t.exchangeMu.Lock()
	defer t.exchangeMu.Unlock()

	startedAt := time.Now()
	n, err := t.Conn.Write(buffer)
	if n > 0 {
		t.writeFrames.feed(buffer[:n], startedAt, 8, t.captureRequest, t.collector.recordDecodeFailure)
	}
	return n, err
}

func (t *tapConn) Read(buffer []byte) (int, error) {
	t.readMu.Lock()
	defer t.readMu.Unlock()

	n, err := t.Conn.Read(buffer)
	if n > 0 {
		readAt := time.Now()
		t.exchangeMu.Lock()
		t.readFrames.feed(buffer[:n], readAt, 4, t.captureResponse, t.collector.recordDecodeFailure)
		t.exchangeMu.Unlock()
	}
	return n, err
}

func (t *tapConn) captureRequest(frame []byte, requestAt, _ time.Time) {
	apiKey, apiVersion, correlationID, err := readRequestHeader(frame)
	if err != nil {
		t.collector.recordDecodeFailure(false)
		return
	}

	pending := pendingRequest{apiKey: apiKey, apiVersion: apiVersion, exchangeIndex: -1}
	if apiKey == protocol.OffsetCommit {
		t.collector.recordOffsetCommit()
	}
	if isCapturedAPI(apiKey) {
		request, decodeErr := decodeRequestFrame(frame)
		exchange := Exchange{
			Identity:      t.collector.identity,
			Role:          t.role,
			ConnectionID:  t.connectionID,
			DialTarget:    t.dialTarget,
			Peer:          t.peer,
			APIKey:        int16(apiKey),
			APIVersion:    apiVersion,
			CorrelationID: correlationID,
			RequestAt:     requestAt,
			MatchStatus:   "REQUEST_ONLY",
		}
		exchange.RequestFrameLength, _, exchange.RequestFrameHex, exchange.RequestFrameTruncated = captureFrameEvidence(frame)
		pending.tracked = true
		if decodeErr != nil {
			exchange.MatchStatus = "REQUEST_DECODE_ERROR"
			t.collector.recordDecodeFailure(false)
		} else {
			exchange.GroupID = request.GroupID
			exchange.MemberID = request.MemberID
			exchange.GenerationID = request.GenerationID
			exchange.GroupInstanceID = request.GroupInstanceID
			exchange.CoordinatorKeyType = request.CoordinatorKeyType
			exchange.RequestProtocolType = request.ProtocolType
			exchange.RequestProtocolName = request.ProtocolName
			exchange.OffsetCommitRequest = request.OffsetCommit
			pending.requestDecoded = true
		}
		pending.exchangeIndex = t.collector.appendRequest(exchange)
	}

	t.pendingMu.Lock()
	t.pending[correlationID] = pending
	t.pendingMu.Unlock()
}

func (t *tapConn) captureResponse(frame []byte, _, responseAt time.Time) {
	correlationID, err := readResponseCorrelation(frame)
	if err != nil {
		t.collector.recordDecodeFailure(false)
		return
	}

	t.pendingMu.Lock()
	pending, ok := t.pending[correlationID]
	if ok {
		delete(t.pending, correlationID)
	}
	hasPendingHeartbeat := false
	if !ok {
		for _, request := range t.pending {
			if request.tracked && request.apiKey == protocol.Heartbeat {
				hasPendingHeartbeat = true
				break
			}
		}
	}
	t.pendingMu.Unlock()

	if !ok {
		if hasPendingHeartbeat {
			t.collector.recordUnmatchedResponse()
		}
		return
	}
	if !pending.tracked {
		return
	}

	response, decodeErr := decodeResponseFrame(frame, pending.apiKey, pending.apiVersion)
	if decodeErr != nil {
		t.collector.recordDecodeFailure(false)
		t.collector.updateResponse(pending.exchangeIndex, response, responseAt, false, "RESPONSE_DECODE_ERROR", decodeErr.Error())
		return
	}
	if response.CorrelationID != correlationID || !pending.requestDecoded {
		t.collector.updateResponse(pending.exchangeIndex, response, responseAt, false, "CORRELATION_OR_REQUEST_GAP", "")
		return
	}
	t.collector.updateResponse(pending.exchangeIndex, response, responseAt, true, "MATCHED", "")
}

func isCapturedAPI(apiKey protocol.ApiKey) bool {
	switch apiKey {
	case protocol.Heartbeat, protocol.FindCoordinator, protocol.JoinGroup, protocol.LeaveGroup,
		protocol.SyncGroup, protocol.Metadata, protocol.OffsetFetch, protocol.OffsetCommit:
		return true
	default:
		return false
	}
}

func readRequestHeader(frame []byte) (protocol.ApiKey, int16, int32, error) {
	if len(frame) < 12 {
		return 0, 0, 0, fmt.Errorf("Kafka request frame is shorter than its fixed header")
	}
	bodySize := int64(binary.BigEndian.Uint32(frame[:4]))
	if bodySize != int64(len(frame)-4) {
		return 0, 0, 0, fmt.Errorf("Kafka request frame size does not match its prefix")
	}
	apiKey := protocol.ApiKey(int16(binary.BigEndian.Uint16(frame[4:6])))
	apiVersion := int16(binary.BigEndian.Uint16(frame[6:8]))
	correlationID := int32(binary.BigEndian.Uint32(frame[8:12]))
	return apiKey, apiVersion, correlationID, nil
}

func readResponseCorrelation(frame []byte) (int32, error) {
	if len(frame) < 8 {
		return 0, fmt.Errorf("Kafka response frame is shorter than its fixed header")
	}
	bodySize := int64(binary.BigEndian.Uint32(frame[:4]))
	if bodySize != int64(len(frame)-4) {
		return 0, fmt.Errorf("Kafka response frame size does not match its prefix")
	}
	return int32(binary.BigEndian.Uint32(frame[4:8])), nil
}

func (f *frameAccumulator) feed(data []byte, at time.Time, minBodySize int64, capture func([]byte, time.Time, time.Time), issue func(bool)) {
	for len(data) > 0 {
		if f.discard > 0 {
			consume := int64(len(data))
			if consume > f.discard {
				consume = f.discard
			}
			data = data[consume:]
			f.discard -= consume
			if f.discard == 0 {
				f.reset()
			}
			continue
		}

		if len(f.buffer) == 0 {
			f.firstAt = at
		}
		if len(f.buffer) < 4 {
			need := 4 - len(f.buffer)
			if need > len(data) {
				need = len(data)
			}
			f.buffer = append(f.buffer, data[:need]...)
			data = data[need:]
			if len(f.buffer) < 4 {
				return
			}
		}

		if f.want == 0 {
			bodySize := int64(binary.BigEndian.Uint32(f.buffer[:4]))
			if bodySize < minBodySize {
				issue(false)
				f.buffer = nil
				f.firstAt = time.Time{}
				f.discard = bodySize
				continue
			}
			if bodySize > maxDecodedFrameBytes {
				issue(true)
				f.buffer = nil
				f.firstAt = time.Time{}
				f.discard = bodySize
				continue
			}
			f.want = bodySize + 4
		}

		remaining := f.want - int64(len(f.buffer))
		if remaining > 0 {
			consume := int64(len(data))
			if consume > remaining {
				consume = remaining
			}
			if consume == 0 {
				return
			}
			f.buffer = append(f.buffer, data[:consume]...)
			data = data[consume:]
		}
		if int64(len(f.buffer)) == f.want {
			frame := append([]byte(nil), f.buffer...)
			firstAt := f.firstAt
			f.reset()
			capture(frame, firstAt, at)
		}
	}
}

func (f *frameAccumulator) reset() {
	f.buffer = nil
	f.firstAt = time.Time{}
	f.want = 0
}
