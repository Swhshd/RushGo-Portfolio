package kafkacollector

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	findcoordinatorapi "github.com/segmentio/kafka-go/protocol/findcoordinator"
	heartbeatapi "github.com/segmentio/kafka-go/protocol/heartbeat"
	metadataapi "github.com/segmentio/kafka-go/protocol/metadata"
)

var errUnexpectedAPI = errors.New("unexpected Kafka API")

func TestClassifyHeartbeatResponseCodeSeparatesBrokerFacts(t *testing.T) {
	tests := []struct {
		name string
		code int16
		want string
	}{
		{name: "accepted now", code: 0, want: "SUCCESS_ACCEPTED_AT_RESPONSE_TIME"},
		{name: "illegal generation", code: int16(kafkago.IllegalGeneration), want: "ILLEGAL_GENERATION"},
		{name: "unknown member", code: int16(kafkago.UnknownMemberId), want: "UNKNOWN_MEMBER_ID"},
		{name: "rebalance in progress", code: int16(kafkago.RebalanceInProgress), want: "REBALANCE_IN_PROGRESS"},
		{name: "coordinator unavailable", code: int16(kafkago.GroupCoordinatorNotAvailable), want: "COORDINATOR_NOT_READY"},
		{name: "coordinator migration", code: int16(kafkago.NotCoordinatorForGroup), want: "COORDINATOR_MIGRATION"},
		{name: "authentication", code: int16(kafkago.SASLAuthenticationFailed), want: "AUTHENTICATION_FAILED"},
		{name: "authorization", code: int16(kafkago.GroupAuthorizationFailed), want: "AUTHORIZATION_FAILED"},
		{name: "other broker error", code: int16(kafkago.InvalidGroupId), want: "OTHER_BROKER_ERROR"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyHeartbeatResponseCode(test.code); got != test.want {
				t.Fatalf("classification = %q, want %q", got, test.want)
			}
		})
	}
}

func TestDecodeHeartbeatFramesPreservesWireTupleAndCorrelation(t *testing.T) {
	requestWire := new(bytes.Buffer)
	request := &heartbeatapi.Request{
		GroupID:         "component-group",
		GenerationID:    71,
		MemberID:        "member-a",
		GroupInstanceID: "instance-a",
	}
	if err := protocol.WriteRequest(requestWire, 3, 90210, "recovery-component", request); err != nil {
		t.Fatalf("encode request: %v", err)
	}

	decodedRequest, err := decodeRequestFrame(requestWire.Bytes())
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if decodedRequest.APIKey != protocol.Heartbeat || decodedRequest.APIVersion != 3 || decodedRequest.CorrelationID != 90210 {
		t.Fatalf("request header = %#v", decodedRequest)
	}
	if decodedRequest.GroupID != "component-group" || decodedRequest.MemberID != "member-a" ||
		decodedRequest.GenerationID != 71 || decodedRequest.GroupInstanceID != "instance-a" {
		t.Fatalf("request tuple = %#v", decodedRequest)
	}

	responseWire := encodeHeartbeatResponseFrame(decodedRequest.APIVersion, decodedRequest.CorrelationID, int16(kafkago.IllegalGeneration), 0)
	decodedResponse, err := decodeResponseFrame(responseWire, decodedRequest.APIKey, decodedRequest.APIVersion)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if decodedResponse.CorrelationID != decodedRequest.CorrelationID || decodedResponse.ErrorCode == nil ||
		*decodedResponse.ErrorCode != int16(kafkago.IllegalGeneration) {
		t.Fatalf("response = %#v", decodedResponse)
	}
	if got := classifyHeartbeatResponseCode(*decodedResponse.ErrorCode); got != "ILLEGAL_GENERATION" {
		t.Fatalf("response classification = %q", got)
	}
}

func TestDecodeHeartbeatV4UsesFlexibleHeaderBeforeWireErrorCode(t *testing.T) {
	frame := make([]byte, 16)
	binary.BigEndian.PutUint32(frame[0:4], 12)
	binary.BigEndian.PutUint32(frame[4:8], 90210)
	frame[8] = 0                               // v4 flexible response-header tagged-field count
	binary.BigEndian.PutUint32(frame[9:13], 0) // throttle_time_ms
	binary.BigEndian.PutUint16(frame[13:15], uint16(kafkago.UnknownMemberId))
	frame[15] = 0 // v4 flexible response-body tagged-field count

	decoded, err := decodeResponseFrame(frame, protocol.Heartbeat, 4)
	if err != nil {
		t.Fatalf("decode v4 Heartbeat response: %v", err)
	}
	if decoded.ErrorCode == nil || *decoded.ErrorCode != int16(kafkago.UnknownMemberId) {
		t.Fatalf("wire error code = %d, want UNKNOWN_MEMBER_ID (%d)", valueOrZero16(decoded.ErrorCode), kafkago.UnknownMemberId)
	}
	if decoded.WireSchemaErrorCode == nil || *decoded.WireSchemaErrorCode != int16(kafkago.UnknownMemberId) {
		t.Fatalf("local wire-schema error code = %d, want UNKNOWN_MEMBER_ID (%d)", valueOrZero16(decoded.WireSchemaErrorCode), kafkago.UnknownMemberId)
	}
	if decoded.CodecErrorCode == nil || *decoded.CodecErrorCode != 0 {
		t.Fatalf("locked kafka-go typed error code = %d, want zero from misordered fields", valueOrZero16(decoded.CodecErrorCode))
	}
	if decoded.WireSchemaSource != "LOCAL_HEARTBEAT_WIRE_SCHEMA_V4" {
		t.Fatalf("wire-schema source = %q, want local wire-schema decoder", decoded.WireSchemaSource)
	}
	if decoded.CodeAgreement != "RAW_CODEC_DISAGREEMENT" {
		t.Fatalf("historical raw/typed comparison = %q, want disagreement", decoded.CodeAgreement)
	}
	if decoded.WireSchemaAgreement != "RAW_WIRE_SCHEMA_AGREE" {
		t.Fatalf("raw/wire-schema comparison = %q, want agreement", decoded.WireSchemaAgreement)
	}
	if decoded.TypedThrottleTimeMs == nil || *decoded.TypedThrottleTimeMs != int32(kafkago.UnknownMemberId) {
		t.Fatalf("typed throttle = %d, want the misread error value %d", valueOrZero32(decoded.TypedThrottleTimeMs), kafkago.UnknownMemberId)
	}
	if decoded.WireSchemaThrottleTimeMs == nil || *decoded.WireSchemaThrottleTimeMs != 0 {
		t.Fatalf("wire-schema throttle = %d, want 0", valueOrZero32(decoded.WireSchemaThrottleTimeMs))
	}
	if decoded.ErrorCodeByteOffset != 13 || decoded.FrameLength != len(frame) || decoded.FramePrefixHex != hex.EncodeToString(frame) {
		t.Fatalf("raw v4 response evidence = offset:%d length:%d prefix:%q", decoded.ErrorCodeByteOffset, decoded.FrameLength, decoded.FramePrefixHex)
	}
}

func TestDecodeHeartbeatResponseCodecMatchesWireSchemaForSupportedVersions(t *testing.T) {
	cases := []struct {
		version    int16
		errorCode  int16
		throttleMS int32
	}{
		{0, 0, 0},
		{0, int16(kafkago.IllegalGeneration), 0},
		{1, int16(kafkago.IllegalGeneration), 0},
		{1, int16(kafkago.UnknownMemberId), 1234},
		{2, int16(kafkago.RebalanceInProgress), 7},
		{3, int16(kafkago.IllegalGeneration), 99},
		{4, 0, 0},
		{4, int16(kafkago.IllegalGeneration), 0},
		{4, int16(kafkago.UnknownMemberId), 1234},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("v%d-code%d", tc.version, tc.errorCode), func(t *testing.T) {
			frame := encodeHeartbeatResponseFrame(tc.version, 42, tc.errorCode, tc.throttleMS)
			decoded, err := decodeResponseFrame(frame, protocol.Heartbeat, tc.version)
			if err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if decoded.ErrorCode == nil || *decoded.ErrorCode != tc.errorCode {
				t.Fatalf("raw error code = %d, want %d", valueOrZero16(decoded.ErrorCode), tc.errorCode)
			}
			if decoded.WireSchemaErrorCode == nil || *decoded.WireSchemaErrorCode != tc.errorCode {
				t.Fatalf("local wire-schema error code = %d, want %d", valueOrZero16(decoded.WireSchemaErrorCode), tc.errorCode)
			}
			if decoded.WireSchemaAgreement != "RAW_WIRE_SCHEMA_AGREE" {
				t.Fatalf("wire-schema agreement = %q, want RAW_WIRE_SCHEMA_AGREE", decoded.WireSchemaAgreement)
			}
			if decoded.WireSchemaSource == "" {
				t.Fatal("local wire-schema source is empty")
			}
		})
	}
}

func TestDecodeHeartbeatResponseCodecRejectsMalformedFrames(t *testing.T) {
	valid := encodeHeartbeatResponseFrame(4, 42, int16(kafkago.IllegalGeneration), 0)
	cases := map[string][]byte{
		"nonempty-header-tags": func() []byte {
			frame := append([]byte(nil), valid...)
			frame[8] = 1
			return frame
		}(),
		"nonempty-body-tags": func() []byte {
			frame := append([]byte(nil), valid...)
			frame[15] = 1
			return frame
		}(),
		"truncated": valid[:len(valid)-1],
		"trailing":  append(append([]byte(nil), valid...), 0),
		"unsupported-version": func() []byte {
			return encodeHeartbeatResponseFrame(5, 42, int16(kafkago.IllegalGeneration), 0)
		}(),
		"short": {0, 0, 0, 1},
	}
	for name, frame := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeHeartbeatResponseWireSchema(4, frame); err == nil &&
				name != "unsupported-version" {
				t.Fatalf("malformed frame %q was accepted", name)
			}
		})
	}
	if _, err := decodeHeartbeatResponseWireSchema(5, cases["unsupported-version"]); err == nil {
		t.Fatal("unsupported version was accepted")
	}
}

func TestDecodeHeartbeatV4NonemptyHeaderTagsLeavesRawClassificationUnknown(t *testing.T) {
	frame := make([]byte, 19)
	binary.BigEndian.PutUint32(frame[0:4], 15)
	binary.BigEndian.PutUint32(frame[4:8], 90210)
	frame[8] = 1  // one response-header tag
	frame[9] = 0  // tag ID 0
	frame[10] = 1 // one-byte tag payload
	frame[11] = 0
	binary.BigEndian.PutUint32(frame[12:16], 0) // throttle_time_ms
	binary.BigEndian.PutUint16(frame[16:18], uint16(kafkago.UnknownMemberId))
	frame[18] = 0 // v4 flexible response-body tagged-field count

	decoded, err := decodeResponseFrame(frame, protocol.Heartbeat, 4)
	if err != nil {
		t.Fatalf("decode v4 Heartbeat response with header tags: %v", err)
	}
	if decoded.ErrorCode != nil || decoded.WireSchemaAgreement != "WIRE_SCHEMA_HEADER_TAGS_PRESENT" ||
		decoded.WireSchemaHeaderTagCount != 1 ||
		decoded.WireSchemaErrorCode == nil || *decoded.WireSchemaErrorCode != int16(kafkago.UnknownMemberId) {
		t.Fatalf("tagged v4 response was given an unverified raw classification: %#v", decoded)
	}
	if decoded.FrameLength != len(frame) || decoded.FramePrefixHex != hex.EncodeToString(frame) {
		t.Fatalf("unclassified tagged response raw evidence = %#v", decoded)
	}
}

func TestTransportTapCorrelatesHeartbeatResponseOnSameConnection(t *testing.T) {
	exchanges, stats := roundTripThroughTap(t, 0)
	if len(exchanges) != 1 {
		t.Fatalf("captured exchanges = %d, want 1", len(exchanges))
	}
	exchange := exchanges[0]
	if !exchange.Matched || exchange.MatchStatus != "MATCHED" {
		t.Fatalf("match state = %#v", exchange)
	}
	if exchange.Role != "probe" || exchange.APIKey != int16(protocol.Heartbeat) ||
		exchange.APIVersion != 3 || exchange.CorrelationID != 90210 {
		t.Fatalf("captured header = %#v", exchange)
	}
	if exchange.GroupID != "component-group" || exchange.MemberID != "member-a" ||
		exchange.GenerationID != 71 || exchange.GroupInstanceID != "instance-a" {
		t.Fatalf("captured tuple = %#v", exchange)
	}
	if exchange.ResponseErrorCode == nil || *exchange.ResponseErrorCode != int16(kafkago.IllegalGeneration) ||
		exchange.ResponseClassification != "ILLEGAL_GENERATION" {
		t.Fatalf("captured response = %#v", exchange)
	}
	if exchange.RequestAt.IsZero() || exchange.ResponseAt == nil || exchange.ResponseAt.Before(exchange.RequestAt) {
		t.Fatalf("request/response timestamps = %s / %s", exchange.RequestAt, exchange.ResponseAt)
	}
	if stats.UnmatchedResponses != 0 {
		t.Fatalf("unmatched responses = %d, want 0", stats.UnmatchedResponses)
	}
}

func TestTransportTapCapturesFullHeartbeatRequestAndResponseFrames(t *testing.T) {
	exchanges, _ := roundTripThroughTap(t, 0)
	if len(exchanges) != 1 {
		t.Fatalf("captured exchanges = %d, want 1", len(exchanges))
	}

	requestWire := new(bytes.Buffer)
	if err := protocol.WriteRequest(requestWire, 3, 90210, "recovery-component",
		&heartbeatapi.Request{GroupID: "component-group", GenerationID: 71, MemberID: "member-a", GroupInstanceID: "instance-a"}); err != nil {
		t.Fatalf("encode expected request: %v", err)
	}
	responseWire := encodeHeartbeatResponseFrame(3, 90210, int16(kafkago.IllegalGeneration), 0)

	var fields map[string]any
	encoded, err := json.Marshal(exchanges[0])
	if err != nil {
		t.Fatalf("marshal exchange: %v", err)
	}
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("unmarshal exchange: %v", err)
	}
	if got := fields["requestFrameHex"]; got != hex.EncodeToString(requestWire.Bytes()) {
		t.Fatalf("requestFrameHex = %v, want full request %s", got, hex.EncodeToString(requestWire.Bytes()))
	}
	if got := fields["requestFrameLength"]; got != float64(requestWire.Len()) {
		t.Fatalf("requestFrameLength = %v, want %d", got, requestWire.Len())
	}
	if got := fields["requestFrameTruncated"]; got != false {
		t.Fatalf("requestFrameTruncated = %v, want false", got)
	}
	if got := fields["responseFrameHex"]; got != hex.EncodeToString(responseWire) {
		t.Fatalf("responseFrameHex = %v, want full response %s", got, hex.EncodeToString(responseWire))
	}
	if got := fields["responseFrameTruncated"]; got != false {
		t.Fatalf("responseFrameTruncated = %v, want false", got)
	}
}

func TestTransportTapRetainsMalformedResponseFrameAndDecoderError(t *testing.T) {
	collector := NewCollector(Identity{RunID: "unit-run", SourceRevision: "test-revision", Candidate: "test-candidate"})
	clientConn, serverConn := net.Pipe()
	tap := collector.newTapConn("probe", clientConn, "pipe-peer")
	t.Cleanup(func() {
		_ = tap.Close()
		_ = serverConn.Close()
	})

	malformed := make([]byte, 8)
	binary.BigEndian.PutUint32(malformed[:4], 4)
	binary.BigEndian.PutUint32(malformed[4:8], 90210)
	serverErr := make(chan error, 1)
	go func() {
		version, correlationID, _, message, err := protocol.ReadRequest(serverConn)
		if err != nil {
			serverErr <- err
			return
		}
		if version != 3 || correlationID != 90210 || message.ApiKey() != protocol.Heartbeat {
			serverErr <- errUnexpectedAPI
			return
		}
		_, err = serverConn.Write(malformed)
		serverErr <- err
	}()

	requestWire := new(bytes.Buffer)
	if err := protocol.WriteRequest(requestWire, 3, 90210, "recovery-component",
		&heartbeatapi.Request{GroupID: "component-group", GenerationID: 71, MemberID: "member-a", GroupInstanceID: "instance-a"}); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	if _, err := tap.Write(requestWire.Bytes()); err != nil {
		t.Fatalf("write request: %v", err)
	}
	_, _, callErr := protocol.ReadResponse(tap, protocol.Heartbeat, 3)
	if callErr == nil {
		t.Fatal("malformed Heartbeat response decoded without error")
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server response: %v", err)
	}

	partial, decodeErr := decodeResponseFrame(malformed, protocol.Heartbeat, 3)
	if decodeErr == nil {
		t.Fatal("malformed response frame decoded without error")
	}
	if partial.FrameLength != len(malformed) {
		t.Fatalf("partial response frame length = %d, want %d", partial.FrameLength, len(malformed))
	}

	exchanges := collector.Snapshot()
	if len(exchanges) != 1 {
		t.Fatalf("captured exchanges = %d, want 1", len(exchanges))
	}
	var fields map[string]any
	encoded, err := json.Marshal(exchanges[0])
	if err != nil {
		t.Fatalf("marshal exchange: %v", err)
	}
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("unmarshal exchange: %v", err)
	}
	if got := fields["responseFrameHex"]; got != hex.EncodeToString(malformed) {
		t.Fatalf("responseFrameHex = %v, want %s", got, hex.EncodeToString(malformed))
	}
	if got := fields["responseDecoderError"]; got != decodeErr.Error() {
		t.Fatalf("responseDecoderError = %v, want %q", got, decodeErr)
	}
	if got := fields["matchStatus"]; got != "RESPONSE_DECODE_ERROR" {
		t.Fatalf("matchStatus = %v, want RESPONSE_DECODE_ERROR", got)
	}
}

func TestTransportTapRegistersRequestBeforeProcessingFastResponse(t *testing.T) {
	collector := NewCollector(Identity{RunID: "unit-run", SourceRevision: "test-revision", Candidate: "test-candidate"})
	clientConn, serverConn := net.Pipe()
	allowWriteReturn := make(chan struct{})
	var allowWriteReturnOnce sync.Once
	releaseWriteReturn := func() { allowWriteReturnOnce.Do(func() { close(allowWriteReturn) }) }
	gatedConn := &writeReturnGateConn{
		Conn:                clientConn,
		wireWriteComplete:   make(chan struct{}),
		allowWriteReturn:    allowWriteReturn,
		responseReadStarted: make(chan struct{}),
	}
	tap := collector.newTapConn("probe", gatedConn, "pipe-peer")
	t.Cleanup(func() {
		releaseWriteReturn()
		_ = tap.Close()
		_ = serverConn.Close()
	})

	serverDone := make(chan error, 1)
	go func() {
		version, correlationID, _, message, err := protocol.ReadRequest(serverConn)
		if err != nil {
			serverDone <- err
			return
		}
		if message.ApiKey() != protocol.Heartbeat {
			serverDone <- errUnexpectedAPI
			return
		}
		_, err = serverConn.Write(encodeHeartbeatResponseFrame(version, correlationID, int16(kafkago.IllegalGeneration), 0))
		serverDone <- err
	}()

	type readResult struct {
		err error
	}
	readDone := make(chan readResult, 1)
	go func() {
		_, err := tap.Read(make([]byte, 64))
		readDone <- readResult{err: err}
	}()

	requestWire := new(bytes.Buffer)
	if err := protocol.WriteRequest(requestWire, 3, 90210, "recovery-component",
		&heartbeatapi.Request{GroupID: "component-group", GenerationID: 71, MemberID: "member-a", GroupInstanceID: "instance-a"}); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := tap.Write(requestWire.Bytes())
		writeDone <- err
	}()

	select {
	case <-gatedConn.wireWriteComplete:
	case <-time.After(2 * time.Second):
		t.Fatal("underlying request write did not reach the broker")
	}
	select {
	case <-gatedConn.responseReadStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("fast broker response was not delivered to the tap reader")
	}

	responseReadFinishedBeforeWriteReturn := false
	select {
	case result := <-readDone:
		responseReadFinishedBeforeWriteReturn = true
		if result.err != nil {
			t.Fatalf("read fast response before write return: %v", result.err)
		}
	case <-time.After(100 * time.Millisecond):
	}
	releaseWriteReturn()

	if err := <-writeDone; err != nil {
		t.Fatalf("write request: %v", err)
	}
	if !responseReadFinishedBeforeWriteReturn {
		if result := <-readDone; result.err != nil {
			t.Fatalf("read response after request registration: %v", result.err)
		}
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("broker response: %v", err)
	}
	if responseReadFinishedBeforeWriteReturn {
		t.Fatal("tap processed the broker response before the request was registered by Write")
	}

	exchanges := collector.Snapshot()
	if len(exchanges) != 1 || !exchanges[0].Matched || exchanges[0].MatchStatus != "MATCHED" {
		t.Fatalf("fast response exchange = %#v, want one matched request/response", exchanges)
	}
	if stats := collector.Stats(); stats.UnmatchedResponses != 0 {
		t.Fatalf("unmatched responses = %d, want 0", stats.UnmatchedResponses)
	}
}

func TestTransportTapNeverAssociatesDifferentCorrelationID(t *testing.T) {
	exchanges, stats := roundTripThroughTap(t, 1)
	if len(exchanges) != 1 {
		t.Fatalf("captured exchanges = %d, want 1", len(exchanges))
	}
	exchange := exchanges[0]
	if exchange.Matched || exchange.MatchStatus != "REQUEST_ONLY" || exchange.ResponseErrorCode != nil {
		t.Fatalf("mismatched response was accepted: %#v", exchange)
	}
	if stats.UnmatchedResponses != 1 {
		t.Fatalf("unmatched responses = %d, want 1", stats.UnmatchedResponses)
	}
}

func roundTripThroughTap(t *testing.T, responseCorrelationDelta int32) ([]Exchange, CollectorStats) {
	t.Helper()

	collector := NewCollector(Identity{RunID: "unit-run", SourceRevision: "test-revision", Candidate: "test-candidate"})
	clientConn, serverConn := net.Pipe()
	tap := collector.newTapConn("probe", clientConn, "pipe-peer")
	t.Cleanup(func() {
		_ = tap.Close()
		_ = serverConn.Close()
	})

	serverErr := make(chan error, 1)
	go func() {
		version, correlationID, _, message, err := protocol.ReadRequest(serverConn)
		if err != nil {
			serverErr <- err
			return
		}
		if message.ApiKey() != protocol.Heartbeat {
			serverErr <- errUnexpectedAPI
			return
		}
		_, err = serverConn.Write(encodeHeartbeatResponseFrame(version, correlationID+responseCorrelationDelta, int16(kafkago.IllegalGeneration), 0))
		serverErr <- err
	}()

	requestWire := new(bytes.Buffer)
	if err := protocol.WriteRequest(requestWire, 3, 90210, "recovery-component",
		&heartbeatapi.Request{GroupID: "component-group", GenerationID: 71, MemberID: "member-a", GroupInstanceID: "instance-a"}); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	if _, err := tap.Write(requestWire.Bytes()); err != nil {
		t.Fatalf("write request: %v", err)
	}
	correlationID, _, err := protocol.ReadResponse(tap, protocol.Heartbeat, 3)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if want := int32(90210) + responseCorrelationDelta; correlationID != want {
		t.Fatalf("response correlation ID = %d, want %d", correlationID, want)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server round trip: %v", err)
	}
	return collector.Snapshot(), collector.Stats()
}

func encodeHeartbeatResponseFrame(version int16, correlationID int32, errorCode int16, throttleTimeMs int32) []byte {
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body, uint32(correlationID))
	if version == 4 {
		body = append(body, 0) // flexible response-header tagged fields
	}
	if version > 0 {
		throttle := make([]byte, 4)
		binary.BigEndian.PutUint32(throttle, uint32(throttleTimeMs))
		body = append(body, throttle...)
	}
	code := make([]byte, 2)
	binary.BigEndian.PutUint16(code, uint16(errorCode))
	body = append(body, code...)
	if version == 4 {
		body = append(body, 0) // flexible response-body tagged fields
	}
	frame := make([]byte, 4, len(body)+4)
	binary.BigEndian.PutUint32(frame, uint32(len(body)))
	return append(frame, body...)
}

func valueOrZero16(value *int16) int16 {
	if value == nil {
		return 0
	}
	return *value
}

func valueOrZero32(value *int32) int32 {
	if value == nil {
		return 0
	}
	return *value
}

type writeReturnGateConn struct {
	net.Conn
	wireWriteComplete   chan struct{}
	allowWriteReturn    <-chan struct{}
	responseReadStarted chan struct{}
	readOnce            sync.Once
}

func (c *writeReturnGateConn) Write(buffer []byte) (int, error) {
	n, err := c.Conn.Write(buffer)
	if n > 0 {
		close(c.wireWriteComplete)
	}
	if err != nil {
		return n, err
	}
	<-c.allowWriteReturn
	return n, nil
}

func (c *writeReturnGateConn) Read(buffer []byte) (int, error) {
	n, err := c.Conn.Read(buffer)
	if n > 0 {
		c.readOnce.Do(func() { close(c.responseReadStarted) })
	}
	return n, err
}

func TestTransportTapAttachesMatchedClusterAndCoordinatorFacts(t *testing.T) {
	collector := NewCollector(Identity{RunID: "unit-run", SourceRevision: "test-revision", Candidate: "test-candidate"})
	clientConn, serverConn := net.Pipe()
	tap := collector.newTapConn("probe", clientConn, "broker-4:9092")
	t.Cleanup(func() {
		_ = tap.Close()
		_ = serverConn.Close()
	})

	serverErr := make(chan error, 3)
	go func() {
		for i := 0; i < 3; i++ {
			version, correlationID, _, message, err := protocol.ReadRequest(serverConn)
			if err != nil {
				serverErr <- err
				return
			}
			var response protocol.Message
			switch request := message.(type) {
			case *metadataapi.Request:
				response = &metadataapi.Response{ClusterID: "isolated-cluster"}
			case *findcoordinatorapi.Request:
				if request.Key != "component-group" || request.KeyType != 0 {
					serverErr <- errUnexpectedAPI
					return
				}
				response = &findcoordinatorapi.Response{NodeID: 4, Host: "broker-4", Port: 9092}
			case *heartbeatapi.Request:
				response = &heartbeatapi.Response{ErrorCode: int16(kafkago.IllegalGeneration)}
			default:
				serverErr <- errUnexpectedAPI
				return
			}
			if heartbeatResponse, ok := response.(*heartbeatapi.Response); ok {
				_, err = serverConn.Write(encodeHeartbeatResponseFrame(version, correlationID, heartbeatResponse.ErrorCode, heartbeatResponse.ThrottleTimeMs))
			} else {
				responseWire := new(bytes.Buffer)
				if err := protocol.WriteResponse(responseWire, version, correlationID, response); err != nil {
					serverErr <- err
					return
				}
				_, err = serverConn.Write(responseWire.Bytes())
			}
			serverErr <- err
		}
	}()

	requests := []struct {
		version int16
		corrID  int32
		apiKey  protocol.ApiKey
		message protocol.Message
	}{
		{version: 2, corrID: 1, apiKey: protocol.Metadata, message: &metadataapi.Request{}},
		{version: 2, corrID: 2, apiKey: protocol.FindCoordinator, message: &findcoordinatorapi.Request{Key: "component-group", KeyType: 0}},
		{version: 3, corrID: 3, apiKey: protocol.Heartbeat, message: &heartbeatapi.Request{GroupID: "component-group", GenerationID: 71, MemberID: "member-a"}},
	}
	for _, request := range requests {
		wire := new(bytes.Buffer)
		if err := protocol.WriteRequest(wire, request.version, request.corrID, "recovery-component", request.message); err != nil {
			t.Fatalf("encode API %d: %v", request.apiKey, err)
		}
		if _, err := tap.Write(wire.Bytes()); err != nil {
			t.Fatalf("write API %d: %v", request.apiKey, err)
		}
		correlationID, _, err := protocol.ReadResponse(tap, request.apiKey, request.version)
		if err != nil {
			t.Fatalf("read API %d response: %v", request.apiKey, err)
		}
		if correlationID != request.corrID {
			t.Fatalf("API %d correlation = %d, want %d", request.apiKey, correlationID, request.corrID)
		}
	}
	for range requests {
		if err := <-serverErr; err != nil {
			t.Fatalf("server response: %v", err)
		}
	}

	var heartbeat *Exchange
	for _, exchange := range collector.Snapshot() {
		if exchange.APIKey == int16(protocol.Heartbeat) {
			copy := exchange
			heartbeat = &copy
			break
		}
	}
	if heartbeat == nil || !heartbeat.Matched {
		t.Fatalf("captured heartbeat exchange = %#v", heartbeat)
	}
	if heartbeat.ClusterID != "isolated-cluster" {
		t.Fatalf("heartbeat cluster ID = %q", heartbeat.ClusterID)
	}
	if heartbeat.CoordinatorNodeID == nil || *heartbeat.CoordinatorNodeID != 4 ||
		heartbeat.CoordinatorHost != "broker-4" || heartbeat.CoordinatorPort != 9092 {
		t.Fatalf("heartbeat coordinator = %#v", heartbeat)
	}
}

func TestClassifyCallOutcomeKeepsLocalFailuresSeparateFromBrokerResponses(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name    string
		ctxErr  error
		callErr error
		want    string
	}{
		{name: "local cancellation", ctxErr: cancelled.Err(), want: "LOCAL_CANCELLED"},
		{name: "context deadline", ctxErr: context.DeadlineExceeded, want: "TIMEOUT"},
		{name: "network timeout", callErr: &net.OpError{Op: "read", Net: "tcp", Err: context.DeadlineExceeded}, want: "TIMEOUT"},
		{name: "network failure", callErr: &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset")}, want: "NETWORK_ERROR"},
		{name: "broker response returned", want: "CALL_RETURNED"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyCallOutcome(test.ctxErr, test.callErr); got != test.want {
				t.Fatalf("call outcome = %q, want %q", got, test.want)
			}
		})
	}
}
