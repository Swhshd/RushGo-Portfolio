package kafkacollector

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/consumer"
	findcoordinatorapi "github.com/segmentio/kafka-go/protocol/findcoordinator"
	heartbeatapi "github.com/segmentio/kafka-go/protocol/heartbeat"
	joingroupapi "github.com/segmentio/kafka-go/protocol/joingroup"
	leavegroupapi "github.com/segmentio/kafka-go/protocol/leavegroup"
	metadataapi "github.com/segmentio/kafka-go/protocol/metadata"
	offsetcommitapi "github.com/segmentio/kafka-go/protocol/offsetcommit"
	offsetfetchapi "github.com/segmentio/kafka-go/protocol/offsetfetch"
	syncgroupapi "github.com/segmentio/kafka-go/protocol/syncgroup"
)

const maxCapturedFrameBytes = 1 << 20

type wireRequest struct {
	APIKey             protocol.ApiKey
	APIVersion         int16
	CorrelationID      int32
	ClientID           string
	GroupID            string
	MemberID           string
	GenerationID       int32
	GroupInstanceID    string
	CoordinatorKeyType int8
	ProtocolType       string
	ProtocolName       string
	OffsetCommit       []wireOffset
}

type heartbeatWireDecode struct {
	ErrorCode      int16
	ThrottleTimeMs int32
	HeaderTagCount int
	BodyTagCount   int
	Source         string
}

type wireAssignment struct {
	Topic      string
	Partitions []int32
}

type wireOffset struct {
	Topic           string
	Partition       int32
	CommittedOffset int64
	ErrorCode       int16
}

type wireResponse struct {
	APIKey                   protocol.ApiKey
	APIVersion               int16
	CorrelationID            int32
	ErrorCode                *int16
	CodecErrorCode           *int16
	WireSchemaErrorCode      *int16
	TypedThrottleTimeMs      *int32
	WireSchemaThrottleTimeMs *int32
	WireSchemaSource         string
	WireSchemaHeaderTagCount int
	WireSchemaBodyTagCount   int
	ErrorCodeByteOffset      int
	CodeAgreement            string
	WireSchemaAgreement      string
	FrameLength              int
	FramePrefixHex           string
	FrameHex                 string
	FrameHexTruncated        bool
	MemberErrorCode          *int16
	GenerationID             *int32
	MemberID                 string
	GroupInstanceID          string
	ProtocolType             string
	ProtocolName             string
	AssignmentVersion        int16
	Assignments              []wireAssignment
	AssignmentError          string
	OffsetFetchErrorCode     *int16
	OffsetFetchCommitted     []wireOffset
	OffsetCommitResults      []wireOffset
	ClusterID                string
	CoordinatorNodeID        *int32
	CoordinatorHost          string
	CoordinatorPort          int32
}

func decodeRequestFrame(frame []byte) (wireRequest, error) {
	version, correlationID, clientID, message, err := protocol.ReadRequest(bytes.NewReader(frame))
	if err != nil {
		return wireRequest{}, fmt.Errorf("decode Kafka request frame: %w", err)
	}
	request := wireRequest{
		APIKey:        message.ApiKey(),
		APIVersion:    version,
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	switch typed := message.(type) {
	case *heartbeatapi.Request:
		request.GroupID = typed.GroupID
		request.MemberID = typed.MemberID
		request.GenerationID = typed.GenerationID
		request.GroupInstanceID = typed.GroupInstanceID
	case *findcoordinatorapi.Request:
		request.GroupID = typed.Key
		request.CoordinatorKeyType = typed.KeyType
	case *joingroupapi.Request:
		request.GroupID = typed.GroupID
		request.MemberID = typed.MemberID
		request.GroupInstanceID = typed.GroupInstanceID
		request.ProtocolType = typed.ProtocolType
		if len(typed.Protocols) > 0 {
			request.ProtocolName = typed.Protocols[0].Name
		}
	case *leavegroupapi.Request:
		request.GroupID = typed.GroupID
		request.MemberID = typed.MemberID
		if len(typed.Members) > 0 {
			request.MemberID = typed.Members[0].MemberID
			request.GroupInstanceID = typed.Members[0].GroupInstanceID
		}
	case *offsetcommitapi.Request:
		request.GroupID = typed.GroupID
		request.MemberID = typed.MemberID
		request.GenerationID = typed.GenerationID
		request.GroupInstanceID = typed.GroupInstanceID
		for _, topic := range typed.Topics {
			for _, partition := range topic.Partitions {
				request.OffsetCommit = append(request.OffsetCommit, wireOffset{
					Topic: topic.Name, Partition: partition.PartitionIndex,
					CommittedOffset: partition.CommittedOffset,
				})
			}
		}
	case *offsetfetchapi.Request:
		request.GroupID = typed.GroupID
	case *syncgroupapi.Request:
		request.GroupID = typed.GroupID
		request.MemberID = typed.MemberID
		request.GenerationID = typed.GenerationID
		request.GroupInstanceID = typed.GroupInstanceID
		request.ProtocolType = typed.ProtocolType
		request.ProtocolName = typed.ProtocolName
	case *metadataapi.Request:
	default:
		return wireRequest{}, fmt.Errorf("Kafka API %d is not a captured request type", message.ApiKey())
	}
	return request, nil
}

func decodeResponseFrame(frame []byte, apiKey protocol.ApiKey, apiVersion int16) (wireResponse, error) {
	frameLength, framePrefixHex, frameHex, frameHexTruncated := captureFrameEvidence(frame)
	response := wireResponse{
		APIKey:            apiKey,
		APIVersion:        apiVersion,
		FrameLength:       frameLength,
		FramePrefixHex:    framePrefixHex,
		FrameHex:          frameHex,
		FrameHexTruncated: frameHexTruncated,
	}
	correlationID, message, err := protocol.ReadResponse(bytes.NewReader(frame), apiKey, apiVersion)
	if err != nil {
		return response, fmt.Errorf("decode Kafka response frame: %w", err)
	}
	response.CorrelationID = correlationID
	switch typed := message.(type) {
	case *heartbeatapi.Response:
		// Preserve the locked kafka-go typed value under its historical field.
		typedCode := typed.ErrorCode
		typedThrottle := typed.ThrottleTimeMs
		response.CodecErrorCode = &typedCode
		response.TypedThrottleTimeMs = &typedThrottle
		local, codecErr := decodeHeartbeatResponseWireSchema(apiVersion, frame)
		if codecErr == nil {
			response.WireSchemaErrorCode = &local.ErrorCode
			response.WireSchemaThrottleTimeMs = &local.ThrottleTimeMs
			response.WireSchemaSource = local.Source
			response.WireSchemaHeaderTagCount = local.HeaderTagCount
			response.WireSchemaBodyTagCount = local.BodyTagCount
		} else {
			response.WireSchemaSource = "LOCAL_WIRE_SCHEMA_UNSUPPORTED_OR_MALFORMED"
		}
		offset, offsetStatus, offsetErr := heartbeatResponseErrorCodeOffset(apiVersion, frame)
		if offsetErr != nil {
			return response, offsetErr
		}
		if offsetStatus == "RAW_OFFSET_UNKNOWN_NONEMPTY_HEADER_TAGS" {
			response.WireSchemaAgreement = "WIRE_SCHEMA_HEADER_TAGS_PRESENT"
			return response, nil
		}
		wireCode := int16(binary.BigEndian.Uint16(frame[offset : offset+2]))
		response.ErrorCode = &wireCode
		response.ErrorCodeByteOffset = offset
		if wireCode == typedCode {
			response.CodeAgreement = "RAW_CODEC_AGREE"
		} else {
			response.CodeAgreement = "RAW_CODEC_DISAGREEMENT"
		}
		if codecErr != nil {
			response.WireSchemaAgreement = "WIRE_SCHEMA_DECODE_UNAVAILABLE"
			return response, codecErr
		}
		if local.HeaderTagCount != 0 || local.BodyTagCount != 0 {
			response.WireSchemaAgreement = "WIRE_SCHEMA_TAGS_PRESENT"
		} else if wireCode == local.ErrorCode {
			response.WireSchemaAgreement = "RAW_WIRE_SCHEMA_AGREE"
		} else {
			response.WireSchemaAgreement = "RAW_WIRE_SCHEMA_DISAGREEMENT"
		}
	case *findcoordinatorapi.Response:
		code := typed.ErrorCode
		nodeID := typed.NodeID
		response.ErrorCode = &code
		response.CoordinatorNodeID = &nodeID
		response.CoordinatorHost = typed.Host
		response.CoordinatorPort = typed.Port
	case *joingroupapi.Response:
		code := typed.ErrorCode
		generationID := typed.GenerationID
		response.ErrorCode = &code
		response.GenerationID = &generationID
		response.MemberID = typed.MemberID
		response.ProtocolType = typed.ProtocolType
		response.ProtocolName = typed.ProtocolName
	case *leavegroupapi.Response:
		code := typed.ErrorCode
		response.ErrorCode = &code
		if len(typed.Members) > 0 {
			memberCode := typed.Members[0].ErrorCode
			response.MemberErrorCode = &memberCode
			response.MemberID = typed.Members[0].MemberID
		}
	case *syncgroupapi.Response:
		code := typed.ErrorCode
		response.ErrorCode = &code
		response.ProtocolType = typed.ProtocolType
		response.ProtocolName = typed.ProtocolName
		if typed.ErrorCode == 0 && len(typed.Assignments) > 0 {
			var assignment consumer.Assignment
			if err := protocol.Unmarshal(typed.Assignments, consumer.MaxVersionSupported, &assignment); err != nil {
				response.AssignmentError = "SYNCGROUP_ASSIGNMENT_DECODE_FAILED"
			} else {
				response.AssignmentVersion = assignment.Version
				for _, topic := range assignment.AssignedPartitions {
					partitions := append([]int32(nil), topic.Partitions...)
					response.Assignments = append(response.Assignments, wireAssignment{
						Topic: topic.Topic, Partitions: partitions,
					})
				}
			}
		} else if typed.ErrorCode == 0 {
			response.AssignmentError = "SYNCGROUP_ASSIGNMENT_EMPTY"
		}
	case *offsetfetchapi.Response:
		code := typed.ErrorCode
		response.ErrorCode = &code
		response.OffsetFetchErrorCode = &code
		for _, topic := range typed.Topics {
			for _, partition := range topic.Partitions {
				response.OffsetFetchCommitted = append(response.OffsetFetchCommitted, wireOffset{
					Topic: topic.Name, Partition: partition.PartitionIndex,
					CommittedOffset: partition.CommittedOffset, ErrorCode: partition.ErrorCode,
				})
			}
		}
	case *offsetcommitapi.Response:
		for _, topic := range typed.Topics {
			for _, partition := range topic.Partitions {
				response.OffsetCommitResults = append(response.OffsetCommitResults, wireOffset{
					Topic: topic.Name, Partition: partition.PartitionIndex,
					ErrorCode: partition.ErrorCode,
				})
			}
		}
	case *metadataapi.Response:
		response.ClusterID = typed.ClusterID
	default:
		return response, fmt.Errorf("Kafka API %d is not a captured response type", apiKey)
	}
	return response, nil
}

func captureFrameEvidence(frame []byte) (int, string, string, bool) {
	prefixLength := len(frame)
	if prefixLength > 32 {
		prefixLength = 32
	}
	capturedLength := len(frame)
	if capturedLength > maxCapturedFrameBytes {
		capturedLength = maxCapturedFrameBytes
	}
	return len(frame), hex.EncodeToString(frame[:prefixLength]), hex.EncodeToString(frame[:capturedLength]), capturedLength < len(frame)
}

// decodeHeartbeatResponseWireSchema is a collector-local, versioned Heartbeat
// response decoder. The locked kafka-go v0.4.50 heartbeat Response struct
// declares ErrorCode before ThrottleTimeMs, while the Kafka wire schema for
// v1-v4 places throttle_time_ms first. That mismatch makes the locked typed
// codec read the throttle high bits as the error code (typically zero). This
// decoder follows the wire schema directly and is intentionally separate from
// the raw offset parser. The historical responseCodecErrorCode field keeps the
// locked typed value; the corrected value is reported separately.
func decodeHeartbeatResponseWireSchema(apiVersion int16, frame []byte) (heartbeatWireDecode, error) {
	if len(frame) < 4 || int(binary.BigEndian.Uint32(frame[:4])) != len(frame)-4 {
		return heartbeatWireDecode{}, fmt.Errorf("Heartbeat response length prefix does not match frame")
	}
	if len(frame) < 8 {
		return heartbeatWireDecode{}, fmt.Errorf("Heartbeat response frame omits correlation id")
	}
	body := 8
	headerTags := 0
	if apiVersion == 4 {
		count, next, err := skipTaggedFields(frame, body)
		if err != nil {
			return heartbeatWireDecode{}, fmt.Errorf("Heartbeat v4 response header tags: %w", err)
		}
		headerTags = count
		body = next
	}
	switch apiVersion {
	case 0:
		if len(frame) != body+2 {
			return heartbeatWireDecode{}, fmt.Errorf("Heartbeat v0 response length %d is not %d", len(frame), body+2)
		}
		return heartbeatWireDecode{
			ErrorCode:      int16(binary.BigEndian.Uint16(frame[body : body+2])),
			HeaderTagCount: headerTags,
			Source:         "LOCAL_HEARTBEAT_WIRE_SCHEMA_V0",
		}, nil
	case 1, 2, 3, 4:
		if len(frame) < body+6 {
			return heartbeatWireDecode{}, fmt.Errorf("Heartbeat v%d response omits throttle/error fields", apiVersion)
		}
		throttle := int32(binary.BigEndian.Uint32(frame[body : body+4]))
		code := int16(binary.BigEndian.Uint16(frame[body+4 : body+6]))
		bodyTags := 0
		next := body + 6
		if apiVersion == 4 {
			count, after, err := skipTaggedFields(frame, body+6)
			if err != nil {
				return heartbeatWireDecode{}, fmt.Errorf("Heartbeat v4 response body tags: %w", err)
			}
			bodyTags = count
			next = after
		}
		if next != len(frame) {
			return heartbeatWireDecode{}, fmt.Errorf("Heartbeat v%d response has unexpected trailing bytes", apiVersion)
		}
		return heartbeatWireDecode{
			ErrorCode:      code,
			ThrottleTimeMs: throttle,
			HeaderTagCount: headerTags,
			BodyTagCount:   bodyTags,
			Source:         fmt.Sprintf("LOCAL_HEARTBEAT_WIRE_SCHEMA_V%d", apiVersion),
		}, nil
	default:
		return heartbeatWireDecode{}, fmt.Errorf("unsupported Heartbeat response version %d", apiVersion)
	}
}

func skipTaggedFields(frame []byte, offset int) (int, int, error) {
	if offset < 0 || offset > len(frame) {
		return 0, 0, fmt.Errorf("tag buffer offset %d is outside frame", offset)
	}
	count, countBytes, err := readUnsignedVarint(frame[offset:])
	if err != nil {
		return 0, 0, err
	}
	if count > 1024 {
		return 0, 0, fmt.Errorf("tag count %d exceeds collection bound", count)
	}
	offset += countBytes
	for index := uint64(0); index < count; index++ {
		_, idBytes, err := readUnsignedVarint(frame[offset:])
		if err != nil {
			return 0, 0, fmt.Errorf("tag %d id: %w", index, err)
		}
		offset += idBytes
		size, sizeBytes, err := readUnsignedVarint(frame[offset:])
		if err != nil {
			return 0, 0, fmt.Errorf("tag %d size: %w", index, err)
		}
		offset += sizeBytes
		if size > uint64(len(frame)-offset) {
			return 0, 0, fmt.Errorf("tag %d payload exceeds frame", index)
		}
		offset += int(size)
	}
	return int(count), offset, nil
}

func readUnsignedVarint(data []byte) (uint64, int, error) {
	var value uint64
	for index := 0; index < len(data) && index < 10; index++ {
		current := data[index]
		if index == 9 && current > 1 {
			return 0, 0, fmt.Errorf("unsigned varint overflows 64 bits")
		}
		value |= uint64(current&0x7f) << (7 * index)
		if current&0x80 == 0 {
			return value, index + 1, nil
		}
	}
	return 0, 0, fmt.Errorf("truncated unsigned varint")
}

func heartbeatResponseErrorCodeOffset(apiVersion int16, frame []byte) (int, string, error) {
	var offset int
	switch apiVersion {
	case 0:
		offset = 8
	case 1, 2, 3:
		offset = 12 // throttle_time_ms precedes error_code in the v1-v3 body.
	case 4:
		if len(frame) < 9 {
			return 0, "", fmt.Errorf("Heartbeat v4 response frame omits its flexible response header")
		}
		if frame[8] != 0 {
			return 0, "RAW_OFFSET_UNKNOWN_NONEMPTY_HEADER_TAGS", nil
		}
		offset = 13 // size + correlation + empty header tags + throttle_time_ms.
	default:
		return 0, "", fmt.Errorf("unsupported Heartbeat response version %d for raw error-code offset", apiVersion)
	}
	if offset < 0 || len(frame) < offset+2 {
		return 0, "", fmt.Errorf("Heartbeat response v%d frame is too short for error code at byte offset %d", apiVersion, offset)
	}
	return offset, "RAW_OFFSET_VERIFIED", nil
}

func classifyHeartbeatResponseCode(code int16) string {
	switch kafkago.Error(code) {
	case 0:
		return "SUCCESS_ACCEPTED_AT_RESPONSE_TIME"
	case kafkago.IllegalGeneration:
		return "ILLEGAL_GENERATION"
	case kafkago.UnknownMemberId:
		return "UNKNOWN_MEMBER_ID"
	case kafkago.RebalanceInProgress:
		return "REBALANCE_IN_PROGRESS"
	case kafkago.GroupCoordinatorNotAvailable, kafkago.GroupLoadInProgress:
		return "COORDINATOR_NOT_READY"
	case kafkago.NotCoordinatorForGroup:
		return "COORDINATOR_MIGRATION"
	case kafkago.SASLAuthenticationFailed:
		return "AUTHENTICATION_FAILED"
	case kafkago.GroupAuthorizationFailed, kafkago.ClusterAuthorizationFailed:
		return "AUTHORIZATION_FAILED"
	default:
		return "OTHER_BROKER_ERROR"
	}
}
