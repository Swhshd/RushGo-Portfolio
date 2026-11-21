package kafkacollector

import (
	"bytes"
	"testing"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/consumer"
	offsetcommitapi "github.com/segmentio/kafka-go/protocol/offsetcommit"
	offsetfetchapi "github.com/segmentio/kafka-go/protocol/offsetfetch"
	syncgroupapi "github.com/segmentio/kafka-go/protocol/syncgroup"
)

func responseFrame(t *testing.T, version int16, correlationID int32, message protocol.Message) []byte {
	t.Helper()
	var buffer bytes.Buffer
	if err := protocol.WriteResponse(&buffer, version, correlationID, message); err != nil {
		t.Fatalf("write response frame: %v", err)
	}
	return buffer.Bytes()
}

func TestDecodeSyncGroupResponseCapturesAssignment(t *testing.T) {
	assignment, err := protocol.Marshal(consumer.MaxVersionSupported, consumer.Assignment{
		Version: consumer.MaxVersionSupported,
		AssignedPartitions: []consumer.TopicPartition{
			{Topic: "booking.events", Partitions: []int32{0, 2}},
		},
		UserData: []byte("assignment-user-data"),
	})
	if err != nil {
		t.Fatal(err)
	}
	frame := responseFrame(t, 5, 17, &syncgroupapi.Response{
		ErrorCode: 0, ProtocolType: "consumer", ProtocolName: "range", Assignments: assignment,
	})
	response, err := decodeResponseFrame(frame, protocol.SyncGroup, 5)
	if err != nil {
		t.Fatal(err)
	}
	if response.CorrelationID != 17 || response.ErrorCode == nil || *response.ErrorCode != 0 {
		t.Fatalf("header/error mismatch: %+v", response)
	}
	if response.ProtocolType != "consumer" || response.ProtocolName != "range" {
		t.Fatalf("protocol mismatch: %+v", response)
	}
	if response.AssignmentVersion != consumer.MaxVersionSupported || response.AssignmentError != "" {
		t.Fatalf("assignment decode failed: %+v", response)
	}
	if len(response.Assignments) != 1 || response.Assignments[0].Topic != "booking.events" ||
		len(response.Assignments[0].Partitions) != 2 ||
		response.Assignments[0].Partitions[0] != 0 || response.Assignments[0].Partitions[1] != 2 {
		t.Fatalf("assignment facts mismatch: %+v", response.Assignments)
	}
}

func TestDecodeSyncGroupResponseRejectsMalformedAssignment(t *testing.T) {
	frame := responseFrame(t, 5, 18, &syncgroupapi.Response{
		ErrorCode: 0, ProtocolType: "consumer", ProtocolName: "range",
		Assignments: []byte{0, 1, 0, 0, 0, 9, 'x'},
	})
	response, err := decodeResponseFrame(frame, protocol.SyncGroup, 5)
	if err != nil {
		t.Fatal(err)
	}
	if response.AssignmentError != "SYNCGROUP_ASSIGNMENT_DECODE_FAILED" {
		t.Fatalf("malformed assignment was not rejected: %+v", response)
	}
}

func TestDecodeSyncGroupErrorDoesNotInventAssignment(t *testing.T) {
	frame := responseFrame(t, 5, 19, &syncgroupapi.Response{ErrorCode: 22})
	response, err := decodeResponseFrame(frame, protocol.SyncGroup, 5)
	if err != nil {
		t.Fatal(err)
	}
	if response.ErrorCode == nil || *response.ErrorCode != 22 || len(response.Assignments) != 0 ||
		response.AssignmentError != "" {
		t.Fatalf("rejected SyncGroup was given an assignment: %+v", response)
	}
}

func TestDecodeOffsetFetchResponseCapturesCommittedFrontier(t *testing.T) {
	frame := responseFrame(t, 5, 20, &offsetfetchapi.Response{
		ErrorCode: 0,
		Topics: []offsetfetchapi.ResponseTopic{{
			Name: "booking.events",
			Partitions: []offsetfetchapi.ResponsePartition{{
				PartitionIndex: 0, CommittedOffset: 41, ErrorCode: 0,
			}},
		}},
	})
	response, err := decodeResponseFrame(frame, protocol.OffsetFetch, 5)
	if err != nil {
		t.Fatal(err)
	}
	if response.OffsetFetchErrorCode == nil || *response.OffsetFetchErrorCode != 0 {
		t.Fatalf("offset fetch error missing: %+v", response)
	}
	if len(response.OffsetFetchCommitted) != 1 ||
		response.OffsetFetchCommitted[0].Topic != "booking.events" ||
		response.OffsetFetchCommitted[0].Partition != 0 ||
		response.OffsetFetchCommitted[0].CommittedOffset != 41 ||
		response.OffsetFetchCommitted[0].ErrorCode != 0 {
		t.Fatalf("committed frontier mismatch: %+v", response.OffsetFetchCommitted)
	}
}

func TestDecodeOffsetCommitResponseCapturesPerPartitionError(t *testing.T) {
	frame := responseFrame(t, 7, 21, &offsetcommitapi.Response{
		Topics: []offsetcommitapi.ResponseTopic{{
			Name: "booking.events",
			Partitions: []offsetcommitapi.ResponsePartition{{
				PartitionIndex: 0, ErrorCode: 22,
			}},
		}},
	})
	response, err := decodeResponseFrame(frame, protocol.OffsetCommit, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.OffsetCommitResults) != 1 ||
		response.OffsetCommitResults[0].Topic != "booking.events" ||
		response.OffsetCommitResults[0].Partition != 0 ||
		response.OffsetCommitResults[0].ErrorCode != 22 {
		t.Fatalf("offset commit result mismatch: %+v", response.OffsetCommitResults)
	}
}
