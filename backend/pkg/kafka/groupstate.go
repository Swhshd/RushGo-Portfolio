package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// GroupPartitionSnapshot is one read-only observation of a topic partition in
// a consumer group. CommittedOffset is -1 when the group has no committed
// offset for the partition.
type GroupPartitionSnapshot struct {
	Topic           string
	Partition       int
	FirstOffset     int64
	CommittedOffset int64
	LogEndOffset    int64
}

// GroupMemberSnapshot is the group membership and assignment as reported by
// the coordinator. A member can remain registered while its partition loop is
// stalled, so membership alone is not health.
type GroupMemberSnapshot struct {
	MemberID   string
	ClientID   string
	ClientHost string
	Topics     []GroupMemberTopicSnapshot
}

type GroupMemberTopicSnapshot struct {
	Topic      string
	Partitions []int
}

// GroupSnapshot is a bounded, read-only view of one consumer group.
type GroupSnapshot struct {
	Topic      string
	Group      string
	ObservedAt time.Time
	Members    []GroupMemberSnapshot
	Partitions []GroupPartitionSnapshot
}

// InspectGroup reads group membership, committed offsets, and log-end offsets.
// It never joins the group, commits, resets, or modifies any state.
func InspectGroup(ctx context.Context, brokers []string, topic, group string) (*GroupSnapshot, error) {
	if len(brokers) == 0 || topic == "" || group == "" {
		return nil, errors.New("kafka group inspection requires brokers, topic, and group")
	}
	client := &kafkago.Client{Addr: kafkago.TCP(brokers...), Timeout: 5 * time.Second}
	metadata, err := client.Metadata(ctx, &kafkago.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return nil, fmt.Errorf("read topic metadata: %w", err)
	}
	var partitions []int
	for _, t := range metadata.Topics {
		if t.Name != topic {
			continue
		}
		for _, p := range t.Partitions {
			partitions = append(partitions, p.ID)
		}
	}
	if len(partitions) == 0 {
		return nil, fmt.Errorf("topic %q has no partitions", topic)
	}

	lastRequests := make([]kafkago.OffsetRequest, 0, len(partitions))
	for _, partition := range partitions {
		lastRequests = append(lastRequests, kafkago.LastOffsetOf(partition))
	}
	lastOffsets, err := client.ListOffsets(ctx, &kafkago.ListOffsetsRequest{
		Addr: client.Addr, Topics: map[string][]kafkago.OffsetRequest{topic: lastRequests},
	})
	if err != nil {
		return nil, fmt.Errorf("read partition log-end offsets: %w", err)
	}
	lastByPartition := make(map[int]int64, len(partitions))
	for _, offsets := range lastOffsets.Topics[topic] {
		if offsets.Error != nil {
			return nil, fmt.Errorf("partition %d log-end offset: %w", offsets.Partition, offsets.Error)
		}
		lastByPartition[offsets.Partition] = offsets.LastOffset
	}

	firstRequests := make([]kafkago.OffsetRequest, 0, len(partitions))
	for _, partition := range partitions {
		firstRequests = append(firstRequests, kafkago.FirstOffsetOf(partition))
	}
	firstOffsets, err := client.ListOffsets(ctx, &kafkago.ListOffsetsRequest{
		Addr: client.Addr, Topics: map[string][]kafkago.OffsetRequest{topic: firstRequests},
	})
	if err != nil {
		return nil, fmt.Errorf("read partition first offsets: %w", err)
	}
	firstByPartition := make(map[int]int64, len(partitions))
	for _, offsets := range firstOffsets.Topics[topic] {
		if offsets.Error != nil {
			return nil, fmt.Errorf("partition %d first offset: %w", offsets.Partition, offsets.Error)
		}
		firstByPartition[offsets.Partition] = offsets.FirstOffset
	}

	committed, err := client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{
		GroupID: group, Topics: map[string][]int{topic: partitions},
	})
	if err != nil {
		return nil, fmt.Errorf("read committed group offsets: %w", err)
	}
	if committed.Error != nil {
		return nil, fmt.Errorf("read committed group offsets: %w", committed.Error)
	}
	committedByPartition := make(map[int]int64, len(partitions))
	for _, offsets := range committed.Topics[topic] {
		if offsets.Error != nil {
			return nil, fmt.Errorf("partition %d committed offset: %w", offsets.Partition, offsets.Error)
		}
		committedByPartition[offsets.Partition] = offsets.CommittedOffset
	}

	described, err := client.DescribeGroups(ctx, &kafkago.DescribeGroupsRequest{
		Addr: client.Addr, GroupIDs: []string{group},
	})
	if err != nil {
		return nil, fmt.Errorf("read consumer group membership: %w", err)
	}
	snapshot := &GroupSnapshot{Topic: topic, Group: group, ObservedAt: time.Now().UTC()}
	for _, description := range described.Groups {
		if description.GroupID != group {
			continue
		}
		if description.Error != nil {
			return nil, fmt.Errorf("consumer group %q: %w", group, description.Error)
		}
		for _, member := range description.Members {
			observed := GroupMemberSnapshot{
				MemberID: member.MemberID, ClientID: member.ClientID, ClientHost: member.ClientHost,
			}
			for _, assignment := range member.MemberAssignments.Topics {
				observed.Topics = append(observed.Topics, GroupMemberTopicSnapshot{
					Topic: assignment.Topic, Partitions: append([]int(nil), assignment.Partitions...),
				})
			}
			snapshot.Members = append(snapshot.Members, observed)
		}
	}
	if len(snapshot.Members) == 0 && len(described.Groups) == 0 {
		return nil, fmt.Errorf("consumer group %q was not found", group)
	}
	for _, partition := range partitions {
		committedOffset, ok := committedByPartition[partition]
		if !ok {
			committedOffset = -1
		}
		snapshot.Partitions = append(snapshot.Partitions, GroupPartitionSnapshot{
			Topic: topic, Partition: partition,
			FirstOffset: firstByPartition[partition], CommittedOffset: committedOffset, LogEndOffset: lastByPartition[partition],
		})
	}
	return snapshot, nil
}

// FindRecord scans a bounded offset range of one partition for a matching
// record. It never joins the group and never commits an offset.
func FindRecord(ctx context.Context, brokers []string, topic string, partition int, from, to int64, max int, match func([]byte) bool) (offset int64, found bool, scanned int, err error) {
	if len(brokers) == 0 || topic == "" || partition < 0 || from < 0 || to <= from || max < 1 || match == nil {
		return -1, false, 0, errors.New("invalid bounded record scan")
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, Partition: partition,
		MaxWait: 500 * time.Millisecond, ReadBatchTimeout: 500 * time.Millisecond,
	})
	defer reader.Close()
	if err := reader.SetOffset(from); err != nil {
		return -1, false, 0, fmt.Errorf("position partition reader: %w", err)
	}
	for scanned < max {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return -1, false, scanned, fmt.Errorf("read partition %d from offset %d: %w", partition, from, err)
		}
		if message.Offset < from {
			continue
		}
		if message.Offset >= to {
			return -1, false, scanned, nil
		}
		scanned++
		if match(message.Value) {
			return message.Offset, true, scanned, nil
		}
		// The sampled log-end offset is the exclusive upper bound. Once the
		// last in-range record is inspected, no later record can exist within
		// this bounded scan; do not block waiting for a record at `to`.
		if message.Offset+1 >= to {
			return -1, false, scanned, nil
		}
	}
	return -1, false, scanned, nil
}
