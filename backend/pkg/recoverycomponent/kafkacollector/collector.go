package kafkacollector

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/segmentio/kafka-go/protocol"
)

type Identity struct {
	RunID          string `json:"runId"`
	SourceRevision string `json:"sourceRevision"`
	Candidate      string `json:"candidate"`
}

type Exchange struct {
	Identity                       Identity         `json:"identity"`
	Role                           string           `json:"role"`
	ConnectionID                   uint64           `json:"connectionId"`
	DialTarget                     string           `json:"dialTarget"`
	Peer                           string           `json:"peer"`
	APIKey                         int16            `json:"apiKey"`
	APIVersion                     int16            `json:"apiVersion"`
	CorrelationID                  int32            `json:"correlationId"`
	RequestAt                      time.Time        `json:"requestAt"`
	ResponseAt                     *time.Time       `json:"responseAt,omitempty"`
	MatchStatus                    string           `json:"matchStatus"`
	Matched                        bool             `json:"matched"`
	CallOutcome                    string           `json:"callOutcome,omitempty"`
	GroupID                        string           `json:"groupId,omitempty"`
	MemberID                       string           `json:"memberId,omitempty"`
	GenerationID                   int32            `json:"generationId,omitempty"`
	GroupInstanceID                string           `json:"groupInstanceId"`
	CoordinatorKeyType             int8             `json:"coordinatorKeyType,omitempty"`
	RequestProtocolType            string           `json:"requestProtocolType,omitempty"`
	RequestProtocolName            string           `json:"requestProtocolName,omitempty"`
	OffsetCommitRequest            []wireOffset     `json:"offsetCommitRequest,omitempty"`
	RequestFrameLength             int              `json:"requestFrameLength,omitempty"`
	RequestFrameHex                string           `json:"requestFrameHex,omitempty"`
	RequestFrameTruncated          bool             `json:"requestFrameTruncated"`
	ResponseErrorCode              *int16           `json:"responseErrorCode,omitempty"`
	ResponseCodecErrorCode         *int16           `json:"responseCodecErrorCode,omitempty"`
	ResponseWireSchemaErrorCode    *int16           `json:"responseWireSchemaErrorCode,omitempty"`
	ResponseTypedThrottleMs        *int32           `json:"responseTypedThrottleMs,omitempty"`
	ResponseWireSchemaThrottleMs   *int32           `json:"responseWireSchemaThrottleMs,omitempty"`
	ResponseWireSchemaSource       string           `json:"responseWireSchemaSource,omitempty"`
	ResponseWireSchemaHeaderTags   int              `json:"responseWireSchemaHeaderTags,omitempty"`
	ResponseWireSchemaBodyTags     int              `json:"responseWireSchemaBodyTags,omitempty"`
	ResponseErrorCodeByteOffset    int              `json:"responseErrorCodeByteOffset,omitempty"`
	ResponseCodeAgreement          string           `json:"responseCodeAgreement,omitempty"`
	ResponseWireSchemaAgreement    string           `json:"responseWireSchemaAgreement,omitempty"`
	ResponseFrameLength            int              `json:"responseFrameLength,omitempty"`
	ResponseFramePrefixHex         string           `json:"responseFramePrefixHex,omitempty"`
	ResponseFrameHex               string           `json:"responseFrameHex,omitempty"`
	ResponseFrameTruncated         bool             `json:"responseFrameTruncated"`
	ResponseDecoderError           string           `json:"responseDecoderError,omitempty"`
	ResponseMemberErrorCode        *int16           `json:"responseMemberErrorCode,omitempty"`
	ResponseClassification         string           `json:"responseClassification,omitempty"`
	ResponseGenerationID           int32            `json:"responseGenerationId,omitempty"`
	ResponseMemberID               string           `json:"responseMemberId,omitempty"`
	ResponseProtocolType           string           `json:"responseProtocolType,omitempty"`
	ResponseProtocolName           string           `json:"responseProtocolName,omitempty"`
	ResponseAssignmentVersion      int16            `json:"responseAssignmentVersion,omitempty"`
	ResponseAssignments            []wireAssignment `json:"responseAssignments,omitempty"`
	ResponseAssignmentError        string           `json:"responseAssignmentError,omitempty"`
	ResponseOffsetFetchCommitted   []wireOffset     `json:"responseOffsetFetchCommitted,omitempty"`
	ResponseOffsetCommitResults    []wireOffset     `json:"responseOffsetCommitResults,omitempty"`
	ClusterID                      string           `json:"clusterId,omitempty"`
	ClusterObservedAt              *time.Time       `json:"clusterObservedAt,omitempty"`
	ClusterSourceConnectionID      uint64           `json:"clusterSourceConnectionId,omitempty"`
	ClusterSourcePeer              string           `json:"clusterSourcePeer,omitempty"`
	ClusterSourceAPIVersion        int16            `json:"clusterSourceApiVersion,omitempty"`
	ClusterSourceCorrelationID     *int32           `json:"clusterSourceCorrelationId,omitempty"`
	CoordinatorNodeID              *int32           `json:"coordinatorNodeId,omitempty"`
	CoordinatorHost                string           `json:"coordinatorHost,omitempty"`
	CoordinatorPort                int32            `json:"coordinatorPort,omitempty"`
	CoordinatorObservedAt          *time.Time       `json:"coordinatorObservedAt,omitempty"`
	CoordinatorLookupConnectionID  uint64           `json:"coordinatorLookupConnectionId,omitempty"`
	CoordinatorLookupPeer          string           `json:"coordinatorLookupPeer,omitempty"`
	CoordinatorLookupAPIVersion    int16            `json:"coordinatorLookupApiVersion,omitempty"`
	CoordinatorLookupCorrelationID *int32           `json:"coordinatorLookupCorrelationId,omitempty"`
}

type CollectorStats struct {
	OffsetCommitRequests uint64 `json:"offsetCommitRequests"`
	UnmatchedResponses   uint64 `json:"unmatchedResponses"`
	FrameDecodeFailures  uint64 `json:"frameDecodeFailures"`
	OversizedFrames      uint64 `json:"oversizedFrames"`
}

type metadataFact struct {
	clusterID     string
	observedAt    time.Time
	connectionID  uint64
	peer          string
	apiVersion    int16
	correlationID int32
}

type coordinatorFact struct {
	nodeID        int32
	host          string
	port          int32
	observedAt    time.Time
	connectionID  uint64
	peer          string
	apiVersion    int16
	correlationID int32
}

type Collector struct {
	identity Identity

	mu               sync.RWMutex
	nextConnectionID uint64
	exchanges        []Exchange
	stats            CollectorStats
	latestCluster    metadataFact
	coordinators     map[string]coordinatorFact
}

func NewCollector(identity Identity) *Collector {
	return &Collector{
		identity:     identity,
		coordinators: make(map[string]coordinatorFact),
	}
}

func (c *Collector) WrapDial(role string, dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return c.newTapConn(role, conn, address), nil
	}
}

func (c *Collector) Snapshot() []Exchange {
	c.mu.RLock()
	defer c.mu.RUnlock()

	exchanges := make([]Exchange, len(c.exchanges))
	copy(exchanges, c.exchanges)
	return exchanges
}

func (c *Collector) Stats() CollectorStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

func (c *Collector) newTapConn(role string, conn net.Conn, dialTarget string) *tapConn {
	c.mu.Lock()
	c.nextConnectionID++
	connectionID := c.nextConnectionID
	c.mu.Unlock()

	peer := ""
	if addr := conn.RemoteAddr(); addr != nil {
		peer = addr.String()
	}
	return &tapConn{
		Conn:         conn,
		collector:    c,
		role:         role,
		connectionID: connectionID,
		dialTarget:   dialTarget,
		peer:         peer,
		pending:      make(map[int32]pendingRequest),
	}
}

func (c *Collector) appendRequest(exchange Exchange) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if exchange.APIKey == int16(protocol.Heartbeat) {
		if c.latestCluster.clusterID != "" {
			exchange.ClusterID = c.latestCluster.clusterID
			exchange.ClusterObservedAt = timePtr(c.latestCluster.observedAt)
			exchange.ClusterSourceConnectionID = c.latestCluster.connectionID
			exchange.ClusterSourcePeer = c.latestCluster.peer
			exchange.ClusterSourceAPIVersion = c.latestCluster.apiVersion
			exchange.ClusterSourceCorrelationID = int32Ptr(c.latestCluster.correlationID)
		}
		if coordinator, ok := c.coordinators[exchange.GroupID]; ok {
			exchange.CoordinatorNodeID = int32Ptr(coordinator.nodeID)
			exchange.CoordinatorHost = coordinator.host
			exchange.CoordinatorPort = coordinator.port
			exchange.CoordinatorObservedAt = timePtr(coordinator.observedAt)
			exchange.CoordinatorLookupConnectionID = coordinator.connectionID
			exchange.CoordinatorLookupPeer = coordinator.peer
			exchange.CoordinatorLookupAPIVersion = coordinator.apiVersion
			exchange.CoordinatorLookupCorrelationID = int32Ptr(coordinator.correlationID)
		}
	}
	index := len(c.exchanges)
	c.exchanges = append(c.exchanges, exchange)
	return index
}

func (c *Collector) updateResponse(index int, response wireResponse, responseAt time.Time, matched bool, matchStatus, decoderError string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if index < 0 || index >= len(c.exchanges) {
		return
	}
	exchange := &c.exchanges[index]
	exchange.ResponseAt = timePtr(responseAt)
	exchange.Matched = matched
	exchange.MatchStatus = matchStatus
	if matched && exchange.CallOutcome == "" {
		exchange.CallOutcome = "CALL_RETURNED"
	}
	exchange.ResponseErrorCode = response.ErrorCode
	exchange.ResponseCodecErrorCode = response.CodecErrorCode
	exchange.ResponseWireSchemaErrorCode = response.WireSchemaErrorCode
	exchange.ResponseTypedThrottleMs = response.TypedThrottleTimeMs
	exchange.ResponseWireSchemaThrottleMs = response.WireSchemaThrottleTimeMs
	exchange.ResponseWireSchemaSource = response.WireSchemaSource
	exchange.ResponseWireSchemaHeaderTags = response.WireSchemaHeaderTagCount
	exchange.ResponseWireSchemaBodyTags = response.WireSchemaBodyTagCount
	exchange.ResponseErrorCodeByteOffset = response.ErrorCodeByteOffset
	exchange.ResponseCodeAgreement = response.CodeAgreement
	exchange.ResponseWireSchemaAgreement = response.WireSchemaAgreement
	exchange.ResponseFrameLength = response.FrameLength
	exchange.ResponseFramePrefixHex = response.FramePrefixHex
	exchange.ResponseFrameHex = response.FrameHex
	exchange.ResponseFrameTruncated = response.FrameHexTruncated
	exchange.ResponseDecoderError = decoderError
	exchange.ResponseMemberErrorCode = response.MemberErrorCode
	exchange.ResponseGenerationID = valueOrZero(response.GenerationID)
	exchange.ResponseMemberID = response.MemberID
	exchange.ResponseProtocolType = response.ProtocolType
	exchange.ResponseProtocolName = response.ProtocolName
	exchange.ResponseAssignmentVersion = response.AssignmentVersion
	exchange.ResponseAssignments = response.Assignments
	exchange.ResponseAssignmentError = response.AssignmentError
	exchange.ResponseOffsetFetchCommitted = response.OffsetFetchCommitted
	exchange.ResponseOffsetCommitResults = response.OffsetCommitResults
	if response.ClusterID != "" {
		exchange.ClusterID = response.ClusterID
	}
	if response.CoordinatorNodeID != nil {
		exchange.CoordinatorNodeID = response.CoordinatorNodeID
		exchange.CoordinatorHost = response.CoordinatorHost
		exchange.CoordinatorPort = response.CoordinatorPort
	}
	if response.ErrorCode != nil && exchange.APIKey == int16(protocol.Heartbeat) {
		exchange.ResponseClassification = classifyHeartbeatResponseCode(*response.ErrorCode)
	}
	if !matched {
		return
	}
	if exchange.APIKey == int16(protocol.Metadata) {
		c.latestCluster = metadataFact{
			clusterID:     response.ClusterID,
			observedAt:    responseAt,
			connectionID:  exchange.ConnectionID,
			peer:          exchange.Peer,
			apiVersion:    exchange.APIVersion,
			correlationID: exchange.CorrelationID,
		}
	}
	if exchange.APIKey == int16(protocol.FindCoordinator) && exchange.GroupID != "" && exchange.CoordinatorKeyType == 0 {
		if response.ErrorCode != nil && *response.ErrorCode == 0 && response.CoordinatorNodeID != nil {
			c.coordinators[exchange.GroupID] = coordinatorFact{
				nodeID:        *response.CoordinatorNodeID,
				host:          response.CoordinatorHost,
				port:          response.CoordinatorPort,
				observedAt:    responseAt,
				connectionID:  exchange.ConnectionID,
				peer:          exchange.Peer,
				apiVersion:    exchange.APIVersion,
				correlationID: exchange.CorrelationID,
			}
		} else {
			delete(c.coordinators, exchange.GroupID)
		}
	}
}

func (c *Collector) recordOffsetCommit() {
	c.mu.Lock()
	c.stats.OffsetCommitRequests++
	c.mu.Unlock()
}

func (c *Collector) recordUnmatchedResponse() {
	c.mu.Lock()
	c.stats.UnmatchedResponses++
	c.mu.Unlock()
}

func (c *Collector) recordDecodeFailure(oversized bool) {
	c.mu.Lock()
	if oversized {
		c.stats.OversizedFrames++
	} else {
		c.stats.FrameDecodeFailures++
	}
	c.mu.Unlock()
}

func int32Ptr(value int32) *int32 {
	return &value
}

func timePtr(value time.Time) *time.Time {
	return &value
}

func valueOrZero(value *int32) int32 {
	if value == nil {
		return 0
	}
	return *value
}

func (c *Collector) recordCallOutcome(connectionID uint64, correlationID int32, ctxErr, callErr error) {
	outcome := classifyCallOutcome(ctxErr, callErr)
	c.mu.Lock()
	defer c.mu.Unlock()
	for index := range c.exchanges {
		exchange := &c.exchanges[index]
		if exchange.ConnectionID == connectionID && exchange.CorrelationID == correlationID &&
			exchange.APIKey == int16(protocol.Heartbeat) {
			exchange.CallOutcome = outcome
			return
		}
	}
}

func classifyCallOutcome(ctxErr, callErr error) string {
	if errors.Is(ctxErr, context.Canceled) || errors.Is(callErr, context.Canceled) {
		return "LOCAL_CANCELLED"
	}
	if errors.Is(ctxErr, context.DeadlineExceeded) || errors.Is(callErr, context.DeadlineExceeded) {
		return "TIMEOUT"
	}
	var timeout interface{ Timeout() bool }
	if errors.As(callErr, &timeout) && timeout.Timeout() {
		return "TIMEOUT"
	}
	if callErr != nil {
		return "NETWORK_ERROR"
	}
	return "CALL_RETURNED"
}
