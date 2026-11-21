package kafka

import (
	"context"
	"net"
)

// ConsumerObservation is an inert, default-off hook used by local component
// evidence to observe the normal consumer loop without changing its semantics.
// Production constructors pass nil. DialFunc, when set, must preserve the
// existing connection lifecycle; BeforeCommit may block or fail to model a
// controlled boundary but must not alter ordering in production.
type ConsumerObservation struct {
	DialFunc     func(ctx context.Context, network, address string) (net.Conn, error)
	BeforeCommit func(ctx context.Context, generation int32, topic string,
		partition int, next int64) error
	AfterCommit func(generation int32, topic string, partition int, next int64,
		err error)
}
