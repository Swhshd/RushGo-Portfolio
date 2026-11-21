package kafka

import (
	"context"
	"net"
	"sync"
	"time"
)

// connectionScope closes actual sockets on cancellation. kafka-go coordinator
// commits and reader initialization include context-free network operations.
type connectionScope struct {
	mu          sync.Mutex
	closed      bool
	connections map[*scopedConnection]struct{}
}

type scopedConnection struct {
	net.Conn
	owner *connectionScope
}

// dialObserved applies an optional observation dialer but still tracks the
// returned connection so cancellation closes it exactly like the production
// path. A nil observed function uses the production dialer.
func (s *connectionScope) dialObserved(ctx context.Context, network, address string,
	observed func(context.Context, string, string) (net.Conn, error)) (net.Conn, error) {
	var conn net.Conn
	var err error
	if observed != nil {
		conn, err = observed(ctx, network, address)
	} else {
		conn, err = (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, address)
	}
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		conn.Close()
		return nil, context.Canceled
	}
	wrapped := &scopedConnection{Conn: conn, owner: s}
	if s.connections == nil {
		s.connections = make(map[*scopedConnection]struct{})
	}
	s.connections[wrapped] = struct{}{}
	return wrapped, nil
}

func (c *scopedConnection) Close() error {
	c.owner.mu.Lock()
	delete(c.owner.connections, c)
	c.owner.mu.Unlock()
	return c.Conn.Close()
}

func (s *connectionScope) close() {
	s.mu.Lock()
	s.closed = true
	connections := s.connections
	s.connections = nil
	s.mu.Unlock()
	for conn := range connections {
		_ = conn.Conn.Close()
	}
}
