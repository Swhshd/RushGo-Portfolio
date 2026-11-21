//go:build admission_integration || recoverycomponents

// Package admissiontest contains test-only plumbing for isolated admission
// PostgreSQL integration tests.
package admissiontest

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxCommitAckFrameSize uint32 = 64 << 20

var errCommitAckFrameTooLarge = errors.New("PostgreSQL backend frame exceeds test helper bound")

// CommitAckLoss drops the first armed PostgreSQL CommandComplete(COMMIT)
// backend frame. It is test-only and operates below pgx on plaintext sessions.
type CommitAckLoss struct {
	armed   atomic.Bool
	dropped atomic.Int32
}

// NewCommitAckLoss creates an initially unarmed, one-shot COMMIT response dropper.
func NewCommitAckLoss() *CommitAckLoss {
	return &CommitAckLoss{}
}

// Arm arms the dropper once. It returns false if it has already been armed or hit.
func (d *CommitAckLoss) Arm() bool {
	if d == nil || d.dropped.Load() != 0 {
		return false
	}
	return d.armed.CompareAndSwap(false, true)
}

// DroppedCommitCount reports the number of successful COMMIT completion frames dropped.
func (d *CommitAckLoss) DroppedCommitCount() int32 {
	if d == nil {
		return 0
	}
	return d.dropped.Load()
}

// SingleConnectionPoolConfig clones a parsed pgxpool config, wraps its dialed
// connections, and limits the test repository pool to one connection. TLS is
// rejected because encrypted frames cannot be safely inspected here.
func (d *CommitAckLoss) SingleConnectionPoolConfig(source *pgxpool.Config) (*pgxpool.Config, error) {
	if d == nil || source == nil || source.ConnConfig == nil {
		return nil, errors.New("COMMIT acknowledgment dropper requires a parsed PostgreSQL pool config")
	}
	if source.ConnConfig.TLSConfig != nil {
		return nil, errors.New("COMMIT acknowledgment dropper requires sslmode=disable")
	}

	config := *source
	connConfig := *source.ConnConfig
	originalDial := connConfig.DialFunc
	if originalDial == nil {
		dialer := &net.Dialer{}
		originalDial = dialer.DialContext
	}
	connConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := originalDial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &commitAckDropConn{Conn: conn, fault: d}, nil
	}
	config.ConnConfig = &connConfig
	config.MaxConns = 1
	config.MinConns = 0
	return &config, nil
}

type commitAckDropConn struct {
	net.Conn
	fault *CommitAckLoss

	readMu  sync.Mutex
	pending []byte
}

func (c *commitAckDropConn) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for len(c.pending) == 0 {
		var header [5]byte
		if _, err := io.ReadFull(c.Conn, header[:]); err != nil {
			return 0, err
		}
		length := binary.BigEndian.Uint32(header[1:5])
		if length < 4 {
			return 0, fmt.Errorf("invalid PostgreSQL backend frame length %d", length)
		}
		if length > maxCommitAckFrameSize {
			return 0, errCommitAckFrameTooLarge
		}
		body := make([]byte, int(length-4))
		if _, err := io.ReadFull(c.Conn, body); err != nil {
			return 0, err
		}
		if header[0] == 'C' && bytes.Equal(body, []byte("COMMIT\x00")) && c.fault.armed.CompareAndSwap(true, false) {
			c.fault.dropped.Add(1)
			_ = c.Conn.Close()
			return 0, io.EOF
		}
		c.pending = append(c.pending[:0], header[:]...)
		c.pending = append(c.pending, body...)
	}

	n := copy(dst, c.pending)
	c.pending = c.pending[n:]
	if len(c.pending) == 0 {
		c.pending = nil
	}
	return n, nil
}
