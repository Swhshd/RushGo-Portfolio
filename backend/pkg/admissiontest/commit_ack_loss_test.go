//go:build admission_integration || recoverycomponents

package admissiontest

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
)

func TestCommitAckLossForwardsOtherFramesAndDropsWholeCommitFrame(t *testing.T) {
	fault := NewCommitAckLoss()
	client, server := net.Pipe()
	wrapped := &commitAckDropConn{Conn: client, fault: fault}
	t.Cleanup(func() {
		_ = wrapped.Close()
		_ = server.Close()
	})

	selectFrame := backendCommandCompleteFrame("SELECT 1")
	writeErr := make(chan error, 1)
	go func() {
		_, err := server.Write(selectFrame)
		writeErr <- err
	}()
	forwarded := readFrameInChunks(t, wrapped, len(selectFrame))
	if err := <-writeErr; err != nil {
		t.Fatal("write backend frame:", err)
	}
	if !bytes.Equal(forwarded, selectFrame) {
		t.Fatalf("forwarded backend frame=%x; want exact original frame %x", forwarded, selectFrame)
	}
	if fault.DroppedCommitCount() != 0 {
		t.Fatal("unarmed non-COMMIT frame was counted as a dropped acknowledgment")
	}

	if !fault.Arm() || fault.Arm() {
		t.Fatal("COMMIT acknowledgment dropper did not enforce one-shot arming")
	}
	armedSelectFrame := backendCommandCompleteFrame("SELECT 2")
	go func() {
		_, err := server.Write(armedSelectFrame)
		writeErr <- err
	}()
	forwarded = readFrameInChunks(t, wrapped, len(armedSelectFrame))
	if err := <-writeErr; err != nil {
		t.Fatal("write armed non-COMMIT backend frame:", err)
	}
	if !bytes.Equal(forwarded, armedSelectFrame) {
		t.Fatalf("armed non-COMMIT frame=%x; want exact original frame %x", forwarded, armedSelectFrame)
	}
	if fault.DroppedCommitCount() != 0 {
		t.Fatal("armed non-COMMIT frame consumed the drop or was counted as a COMMIT acknowledgment")
	}

	commitFrame := backendCommandCompleteFrame("COMMIT")
	go func() {
		_, _ = server.Write(commitFrame)
	}()
	readBuffer := make([]byte, len(commitFrame))
	if n, err := wrapped.Read(readBuffer); n != 0 || err != io.EOF {
		t.Fatalf("read after COMMIT frame: n=%d err=%v; want EOF with no frame bytes forwarded", n, err)
	}
	if fault.DroppedCommitCount() != 1 {
		t.Fatalf("dropped COMMIT completion count=%d; want one", fault.DroppedCommitCount())
	}
	if fault.Arm() {
		t.Fatal("COMMIT acknowledgment dropper armed more than once")
	}
}

func readFrameInChunks(t *testing.T, conn net.Conn, frameSize int) []byte {
	t.Helper()
	forwarded := make([]byte, 0, frameSize)
	chunk := make([]byte, 3)
	for len(forwarded) < frameSize {
		n, err := conn.Read(chunk)
		if err != nil {
			t.Fatal("read forwarded backend frame chunk:", err)
		}
		forwarded = append(forwarded, chunk[:n]...)
	}
	return forwarded
}

func backendCommandCompleteFrame(tag string) []byte {
	body := append([]byte(tag), 0)
	frame := make([]byte, 5+len(body))
	frame[0] = 'C'
	binary.BigEndian.PutUint32(frame[1:5], uint32(4+len(body)))
	copy(frame[5:], body)
	return frame
}
