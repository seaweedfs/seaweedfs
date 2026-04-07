package blockvol

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestRebuildTransport_SessionControlRoundTrip(t *testing.T) {
	msg := SessionControlMsg{
		Epoch:      5,
		SessionID:  42,
		Command:    SessionCmdStartRebuild,
		BaseLSN:    1000,
		TargetLSN:  2000,
		SnapshotID: 7,
	}
	encoded := EncodeSessionControl(msg)
	decoded, err := DecodeSessionControl(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded != msg {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", decoded, msg)
	}
}

func TestRebuildTransport_SessionAckRoundTrip(t *testing.T) {
	msg := SessionAckMsg{
		Epoch:         5,
		SessionID:     42,
		Phase:         SessionAckCompleted,
		WALAppliedLSN: 2500,
		BaseComplete:  true,
		AchievedLSN:   2500,
	}
	encoded := EncodeSessionAck(msg)
	decoded, err := DecodeSessionAck(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded != msg {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", decoded, msg)
	}
}

func TestRebuildTransport_BaseBlockStreaming(t *testing.T) {
	// Create primary with data.
	primary := createTestVolForTransport(t, "primary")
	defer primary.Close()
	if err := primary.HandleAssignment(1, RolePrimary, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	blocks := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		blocks[i] = bytes.Repeat([]byte{byte(0xA0 + i)}, 4096)
		if err := primary.WriteLBA(uint64(i), blocks[i]); err != nil {
			t.Fatalf("write LBA %d: %v", i, err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// Create replica with rebuild session.
	replica := createTestVolForTransport(t, "replica")
	defer replica.Close()
	if err := replica.HandleAssignment(1, RoleReplica, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := replica.StartRebuildSession(RebuildSessionConfig{
		SessionID: 1,
		Epoch:     1,
		BaseLSN:   5,
		TargetLSN: 5,
	}); err != nil {
		t.Fatal(err)
	}

	// Set up localhost TCP connection.
	server := NewRebuildTransportServer(primary, 1, 1, 5, 5)
	client := NewRebuildTransportClient(replica, 1)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Server side: accept and serve.
	serverDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		serverDone <- server.ServeBaseBlocks(conn)
	}()

	// Client side: connect and receive.
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	totalBlocks, err := client.ReceiveBaseBlocks(conn)
	if err != nil {
		t.Fatalf("receive base blocks: %v", err)
	}

	// Wait for server.
	if err := <-serverDone; err != nil {
		t.Fatalf("serve base blocks: %v", err)
	}

	// The volume is 4MB / 4K = 1024 LBAs, but only 5 have data.
	// All 1024 blocks are streamed (full extent).
	t.Logf("received %d base blocks", totalBlocks)
	if totalBlocks == 0 {
		t.Fatal("no base blocks received")
	}

	// Verify the 5 written blocks are readable on replica.
	for i := 0; i < 5; i++ {
		data, err := replica.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("replica read LBA %d: %v", i, err)
		}
		if !bytes.Equal(data, blocks[i]) {
			t.Fatalf("replica LBA %d mismatch: got[0]=0x%02x want[0]=0x%02x",
				i, data[0], blocks[i][0])
		}
	}
	t.Log("base block streaming: all 5 blocks verified on replica")
}

func TestRebuildTransport_SessionControlOverTCP(t *testing.T) {
	// Test session control messages over real TCP.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()

		// Read session control.
		msgType, payload, err := ReadFrame(conn)
		if err != nil {
			done <- err
			return
		}
		if msgType != MsgSessionControl {
			done <- fmt.Errorf("unexpected msg type: 0x%02x", msgType)
			return
		}
		ctrl, err := DecodeSessionControl(payload)
		if err != nil {
			done <- err
			return
		}

		// Reply with session ack.
		ack := SessionAckMsg{
			Epoch:     ctrl.Epoch,
			SessionID: ctrl.SessionID,
			Phase:     SessionAckAccepted,
		}
		done <- SendSessionAck(conn, ack)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send session control.
	ctrl := SessionControlMsg{
		Epoch:     3,
		SessionID: 99,
		Command:   SessionCmdStartRebuild,
		BaseLSN:   500,
		TargetLSN: 1000,
	}
	if err := SendSessionControl(conn, ctrl); err != nil {
		t.Fatalf("send control: %v", err)
	}

	// Read ack.
	msgType, payload, err := ReadFrame(conn)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if msgType != MsgSessionAck {
		t.Fatalf("unexpected ack type: 0x%02x", msgType)
	}
	ack, err := DecodeSessionAck(payload)
	if err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.SessionID != 99 || ack.Phase != SessionAckAccepted {
		t.Fatalf("ack mismatch: %+v", ack)
	}

	if err := <-done; err != nil {
		t.Fatalf("server: %v", err)
	}
	t.Log("session control over TCP: round-trip verified")
}

func createTestVolForTransport(t *testing.T, name string) *BlockVol {
	t.Helper()
	opts := CreateOptions{
		VolumeSize: 4 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    1 * 1024 * 1024,
	}
	vol, err := CreateBlockVol(t.TempDir()+"/"+name+".blk", opts)
	if err != nil {
		t.Fatal(err)
	}
	return vol
}
