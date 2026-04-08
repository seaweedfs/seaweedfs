package component

// End-to-end rebuild tests that wire all three layers:
//   - Session control (MsgSessionControl/MsgSessionAck) over TCP
//   - Base lane (RebuildTransportServer → RebuildTransportClient) over TCP
//   - WAL lane (ShipAll → ReplicaReceiver) over TCP
//
// These test the full rebuild flow as it would work in production.

import (
	"bytes"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestRebuild_E2E_FullProtocol exercises the complete rebuild protocol:
//
//  1. Primary has 50 blocks of data
//  2. Session control: primary sends start_rebuild, replica acks
//  3. Base lane: primary streams extent via TCP, replica applies with bitmap
//  4. WAL lane: primary writes new data, ships via TCP, replica applies
//  5. Session completes, all data verified on replica
func TestRebuild_E2E_FullProtocol(t *testing.T) {
	primary, replica := createE2EPair(t)
	defer primary.Close()
	defer replica.Close()

	// Step 1: Write initial data on primary.
	numBlocks := 50
	blockData := make(map[uint64][]byte)
	for i := 0; i < numBlocks; i++ {
		data := bytes.Repeat([]byte{byte(0x30 + i)}, 4096)
		blockData[uint64(i)] = data
		if err := primary.WriteLBA(uint64(i), data); err != nil {
			t.Fatalf("primary write LBA %d: %v", i, err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}
	if err := primary.ForceFlush(); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	baseLSN := primary.Status().WALHeadLSN
	liveWrites := 10
	targetLSN := baseLSN + uint64(liveWrites)
	t.Logf("primary: %d blocks, baseLSN=%d, targetLSN=%d", numBlocks, baseLSN, targetLSN)

	// Step 2: Session control over TCP.
	ctrlLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ctrlLn.Close()

	// Replica side: accept session control, start rebuild session.
	sessionReady := make(chan error, 1)
	go func() {
		conn, err := ctrlLn.Accept()
		if err != nil {
			sessionReady <- err
			return
		}
		defer conn.Close()

		// Read start_rebuild control message.
		msgType, payload, err := blockvol.ReadFrame(conn)
		if err != nil {
			sessionReady <- err
			return
		}
		if msgType != blockvol.MsgSessionControl {
			sessionReady <- err
			return
		}
		ctrl, err := blockvol.DecodeSessionControl(payload)
		if err != nil {
			sessionReady <- err
			return
		}

		// Start rebuild session on replica.
		if err := replica.StartRebuildSession(blockvol.RebuildSessionConfig{
			SessionID: ctrl.SessionID,
			Epoch:     ctrl.Epoch,
			BaseLSN:   ctrl.BaseLSN,
			TargetLSN: ctrl.TargetLSN,
		}); err != nil {
			sessionReady <- err
			return
		}

		// Send accepted ack.
		blockvol.SendSessionAck(conn, blockvol.SessionAckMsg{
			Epoch:     ctrl.Epoch,
			SessionID: ctrl.SessionID,
			Phase:     blockvol.SessionAckAccepted,
		})
		sessionReady <- nil
	}()

	// Primary side: send start_rebuild.
	ctrlConn, err := net.Dial("tcp", ctrlLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer ctrlConn.Close()

	sessionID := uint64(42)
	if err := blockvol.SendSessionControl(ctrlConn, blockvol.SessionControlMsg{
		Epoch:     1,
		SessionID: sessionID,
		Command:   blockvol.SessionCmdStartRebuild,
		BaseLSN:   baseLSN,
		TargetLSN: targetLSN,
	}); err != nil {
		t.Fatalf("send session control: %v", err)
	}

	// Wait for replica to accept.
	if err := <-sessionReady; err != nil {
		t.Fatalf("session setup: %v", err)
	}

	// Read accepted ack.
	msgType, payload, err := blockvol.ReadFrame(ctrlConn)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if msgType != blockvol.MsgSessionAck {
		t.Fatalf("unexpected ack type: 0x%02x", msgType)
	}
	ack, _ := blockvol.DecodeSessionAck(payload)
	if ack.Phase != blockvol.SessionAckAccepted {
		t.Fatalf("expected accepted, got phase=%d", ack.Phase)
	}
	t.Logf("session control: start_rebuild accepted (session=%d)", sessionID)

	// Step 3: Wire WAL lane via real TCP shipping.
	if err := replica.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatal(err)
	}
	recvAddr := replica.ReplicaReceiverAddr()
	primary.SetReplicaAddr(recvAddr.DataAddr, recvAddr.CtrlAddr)
	t.Logf("WAL lane: %s/%s", recvAddr.DataAddr, recvAddr.CtrlAddr)

	// Step 4: Base lane over TCP (concurrent with WAL lane).
	baseLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer baseLn.Close()

	var wg sync.WaitGroup
	var baseErr error

	// Server: stream base blocks.
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := baseLn.Accept()
		if err != nil {
			baseErr = err
			return
		}
		defer conn.Close()
		server := blockvol.NewRebuildTransportServer(primary, sessionID, 1, baseLSN, targetLSN)
		baseErr = server.ServeBaseBlocks(conn)
	}()

	// Client: receive base blocks.
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := net.Dial("tcp", baseLn.Addr().String())
		if err != nil {
			baseErr = err
			return
		}
		defer conn.Close()
		client := blockvol.NewRebuildTransportClient(replica, sessionID)
		blocks, err := client.ReceiveBaseBlocks(conn)
		if err != nil {
			baseErr = err
			return
		}
		t.Logf("base lane: received %d blocks", blocks)
	}()

	// Step 5: WAL lane — primary writes new data, ships via TCP. The replica
	// receiver automatically routes active rebuild-session WAL into the WAL lane.
	for i := 0; i < liveWrites; i++ {
		lba := uint64(i) // overwrite first 10 blocks
		data := bytes.Repeat([]byte{byte(0xF0 + i)}, 4096)
		blockData[lba] = data // update expected
		if err := primary.WriteLBA(lba, data); err != nil {
			t.Fatalf("primary live write LBA %d: %v", lba, err)
		}
	}

	// Wait for base lane to complete.
	wg.Wait()
	if baseErr != nil {
		t.Fatalf("base lane: %v", baseErr)
	}

	// Wait for shipped WAL to reach the active rebuild session through the live
	// receiver path.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, progress, ok := replica.ActiveRebuildSession()
		if ok && progress.WALAppliedLSN >= targetLSN {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if _, progress, ok := replica.ActiveRebuildSession(); !ok || progress.WALAppliedLSN < targetLSN {
		t.Fatalf("rebuild WAL lane did not reach target via receiver routing: ok=%v walApplied=%d target=%d receivedLSN=%d",
			ok, progress.WALAppliedLSN, targetLSN, replica.ReceivedLSN())
	}

	// Step 6: Completion.
	achieved, completed, err := replica.TryCompleteRebuildSession(sessionID)
	if err != nil {
		t.Fatalf("try complete: %v", err)
	}
	if !completed {
		cfg, progress, _ := replica.ActiveRebuildSession()
		t.Fatalf("session not complete: achieved=%d target=%d walApplied=%d baseComplete=%v",
			achieved, cfg.TargetLSN, progress.WALAppliedLSN, progress.BaseComplete)
	}
	t.Logf("rebuild completed: achievedLSN=%d", achieved)

	// Step 7: Verify all blocks on replica.
	for lba, expected := range blockData {
		got, err := replica.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("replica read LBA %d: %v", lba, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("replica LBA %d: got[0]=0x%02x want[0]=0x%02x", lba, got[0], expected[0])
		}
	}

	_, p, _ := replica.ActiveRebuildSession()
	t.Logf("e2e verified: %d blocks correct, base_applied=%d base_skipped=%d bitmap=%d",
		len(blockData), p.BaseBlocksApplied, p.BaseBlocksSkipped, p.BitmapAppliedCount)
}

func TestRebuild_E2E_ReplicaReceiverSessionControlLoop(t *testing.T) {
	primary, replica := createE2EPair(t)
	defer primary.Close()
	defer replica.Close()

	if err := replica.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatalf("start replica receiver: %v", err)
	}
	recvAddr := replica.ReplicaReceiverAddr()
	if recvAddr == nil {
		t.Fatal("expected replica receiver addresses")
	}
	primary.SetReplicaAddr(recvAddr.DataAddr, recvAddr.CtrlAddr)

	numBlocks := 12
	blockData := make(map[uint64][]byte)
	for i := 0; i < numBlocks; i++ {
		data := bytes.Repeat([]byte{byte(0x40 + i)}, 4096)
		blockData[uint64(i)] = data
		if err := primary.WriteLBA(uint64(i), data); err != nil {
			t.Fatalf("primary write LBA %d: %v", i, err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}
	if err := primary.ForceFlush(); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	baseLSN := primary.Status().WALHeadLSN
	targetLSN := baseLSN + 3

	ctrlConn, err := net.Dial("tcp", recvAddr.CtrlAddr)
	if err != nil {
		t.Fatalf("dial control addr %s: %v", recvAddr.CtrlAddr, err)
	}
	defer ctrlConn.Close()

	ackCh := make(chan blockvol.SessionAckMsg, 16)
	readErrCh := make(chan error, 1)
	go func() {
		for {
			msgType, payload, err := blockvol.ReadFrame(ctrlConn)
			if err != nil {
				readErrCh <- err
				return
			}
			if msgType != blockvol.MsgSessionAck {
				readErrCh <- err
				return
			}
			ack, err := blockvol.DecodeSessionAck(payload)
			if err != nil {
				readErrCh <- err
				return
			}
			ackCh <- ack
		}
	}()

	sessionID := uint64(88)
	if err := blockvol.SendSessionControl(ctrlConn, blockvol.SessionControlMsg{
		Epoch:     1,
		SessionID: sessionID,
		Command:   blockvol.SessionCmdStartRebuild,
		BaseLSN:   baseLSN,
		TargetLSN: targetLSN,
	}); err != nil {
		t.Fatalf("send session control: %v", err)
	}

	waitForAckPhase := func(phase byte, timeout time.Duration) blockvol.SessionAckMsg {
		t.Helper()
		deadline := time.After(timeout)
		for {
			select {
			case ack := <-ackCh:
				if ack.SessionID == sessionID && ack.Phase == phase {
					return ack
				}
			case err := <-readErrCh:
				t.Fatalf("control ack read failed: %v", err)
			case <-deadline:
				t.Fatalf("timed out waiting for session ack phase=0x%02x", phase)
			}
		}
	}

	accepted := waitForAckPhase(blockvol.SessionAckAccepted, 2*time.Second)
	if accepted.WALAppliedLSN != baseLSN {
		t.Fatalf("accepted wal_applied=%d, want baseLSN %d", accepted.WALAppliedLSN, baseLSN)
	}

	baseLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer baseLn.Close()

	var wg sync.WaitGroup
	var baseErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := baseLn.Accept()
		if err != nil {
			baseErr = err
			return
		}
		defer conn.Close()
		server := blockvol.NewRebuildTransportServer(primary, sessionID, 1, baseLSN, targetLSN)
		baseErr = server.ServeBaseBlocks(conn)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := net.Dial("tcp", baseLn.Addr().String())
		if err != nil {
			baseErr = err
			return
		}
		defer conn.Close()
		client := blockvol.NewRebuildTransportClient(replica, sessionID)
		_, err = client.ReceiveBaseBlocks(conn)
		if err != nil {
			baseErr = err
			return
		}
	}()

	for i := 0; i < 3; i++ {
		lba := uint64(i)
		data := bytes.Repeat([]byte{byte(0xE0 + i)}, 4096)
		blockData[lba] = data
		if err := primary.WriteLBA(lba, data); err != nil {
			t.Fatalf("primary live write LBA %d: %v", lba, err)
		}
	}

	wg.Wait()
	if baseErr != nil {
		t.Fatalf("base lane: %v", baseErr)
	}

	running := waitForAckPhase(blockvol.SessionAckRunning, 5*time.Second)
	if running.WALAppliedLSN < baseLSN+1 {
		t.Fatalf("running wal_applied=%d, want >= %d", running.WALAppliedLSN, baseLSN+1)
	}
	baseComplete := waitForAckPhase(blockvol.SessionAckBaseComplete, 5*time.Second)
	if !baseComplete.BaseComplete {
		t.Fatal("expected base_complete ack to report BaseComplete=true")
	}

	achieved, completed, err := replica.TryCompleteRebuildSession(sessionID)
	if err != nil {
		t.Fatalf("try complete: %v", err)
	}
	if !completed {
		t.Fatalf("session not complete, achieved=%d", achieved)
	}
	completedAck := waitForAckPhase(blockvol.SessionAckCompleted, 2*time.Second)
	if completedAck.AchievedLSN < targetLSN {
		t.Fatalf("completed achieved=%d, want >= %d", completedAck.AchievedLSN, targetLSN)
	}

	for lba, expected := range blockData {
		got, err := replica.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("replica read LBA %d: %v", lba, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("replica LBA %d: got[0]=0x%02x want[0]=0x%02x", lba, got[0], expected[0])
		}
	}
}

// TestRebuild_E2E_SessionCancel verifies that cancelling a session during
// active base streaming doesn't leave the system in a broken state.
func TestRebuild_E2E_SessionCancel(t *testing.T) {
	primary, replica := createE2EPair(t)
	defer primary.Close()
	defer replica.Close()

	// Write some data.
	for i := 0; i < 10; i++ {
		primary.WriteLBA(uint64(i), bytes.Repeat([]byte{byte(i)}, 4096))
	}
	primary.SyncCache()
	primary.ForceFlush()
	baseLSN := primary.Status().WALHeadLSN

	// Start rebuild session.
	sessionID := uint64(77)
	replica.StartRebuildSession(blockvol.RebuildSessionConfig{
		SessionID: sessionID, Epoch: 1, BaseLSN: baseLSN, TargetLSN: baseLSN + 5,
	})

	// Apply a few base blocks.
	for lba := uint64(0); lba < 5; lba++ {
		data, _ := primary.ReadLBA(lba, 4096)
		replica.ApplyRebuildSessionBaseBlock(sessionID, lba, data)
	}

	// Cancel mid-session.
	if err := replica.CancelRebuildSession(sessionID, "test_cancel"); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	// Verify no active session.
	_, _, ok := replica.ActiveRebuildSession()
	if ok {
		t.Fatal("expected no active session after cancel")
	}

	// Start a fresh session — should work.
	newSessionID := uint64(78)
	if err := replica.StartRebuildSession(blockvol.RebuildSessionConfig{
		SessionID: newSessionID, Epoch: 1, BaseLSN: baseLSN, TargetLSN: baseLSN + 1,
	}); err != nil {
		t.Fatalf("start new session after cancel: %v", err)
	}

	cfg, _, ok := replica.ActiveRebuildSession()
	if !ok || cfg.SessionID != newSessionID {
		t.Fatalf("expected new session %d, got ok=%v id=%d", newSessionID, ok, cfg.SessionID)
	}
	t.Log("session cancel: mid-rebuild cancel + fresh restart verified")
}

// --- Helpers ---

func createE2EPair(t *testing.T) (primary, replica *blockvol.BlockVol) {
	t.Helper()
	opts := blockvol.CreateOptions{
		VolumeSize: 4 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    2 * 1024 * 1024,
		// Rebuild tests use best_effort — sync_all barrier during live writes
		// competes with the rebuild session's control channel and causes timeouts.
	}
	p, err := blockvol.CreateBlockVol(filepath.Join(t.TempDir(), "primary.blk"), opts)
	if err != nil {
		t.Fatal(err)
	}
	p.HandleAssignment(1, blockvol.RolePrimary, 30*time.Second)
	r, err := blockvol.CreateBlockVol(filepath.Join(t.TempDir(), "replica.blk"), opts)
	if err != nil {
		p.Close()
		t.Fatal(err)
	}
	r.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second)
	return p, r
}
