package component

// Primary-initiated rebuild test with production-scale validation.
//
// Tests the full primary-driven rebuild loop:
//   1. Primary has 1GB volume with data + ongoing writes
//   2. Empty replica joins
//   3. Primary decides rebuild from syncAck facts (applied_lsn=0 < wal_tail)
//   4. Two-line session: base extent + live WAL, flusher active on both sides
//   5. After completion: flush both, compare extent CRC block-by-block
//
// This is the closest to production rebuild behavior:
//   - Real WAL, real extent, real flusher, real TCP
//   - Continuous writes during rebuild
//   - Final validation by extent comparison

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestRebuild_PrimaryInitiated_1GB_WithLiveWrites is the production-scale
// primary-driven rebuild test.
func TestRebuild_PrimaryInitiated_1GB_WithLiveWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in short mode — 1GB volume test")
	}

	volumeSize := uint64(1024 * 1024 * 1024) // 1GB
	walSize := uint64(64 * 1024 * 1024)       // 64MB
	blockSize := uint32(4096)
	totalLBAs := volumeSize / uint64(blockSize)

	// ---------------------------------------------------------------
	// Phase 1: Create primary with data
	// ---------------------------------------------------------------
	primary := createPrimaryVol(t, volumeSize, walSize)
	defer primary.Close()

	fillCount := totalLBAs / 2 // fill 50% of volume
	t.Logf("filling primary: %d blocks (%.0f%% of %d total)...",
		fillCount, float64(fillCount)/float64(totalLBAs)*100, totalLBAs)
	fillStart := time.Now()
	for lba := uint64(0); lba < fillCount; lba++ {
		data := deterministicBlock(lba, 0, blockSize)
		if err := primary.WriteLBA(lba, data); err != nil {
			t.Fatalf("fill LBA %d: %v", lba, err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}
	if err := primary.ForceFlush(); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	baseLSN := primary.Status().WALHeadLSN
	t.Logf("primary filled: %d blocks in %v, baseLSN=%d",
		fillCount, time.Since(fillStart).Round(time.Millisecond), baseLSN)

	// ---------------------------------------------------------------
	// Phase 2: Create empty replica, primary decides rebuild
	// ---------------------------------------------------------------
	replica := createReplicaVol(t, volumeSize, walSize)
	defer replica.Close()

	// Primary decision: replica at applied_lsn=0, primary wal_tail > 0 → rebuild.
	replicaAppliedLSN := uint64(0)
	primaryWALTail := baseLSN // after flush, tail is at baseLSN
	if replicaAppliedLSN >= primaryWALTail {
		t.Skip("replica is already within WAL — no rebuild needed")
	}
	t.Logf("primary decision: replica applied=%d < wal_tail=%d → REBUILD", replicaAppliedLSN, primaryWALTail)

	// ---------------------------------------------------------------
	// Phase 3: Start rebuild session with live traffic
	// ---------------------------------------------------------------
	liveWriteCount := uint64(5000)
	targetLSN := baseLSN + liveWriteCount
	sessionID := uint64(100)

	// Start receiver on replica for WAL lane.
	if err := replica.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatal(err)
	}
	recvAddr := replica.ReplicaReceiverAddr()

	// Start rebuild session via control channel.
	ctrlConn, err := net.Dial("tcp", recvAddr.CtrlAddr)
	if err != nil {
		t.Fatalf("connect ctrl: %v", err)
	}
	defer ctrlConn.Close()

	if err := blockvol.SendSessionControl(ctrlConn, blockvol.SessionControlMsg{
		Epoch:     1,
		SessionID: sessionID,
		Command:   blockvol.SessionCmdStartRebuild,
		BaseLSN:   baseLSN,
		TargetLSN: targetLSN,
	}); err != nil {
		t.Fatalf("send start_rebuild: %v", err)
	}

	// Read accepted ack.
	ackCh := make(chan blockvol.SessionAckMsg, 100)
	go func() {
		for {
			msgType, payload, err := blockvol.ReadFrame(ctrlConn)
			if err != nil {
				return
			}
			if msgType == blockvol.MsgSessionAck {
				ack, _ := blockvol.DecodeSessionAck(payload)
				ackCh <- ack
			}
		}
	}()

	select {
	case ack := <-ackCh:
		if ack.Phase != blockvol.SessionAckAccepted {
			t.Fatalf("expected accepted, got phase=%d", ack.Phase)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for accepted ack")
	}
	t.Logf("rebuild session %d accepted", sessionID)

	// Wire WAL lane: primary ships to replica receiver.
	primary.SetReplicaAddr(recvAddr.DataAddr, recvAddr.CtrlAddr)

	// Start base lane over TCP.
	baseLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer baseLn.Close()

	var wg sync.WaitGroup
	var baseErr atomic.Value

	// Base lane server.
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := baseLn.Accept()
		if err != nil {
			baseErr.Store(fmt.Errorf("accept: %w", err))
			return
		}
		defer conn.Close()
		server := blockvol.NewRebuildTransportServer(primary, sessionID, 1, baseLSN, targetLSN)
		if err := server.ServeBaseBlocks(conn); err != nil {
			baseErr.Store(fmt.Errorf("serve: %w", err))
		}
	}()

	// Base lane client.
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := net.Dial("tcp", baseLn.Addr().String())
		if err != nil {
			baseErr.Store(fmt.Errorf("dial: %w", err))
			return
		}
		defer conn.Close()
		client := blockvol.NewRebuildTransportClient(replica, sessionID)
		blocks, err := client.ReceiveBaseBlocks(conn)
		if err != nil {
			baseErr.Store(fmt.Errorf("receive: %w", err))
			return
		}
		t.Logf("base lane: received %d blocks", blocks)
	}()

	// Live writes on primary during rebuild.
	liveTracker := make(map[uint64]uint64) // lba → generation
	var liveMu sync.Mutex
	writeStart := time.Now()
	for i := uint64(0); i < liveWriteCount; i++ {
		lba := (i * 97) % totalLBAs // spread across volume
		gen := i + 1
		data := deterministicBlock(lba, gen, blockSize)
		if err := primary.WriteLBA(lba, data); err != nil {
			t.Fatalf("live write %d LBA %d: %v", i, lba, err)
		}
		liveMu.Lock()
		liveTracker[lba] = gen // last write wins
		liveMu.Unlock()

		if i%1000 == 0 && i > 0 {
			time.Sleep(1 * time.Millisecond) // pacing
		}
	}
	t.Logf("live writes: %d in %v", liveWriteCount, time.Since(writeStart).Round(time.Millisecond))

	// Wait for base lane.
	wg.Wait()
	if v := baseErr.Load(); v != nil {
		t.Fatalf("base lane: %v", v)
	}

	// Wait for WAL lane to deliver live entries to rebuild session.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, progress, ok := replica.ActiveRebuildSession()
		if ok && progress.WALAppliedLSN >= targetLSN {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// ---------------------------------------------------------------
	// Phase 4: Completion
	// ---------------------------------------------------------------
	achieved, completed, err := replica.TryCompleteRebuildSession(sessionID)
	if err != nil {
		t.Fatalf("try complete: %v", err)
	}
	if !completed {
		_, progress, _ := replica.ActiveRebuildSession()
		t.Fatalf("not completed: walApplied=%d target=%d baseComplete=%v",
			progress.WALAppliedLSN, targetLSN, progress.BaseComplete)
	}
	_, progress, _ := replica.ActiveRebuildSession()
	t.Logf("rebuild completed: achieved=%d base_applied=%d base_skipped=%d bitmap=%d",
		achieved, progress.BaseBlocksApplied, progress.BaseBlocksSkipped, progress.BitmapAppliedCount)

	// ---------------------------------------------------------------
	// Phase 5: Flush both and compare extent CRC
	// ---------------------------------------------------------------
	t.Log("flushing both volumes for CRC comparison...")
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("primary SyncCache: %v", err)
	}
	if err := primary.ForceFlush(); err != nil {
		t.Fatalf("primary ForceFlush: %v", err)
	}
	if err := replica.SyncCache(); err != nil {
		// Replica may not have group commit wired; ignore error.
		t.Logf("replica SyncCache: %v (may be expected)", err)
	}
	if err := replica.ForceFlush(); err != nil {
		t.Logf("replica ForceFlush: %v (may be expected)", err)
	}

	// Compare block by block.
	t.Log("comparing extents block by block...")
	compareStart := time.Now()
	mismatches := 0
	primaryHash := sha256.New()
	replicaHash := sha256.New()

	for lba := uint64(0); lba < totalLBAs; lba++ {
		pData, err := primary.ReadLBA(lba, blockSize)
		if err != nil {
			t.Fatalf("primary read LBA %d: %v", lba, err)
		}
		rData, err := replica.ReadLBA(lba, blockSize)
		if err != nil {
			t.Fatalf("replica read LBA %d: %v", lba, err)
		}

		primaryHash.Write(pData)
		replicaHash.Write(rData)

		if !bytes.Equal(pData, rData) {
			mismatches++
			if mismatches <= 5 {
				// Determine expected data for this LBA.
				liveMu.Lock()
				gen, hasLive := liveTracker[lba]
				liveMu.Unlock()
				if hasLive {
					expected := deterministicBlock(lba, gen, blockSize)
					t.Errorf("LBA %d MISMATCH: primary[0]=0x%02x replica[0]=0x%02x expected[0]=0x%02x (live gen=%d)",
						lba, pData[0], rData[0], expected[0], gen)
				} else if lba < fillCount {
					t.Errorf("LBA %d MISMATCH: primary[0]=0x%02x replica[0]=0x%02x (initial fill, no live override)",
						lba, pData[0], rData[0])
				} else {
					t.Errorf("LBA %d MISMATCH: primary[0]=0x%02x replica[0]=0x%02x (unwritten LBA)",
						lba, pData[0], rData[0])
				}
			}
		}
	}

	pCRC := fmt.Sprintf("%x", primaryHash.Sum(nil))
	rCRC := fmt.Sprintf("%x", replicaHash.Sum(nil))
	t.Logf("comparison done in %v: primary CRC=%s...%s replica CRC=%s...%s",
		time.Since(compareStart).Round(time.Millisecond),
		pCRC[:8], pCRC[len(pCRC)-8:],
		rCRC[:8], rCRC[len(rCRC)-8:])

	if mismatches > 0 {
		t.Fatalf("REBUILD VALIDATION FAILED: %d / %d blocks mismatch", mismatches, totalLBAs)
	}
	if pCRC != rCRC {
		t.Fatalf("EXTENT CRC MISMATCH: primary=%s replica=%s", pCRC, rCRC)
	}
	t.Logf("REBUILD VALIDATION PASSED: %d blocks, CRC match, %d live writes during rebuild",
		totalLBAs, liveWriteCount)
}

// --- Helpers ---

func deterministicBlock(lba uint64, gen uint64, blockSize uint32) []byte {
	data := make([]byte, blockSize)
	// Deterministic pattern from LBA + generation.
	seed := byte((lba*7 + gen*13) & 0xFF)
	for i := range data {
		data[i] = seed ^ byte(i&0xFF)
	}
	// Stamp LBA and gen for debugging.
	binary.LittleEndian.PutUint64(data[0:8], lba)
	binary.LittleEndian.PutUint64(data[8:16], gen)
	return data
}

func createPrimaryVol(t *testing.T, volumeSize, walSize uint64) *blockvol.BlockVol {
	t.Helper()
	opts := blockvol.CreateOptions{
		VolumeSize: volumeSize,
		BlockSize:  4096,
		WALSize:    walSize,
	}
	vol, err := blockvol.CreateBlockVol(filepath.Join(t.TempDir(), "primary.blk"), opts)
	if err != nil {
		t.Fatal(err)
	}
	vol.HandleAssignment(1, blockvol.RolePrimary, 30*time.Second)
	return vol
}

func createReplicaVol(t *testing.T, volumeSize, walSize uint64) *blockvol.BlockVol {
	t.Helper()
	opts := blockvol.CreateOptions{
		VolumeSize: volumeSize,
		BlockSize:  4096,
		WALSize:    walSize,
	}
	vol, err := blockvol.CreateBlockVol(filepath.Join(t.TempDir(), "replica.blk"), opts)
	if err != nil {
		t.Fatal(err)
	}
	vol.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second)
	return vol
}
