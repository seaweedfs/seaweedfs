package iscsi_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

// TestQAPhase3Stability tests Phase 3 integration stability scenarios:
// WAL pressure through iSCSI, crash recovery with sessions, lifecycle stress.
func TestQAPhase3Stability(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// QA-3.1: WAL Pressure + iSCSI E2E
		{name: "e2e_sustained_write_1000", run: testE2ESustainedWrite1000},
		{name: "e2e_write_read_under_pressure", run: testE2EWriteReadUnderPressure},
		{name: "e2e_synccache_under_pressure", run: testE2ESyncCacheUnderPressure},
		{name: "e2e_multiple_sessions_shared_vol", run: testE2EMultipleSessionsSharedVol},

		// QA-3.2: Crash Recovery + Session Reconnect
		{name: "e2e_crash_recovery_via_iscsi", run: testE2ECrashRecoveryViaISCSI},
		{name: "e2e_session_reconnect", run: testE2ESessionReconnect},

		// QA-3.3: Lifecycle & Resource Stress
		{name: "e2e_rapid_open_close_target", run: testE2ERapidOpenCloseTarget},
		{name: "e2e_config_extreme_values", run: testE2EConfigExtremeValues},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// --- Helpers ---

func setupStabilityTarget(t *testing.T, opts blockvol.CreateOptions, cfgs ...blockvol.BlockVolConfig) (*blockvol.BlockVol, net.Conn, *iscsi.TargetServer, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stability.blk")

	var vol *blockvol.BlockVol
	var err error
	if len(cfgs) > 0 {
		vol, err = blockvol.CreateBlockVol(path, opts, cfgs[0])
	} else {
		vol, err = blockvol.CreateBlockVol(path, opts)
	}
	if err != nil {
		t.Fatal(err)
	}

	adapter := &blockVolAdapter{vol: vol}
	config := iscsi.DefaultTargetConfig()
	config.TargetName = intTargetName
	logger := log.New(io.Discard, "", 0)
	ts := iscsi.NewTargetServer("127.0.0.1:0", config, logger)
	ts.AddVolume(intTargetName, adapter)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	go ts.Serve(ln)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Login
	stabilityDoLogin(t, conn)

	t.Cleanup(func() {
		conn.Close()
		ts.Close()
		vol.Close()
	})

	return vol, conn, ts, addr
}

func stabilityDoLogin(t *testing.T, conn net.Conn) {
	t.Helper()
	params := iscsi.NewParams()
	params.Set("InitiatorName", intInitiatorName)
	params.Set("TargetName", intTargetName)
	params.Set("SessionType", "Normal")

	loginReq := &iscsi.PDU{}
	loginReq.SetOpcode(iscsi.OpLoginReq)
	loginReq.SetLoginStages(iscsi.StageSecurityNeg, iscsi.StageFullFeature)
	loginReq.SetLoginTransit(true)
	loginReq.SetISID([6]byte{0x00, 0x02, 0x3D, 0x00, 0x00, 0x01})
	loginReq.SetCmdSN(1)
	loginReq.DataSegment = params.Encode()

	if err := iscsi.WritePDU(conn, loginReq); err != nil {
		t.Fatal(err)
	}
	resp, err := iscsi.ReadPDU(conn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("login failed: %d/%d", resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
}

func stabilityWrite(t *testing.T, conn net.Conn, lba uint32, data []byte, cmdSN uint32) {
	t.Helper()
	var cdb [16]byte
	cdb[0] = iscsi.ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], lba)
	blocks := uint16(len(data) / 4096)
	binary.BigEndian.PutUint16(cdb[7:9], blocks)
	resp := sendSCSICmd(t, conn, cdb, cmdSN, false, true, data, uint32(len(data)))
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("write LBA %d failed: status %d", lba, resp.SCSIStatus())
	}
}

func stabilityRead(t *testing.T, conn net.Conn, lba uint32, cmdSN uint32) []byte {
	t.Helper()
	var cdb [16]byte
	cdb[0] = iscsi.ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], lba)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	resp := sendSCSICmd(t, conn, cdb, cmdSN, true, false, nil, 4096)
	if resp.Opcode() != iscsi.OpSCSIDataIn {
		t.Fatalf("read LBA %d: expected Data-In, got %s", lba, iscsi.OpcodeName(resp.Opcode()))
	}
	return resp.DataSegment
}

// --- QA-3.1: WAL Pressure + iSCSI E2E ---

func testE2ESustainedWrite1000(t *testing.T) {
	// 1000 sequential WRITEs via iSCSI, flusher running.
	_, conn, _, _ := setupStabilityTarget(t, blockvol.CreateOptions{
		VolumeSize: 1024 * 4096,
		BlockSize:  4096,
		WALSize:    256 * 1024, // 256KB WAL
	})

	cmdSN := uint32(2)
	for i := 0; i < 1000; i++ {
		lba := uint32(i % 1024)
		data := make([]byte, 4096)
		data[0] = byte(i % 256)
		data[1] = byte(i / 256)

		var cdb [16]byte
		cdb[0] = iscsi.ScsiWrite10
		binary.BigEndian.PutUint32(cdb[2:6], lba)
		binary.BigEndian.PutUint16(cdb[7:9], 1)

		resp := sendSCSICmd(t, conn, cdb, cmdSN, false, true, data, 4096)
		if resp.SCSIStatus() != iscsi.SCSIStatusGood {
			t.Fatalf("write %d (LBA %d) failed: status %d", i, lba, resp.SCSIStatus())
		}
		cmdSN++
	}
	t.Log("sustained: 1000 WRITEs completed successfully")
}

func testE2EWriteReadUnderPressure(t *testing.T) {
	// Heavy writes to create WAL pressure, concurrent reads should still work.
	cfg := blockvol.DefaultConfig()
	cfg.WALPressureThreshold = 0.3
	cfg.FlushInterval = 5 * time.Millisecond

	_, conn, _, _ := setupStabilityTarget(t, blockvol.CreateOptions{
		VolumeSize: 256 * 4096,
		BlockSize:  4096,
		WALSize:    64 * 1024, // small WAL for pressure
	}, cfg)

	// Write to first 50 LBAs.
	cmdSN := uint32(2)
	for i := 0; i < 50; i++ {
		data := bytes.Repeat([]byte{byte(i)}, 4096)
		stabilityWrite(t, conn, uint32(i), data, cmdSN)
		cmdSN++
	}

	// Read them back under potential WAL pressure.
	for i := 0; i < 50; i++ {
		readData := stabilityRead(t, conn, uint32(i), cmdSN)
		cmdSN++
		expected := bytes.Repeat([]byte{byte(i)}, 4096)
		if !bytes.Equal(readData, expected) {
			t.Fatalf("LBA %d: data mismatch under pressure", i)
		}
	}
}

func testE2ESyncCacheUnderPressure(t *testing.T) {
	// SYNC_CACHE while WAL is under pressure.
	cfg := blockvol.DefaultConfig()
	cfg.WALPressureThreshold = 0.3
	cfg.FlushInterval = 5 * time.Millisecond

	_, conn, _, _ := setupStabilityTarget(t, blockvol.CreateOptions{
		VolumeSize: 256 * 4096,
		BlockSize:  4096,
		WALSize:    64 * 1024,
	}, cfg)

	// Write enough to create pressure.
	cmdSN := uint32(2)
	for i := 0; i < 30; i++ {
		data := bytes.Repeat([]byte{byte(i)}, 4096)
		stabilityWrite(t, conn, uint32(i%256), data, cmdSN)
		cmdSN++
	}

	// SYNC_CACHE should succeed even under pressure.
	var syncCDB [16]byte
	syncCDB[0] = iscsi.ScsiSyncCache10
	resp := sendSCSICmd(t, conn, syncCDB, cmdSN, false, false, nil, 0)
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("SYNC_CACHE under pressure failed: status %d", resp.SCSIStatus())
	}
}

func testE2EMultipleSessionsSharedVol(t *testing.T) {
	// 4 sessions on same BlockVol, each writing to different LBA ranges.
	path := filepath.Join(t.TempDir(), "shared.blk")
	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 1024 * 4096,
		BlockSize:  4096,
		WALSize:    512 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()

	adapter := &blockVolAdapter{vol: vol}
	config := iscsi.DefaultTargetConfig()
	config.TargetName = intTargetName
	logger := log.New(io.Discard, "", 0)
	ts := iscsi.NewTargetServer("127.0.0.1:0", config, logger)
	ts.AddVolume(intTargetName, adapter)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	go ts.Serve(ln)
	defer ts.Close()

	var wg sync.WaitGroup
	wg.Add(4)
	for sess := 0; sess < 4; sess++ {
		go func(id int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				t.Logf("session %d: dial failed: %v", id, err)
				return
			}
			defer conn.Close()

			stabilityDoLogin(t, conn)

			// Each session writes to its own LBA range (id*10 to id*10+9).
			cmdSN := uint32(2)
			baseLBA := uint32(id * 10)
			for i := 0; i < 10; i++ {
				data := make([]byte, 4096)
				data[0] = byte(id)
				data[1] = byte(i)

				var cdb [16]byte
				cdb[0] = iscsi.ScsiWrite10
				binary.BigEndian.PutUint32(cdb[2:6], baseLBA+uint32(i))
				binary.BigEndian.PutUint16(cdb[7:9], 1)

				resp := sendSCSICmd(t, conn, cdb, cmdSN, false, true, data, 4096)
				if resp.SCSIStatus() != iscsi.SCSIStatusGood {
					t.Logf("session %d: write %d failed", id, i)
					return
				}
				cmdSN++
			}

			// Read back and verify.
			for i := 0; i < 10; i++ {
				var cdb [16]byte
				cdb[0] = iscsi.ScsiRead10
				binary.BigEndian.PutUint32(cdb[2:6], baseLBA+uint32(i))
				binary.BigEndian.PutUint16(cdb[7:9], 1)

				resp := sendSCSICmd(t, conn, cdb, cmdSN, true, false, nil, 4096)
				if resp.Opcode() != iscsi.OpSCSIDataIn {
					t.Logf("session %d: read %d: expected Data-In", id, i)
					return
				}
				if resp.DataSegment[0] != byte(id) || resp.DataSegment[1] != byte(i) {
					t.Errorf("session %d: LBA %d data mismatch: got [%d,%d], want [%d,%d]",
						id, baseLBA+uint32(i), resp.DataSegment[0], resp.DataSegment[1], id, i)
				}
				cmdSN++
			}
		}(sess)
	}
	wg.Wait()
}

// --- QA-3.2: Crash Recovery + Session Reconnect ---

func testE2ECrashRecoveryViaISCSI(t *testing.T) {
	// Write via iSCSI, close vol (simulating crash), reopen and verify data via direct read.
	dir := t.TempDir()
	path := filepath.Join(dir, "crash.blk")

	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 256 * 4096,
		BlockSize:  4096,
		WALSize:    128 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	adapter := &blockVolAdapter{vol: vol}
	config := iscsi.DefaultTargetConfig()
	config.TargetName = intTargetName
	logger := log.New(io.Discard, "", 0)
	ts := iscsi.NewTargetServer("127.0.0.1:0", config, logger)
	ts.AddVolume(intTargetName, adapter)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go ts.Serve(ln)

	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	stabilityDoLogin(t, conn)

	// Write 10 blocks via iSCSI.
	cmdSN := uint32(2)
	for i := 0; i < 10; i++ {
		data := bytes.Repeat([]byte{byte(i + 0xA0)}, 4096)
		stabilityWrite(t, conn, uint32(i), data, cmdSN)
		cmdSN++
	}

	// SYNC_CACHE to ensure WAL is durable.
	var syncCDB [16]byte
	syncCDB[0] = iscsi.ScsiSyncCache10
	sendSCSICmd(t, conn, syncCDB, cmdSN, false, false, nil, 0)

	// Close everything (simulate crash).
	conn.Close()
	ts.Close()
	vol.Close()

	// Reopen and verify data.
	vol2, err := blockvol.OpenBlockVol(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer vol2.Close()

	for i := 0; i < 10; i++ {
		data, err := vol2.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d) after recovery: %v", i, err)
		}
		expected := bytes.Repeat([]byte{byte(i + 0xA0)}, 4096)
		if !bytes.Equal(data, expected) {
			t.Fatalf("LBA %d: data mismatch after crash recovery", i)
		}
	}
}

func testE2ESessionReconnect(t *testing.T) {
	// Write via session 1, disconnect, reconnect as session 2, read back.
	dir := t.TempDir()
	path := filepath.Join(dir, "reconnect.blk")

	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 256 * 4096,
		BlockSize:  4096,
		WALSize:    128 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()

	adapter := &blockVolAdapter{vol: vol}
	config := iscsi.DefaultTargetConfig()
	config.TargetName = intTargetName
	logger := log.New(io.Discard, "", 0)
	ts := iscsi.NewTargetServer("127.0.0.1:0", config, logger)
	ts.AddVolume(intTargetName, adapter)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	go ts.Serve(ln)
	defer ts.Close()

	// Session 1: write data.
	conn1, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	stabilityDoLogin(t, conn1)

	writeData := bytes.Repeat([]byte{0xDE}, 4096)
	stabilityWrite(t, conn1, 5, writeData, 2)
	conn1.Close()

	// Session 2: read back.
	conn2, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	stabilityDoLogin(t, conn2)

	readData := stabilityRead(t, conn2, 5, 2)
	if !bytes.Equal(readData, writeData) {
		t.Fatal("data mismatch after session reconnect")
	}
}

// --- QA-3.3: Lifecycle & Resource Stress ---

func testE2ERapidOpenCloseTarget(t *testing.T) {
	// Open TargetServer, do I/O, close. Repeat 10 times. No fd/goroutine leak.
	dir := t.TempDir()
	path := filepath.Join(dir, "rapid.blk")

	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 256 * 4096,
		BlockSize:  4096,
		WALSize:    128 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()

	for cycle := 0; cycle < 10; cycle++ {
		adapter := &blockVolAdapter{vol: vol}
		config := iscsi.DefaultTargetConfig()
		config.TargetName = intTargetName
		logger := log.New(io.Discard, "", 0)
		ts := iscsi.NewTargetServer("127.0.0.1:0", config, logger)
		ts.AddVolume(intTargetName, adapter)

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("cycle %d: listen: %v", cycle, err)
		}
		go ts.Serve(ln)

		conn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
		if err != nil {
			t.Fatalf("cycle %d: dial: %v", cycle, err)
		}
		stabilityDoLogin(t, conn)

		// Write one block.
		data := bytes.Repeat([]byte{byte(cycle)}, 4096)
		stabilityWrite(t, conn, uint32(cycle), data, 2)

		conn.Close()
		ts.Close()
	}

	// Verify all 10 LBAs have correct data.
	for i := 0; i < 10; i++ {
		data, err := vol.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d) after 10 cycles: %v", i, err)
		}
		if data[0] != byte(i) {
			t.Errorf("LBA %d: byte[0] = %d, want %d", i, data[0], i)
		}
	}
}

func testE2EConfigExtremeValues(t *testing.T) {
	// Create BlockVol with extreme but valid config, use via iSCSI.
	cfg := blockvol.DefaultConfig()
	cfg.GroupCommitMaxDelay = 100 * time.Microsecond // very fast
	cfg.DirtyMapShards = 1                           // minimum
	cfg.FlushInterval = 1 * time.Millisecond

	_, conn, _, _ := setupStabilityTarget(t, blockvol.CreateOptions{
		VolumeSize: 64 * 4096,
		BlockSize:  4096,
		WALSize:    32 * 1024, // small WAL
	}, cfg)

	// Write + read should work.
	cmdSN := uint32(2)
	for i := 0; i < 20; i++ {
		data := bytes.Repeat([]byte{byte(i)}, 4096)
		stabilityWrite(t, conn, uint32(i%64), data, cmdSN)
		cmdSN++
	}

	// SYNC_CACHE.
	var syncCDB [16]byte
	syncCDB[0] = iscsi.ScsiSyncCache10
	resp := sendSCSICmd(t, conn, syncCDB, cmdSN, false, false, nil, 0)
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("SYNC_CACHE with extreme config failed: %d", resp.SCSIStatus())
	}
}
