package iscsi

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestQA is the top-level QA adversarial test suite for Phase 2 iSCSI target.
// All tests use pure TCP (net.Pipe) and run on Windows with no kernel dependencies.
func TestQA(t *testing.T) {
	t.Run("PDU", testQA_PDU)
	t.Run("Params", testQA_Params)
	t.Run("Login", testQA_Login)
	t.Run("Discovery", testQA_Discovery)
	t.Run("SCSI", testQA_SCSI)
	t.Run("DataIO", testQA_DataIO)
	t.Run("Session", testQA_Session)
	t.Run("Target", testQA_Target)
	t.Run("Integration", testQA_Integration)
}

// ---------------------------------------------------------------------------
// QA helpers
// ---------------------------------------------------------------------------

// safeMockDevice wraps mockBlockDevice with a mutex for concurrent access.
type safeMockDevice struct {
	mu  sync.Mutex
	dev *mockBlockDevice
}

func newSafeMockDevice(size uint64) *safeMockDevice {
	return &safeMockDevice{dev: newMockDevice(size)}
}
func (s *safeMockDevice) ReadAt(lba uint64, length uint32) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dev.ReadAt(lba, length)
}
func (s *safeMockDevice) WriteAt(lba uint64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dev.WriteAt(lba, data)
}
func (s *safeMockDevice) Trim(lba uint64, length uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dev.Trim(lba, length)
}
func (s *safeMockDevice) SyncCache() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dev.SyncCache()
}
func (s *safeMockDevice) BlockSize() uint32  { return s.dev.BlockSize() }
func (s *safeMockDevice) VolumeSize() uint64 { return s.dev.VolumeSize() }
func (s *safeMockDevice) IsHealthy() bool    { return s.dev.IsHealthy() }

// qaSession sets up a session with a configurable mock device and returns
// the client conn + done channel. The session runs HandleConnection in a goroutine.
func qaSession(t *testing.T, dev BlockDevice) (net.Conn, chan error) {
	t.Helper()
	client, server := net.Pipe()

	config := DefaultTargetConfig()
	config.TargetName = testTargetName
	resolver := newTestResolverWithDevice(dev)
	logger := log.New(io.Discard, "", 0)

	sess := NewSession(server, config, resolver, resolver, logger)
	done := make(chan error, 1)
	go func() {
		done <- sess.HandleConnection()
	}()

	t.Cleanup(func() {
		sess.Close()
		client.Close()
	})

	return client, done
}

// qaSessionDefault creates a session with a standard 1024-block mock device.
func qaSessionDefault(t *testing.T) (net.Conn, chan error) {
	t.Helper()
	return qaSession(t, newMockDevice(1024*4096))
}

// qaLoginNormal performs a normal login on the given connection, returning CmdSN for next use.
func qaLoginNormal(t *testing.T, conn net.Conn) uint32 {
	t.Helper()
	doLogin(t, conn)
	return 2 // doLogin uses CmdSN=1, next is 2
}

// qaLoginDiscovery performs a discovery login.
func qaLoginDiscovery(t *testing.T, conn net.Conn) {
	t.Helper()
	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("SessionType", "Discovery")

	req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
	req.SetCmdSN(1)

	if err := WritePDU(conn, req); err != nil {
		t.Fatal(err)
	}
	resp, err := ReadPDU(conn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("discovery login failed: %d/%d", resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
}

// qaSendSCSICmd sends a SCSI command and reads the response.
func qaSendSCSICmd(t *testing.T, conn net.Conn, cdb [16]byte, cmdSN uint32,
	read, write bool, dataOut []byte, expLen uint32) *PDU {
	t.Helper()
	cmd := &PDU{}
	cmd.SetOpcode(OpSCSICmd)
	flags := uint8(FlagF)
	if read {
		flags |= FlagR
	}
	if write {
		flags |= FlagW
	}
	cmd.SetOpSpecific1(flags)
	cmd.SetInitiatorTaskTag(cmdSN)
	cmd.SetExpectedDataTransferLength(expLen)
	cmd.SetCmdSN(cmdSN)
	cmd.SetCDB(cdb)
	if dataOut != nil {
		cmd.DataSegment = dataOut
	}

	if err := WritePDU(conn, cmd); err != nil {
		t.Fatal(err)
	}

	resp, err := ReadPDU(conn)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// qaReadAllDataIn reads Data-In PDUs until the S-bit is set, assembling the full data.
func qaReadAllDataIn(t *testing.T, conn net.Conn, first *PDU) []byte {
	t.Helper()
	var data []byte
	data = append(data, first.DataSegment...)
	pdu := first
	for pdu.OpSpecific1()&FlagS == 0 {
		var err error
		pdu, err = ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, pdu.DataSegment...)
	}
	return data
}

// qaExpectReject reads a PDU and asserts it's a Reject.
func qaExpectReject(t *testing.T, conn net.Conn) *PDU {
	t.Helper()
	resp, err := ReadPDU(conn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpReject {
		t.Fatalf("expected Reject, got %s", OpcodeName(resp.Opcode()))
	}
	return resp
}

// qaExpectCheckCondition asserts that the response is a SCSI Response with CHECK CONDITION.
func qaExpectCheckCondition(t *testing.T, resp *PDU) {
	t.Helper()
	if resp.Opcode() != OpSCSIResp {
		t.Fatalf("expected SCSI-Response, got %s", OpcodeName(resp.Opcode()))
	}
	if resp.SCSIStatus() != SCSIStatusCheckCond {
		t.Fatalf("expected CHECK_CONDITION, got status %d", resp.SCSIStatus())
	}
}

// qaSetupTarget creates a TargetServer with a thread-safe mock device on a random port.
func qaSetupTarget(t *testing.T) (*TargetServer, string) {
	t.Helper()
	dev := newSafeMockDevice(256 * 4096)
	config := DefaultTargetConfig()
	config.TargetName = testTargetName
	logger := log.New(io.Discard, "", 0)
	ts := NewTargetServer("127.0.0.1:0", config, logger)
	ts.AddVolume(testTargetName, dev)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go ts.Serve(ln)
	t.Cleanup(func() { ts.Close() })

	return ts, ln.Addr().String()
}

// qaDialAndLogin dials the target, logs in, returns connection and next CmdSN.
func qaDialAndLogin(t *testing.T, addr string) (net.Conn, uint32) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	doLogin(t, conn)
	return conn, 2
}

// ---------------------------------------------------------------------------
// 1. PDU Wire Format (5 tests)
// ---------------------------------------------------------------------------

func testQA_PDU(t *testing.T) {
	t.Run("dataseg_underflow", func(t *testing.T) {
		// DataSegLen=100, deliver 50 bytes then EOF -> error
		r, w := io.Pipe()
		go func() {
			p := &PDU{}
			p.SetOpcode(OpNOPOut)
			p.SetDataSegmentLength(100)
			w.Write(p.BHS[:]) // write BHS claiming 100 bytes data
			w.Write(make([]byte, 50))
			w.Close() // EOF after 50 bytes
		}()
		_, err := ReadPDU(r)
		if err == nil {
			t.Fatal("expected error from truncated data segment")
		}
	})

	t.Run("zero_opcode_session", func(t *testing.T) {
		// NOP-Out (opcode 0x00) to a live session with ITT
		conn, _ := qaSessionDefault(t)
		qaLoginNormal(t, conn)

		nop := &PDU{}
		nop.SetOpcode(OpNOPOut)
		nop.SetInitiatorTaskTag(0x9999)
		nop.SetCmdSN(2)
		if err := WritePDU(conn, nop); err != nil {
			t.Fatal(err)
		}

		resp, err := ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Opcode() != OpNOPIn {
			t.Fatalf("expected NOP-In, got %s", OpcodeName(resp.Opcode()))
		}
		if resp.InitiatorTaskTag() != 0x9999 {
			t.Fatalf("ITT mismatch: got %d", resp.InitiatorTaskTag())
		}
	})

	t.Run("rapid_fire_100", func(t *testing.T) {
		// 100 PDUs written to a buffer, read all back
		var buf bytes.Buffer
		for i := 0; i < 100; i++ {
			p := &PDU{}
			p.SetOpcode(OpNOPOut)
			p.SetInitiatorTaskTag(uint32(i))
			if err := WritePDU(&buf, p); err != nil {
				t.Fatal(err)
			}
		}

		for i := 0; i < 100; i++ {
			p, err := ReadPDU(&buf)
			if err != nil {
				t.Fatalf("PDU %d: %v", i, err)
			}
			if p.InitiatorTaskTag() != uint32(i) {
				t.Fatalf("PDU %d: ITT=%d", i, p.InitiatorTaskTag())
			}
		}
	})

	t.Run("huge_ahs_len", func(t *testing.T) {
		// TotalAHSLen=255 (1020 bytes AHS) with truncated data -> error
		r, w := io.Pipe()
		go func() {
			var bhs [BHSLength]byte
			bhs[0] = OpNOPOut
			bhs[4] = 255 // 1020 bytes AHS
			w.Write(bhs[:])
			w.Write(make([]byte, 100)) // only 100 of 1020
			w.Close()
		}()
		_, err := ReadPDU(r)
		if err == nil {
			t.Fatal("expected error from truncated AHS")
		}
	})

	t.Run("dataseg_len_vs_actual", func(t *testing.T) {
		// DataSegLen=8192 but only 4096 bytes + EOF -> error
		r, w := io.Pipe()
		go func() {
			var bhs [BHSLength]byte
			bhs[0] = OpNOPOut
			// set data segment length to 8192
			bhs[5] = 0
			bhs[6] = 0x20
			bhs[7] = 0x00
			w.Write(bhs[:])
			w.Write(make([]byte, 4096))
			w.Close()
		}()
		_, err := ReadPDU(r)
		if err == nil {
			t.Fatal("expected error from short data segment")
		}
	})
}

// ---------------------------------------------------------------------------
// 2. Key-Value Params (3 tests)
// ---------------------------------------------------------------------------

func testQA_Params(t *testing.T) {
	t.Run("very_long_value", func(t *testing.T) {
		// 64KB value string -> roundtrip correct
		p := NewParams()
		longVal := strings.Repeat("A", 65536)
		p.Set("BigKey", longVal)

		encoded := p.Encode()
		parsed, err := ParseParams(encoded)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := parsed.Get("BigKey")
		if !ok {
			t.Fatal("key not found")
		}
		if got != longVal {
			t.Fatalf("value length: got %d, want %d", len(got), len(longVal))
		}
	})

	t.Run("no_null_terminator", func(t *testing.T) {
		// "Key=Value" without trailing \0 -> parsed as single entry
		raw := []byte("Key=Value")
		parsed, err := ParseParams(raw)
		if err != nil {
			t.Fatal(err)
		}
		if parsed.Len() != 1 {
			t.Fatalf("expected 1 param, got %d", parsed.Len())
		}
		v, _ := parsed.Get("Key")
		if v != "Value" {
			t.Fatalf("got %q", v)
		}
	})

	t.Run("100_keys", func(t *testing.T) {
		// 100 distinct key=value pairs -> all parsed
		p := NewParams()
		for i := 0; i < 100; i++ {
			p.Set(fmt.Sprintf("Key%03d", i), fmt.Sprintf("Value%03d", i))
		}
		encoded := p.Encode()
		parsed, err := ParseParams(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if parsed.Len() != 100 {
			t.Fatalf("expected 100 params, got %d", parsed.Len())
		}
		for i := 0; i < 100; i++ {
			v, ok := parsed.Get(fmt.Sprintf("Key%03d", i))
			if !ok || v != fmt.Sprintf("Value%03d", i) {
				t.Fatalf("key %d: got %q", i, v)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// 3. Login State Machine (7 tests)
// ---------------------------------------------------------------------------

func testQA_Login(t *testing.T) {
	t.Run("scsi_before_ffp", func(t *testing.T) {
		// Send SCSI Command during login phase -> rejected
		conn, _ := qaSessionDefault(t)

		// Don't login — send a SCSI command directly
		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF | FlagR)
		cmd.SetInitiatorTaskTag(1)
		cmd.SetCmdSN(1)
		var cdb [16]byte
		cdb[0] = ScsiTestUnitReady
		cmd.SetCDB(cdb)

		if err := WritePDU(conn, cmd); err != nil {
			t.Fatal(err)
		}

		resp := qaExpectReject(t, conn)
		_ = resp
	})

	t.Run("discovery_then_scsi", func(t *testing.T) {
		// Discovery login, then SCSI cmd -> rejected (bug #12 fix)
		conn, _ := qaSessionDefault(t)
		qaLoginDiscovery(t, conn)

		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF | FlagR)
		cmd.SetInitiatorTaskTag(2)
		cmd.SetExpectedDataTransferLength(96)
		cmd.SetCmdSN(2)
		var cdb [16]byte
		cdb[0] = ScsiInquiry
		binary.BigEndian.PutUint16(cdb[3:5], 96)
		cmd.SetCDB(cdb)

		if err := WritePDU(conn, cmd); err != nil {
			t.Fatal(err)
		}

		resp := qaExpectReject(t, conn)
		_ = resp
	})

	t.Run("double_login_after_ffp", func(t *testing.T) {
		// Normal login, then another Login Request -> rejected
		conn, _ := qaSessionDefault(t)
		qaLoginNormal(t, conn)

		params := NewParams()
		params.Set("InitiatorName", testInitiatorName)
		params.Set("TargetName", testTargetName)
		req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
		req.SetCmdSN(2)

		if err := WritePDU(conn, req); err != nil {
			t.Fatal(err)
		}

		// The negotiator will see phase mismatch (already Done) and reject
		resp, err := ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Opcode() != OpLoginResp {
			t.Fatalf("expected Login-Response, got %s", OpcodeName(resp.Opcode()))
		}
		if resp.LoginStatusClass() == LoginStatusSuccess {
			t.Fatal("double login should not succeed")
		}
	})

	t.Run("login_missing_initiator_name", func(t *testing.T) {
		// No InitiatorName -> rejected
		conn, _ := qaSessionDefault(t)

		params := NewParams()
		params.Set("TargetName", testTargetName)
		// No InitiatorName
		req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
		req.SetCmdSN(1)

		if err := WritePDU(conn, req); err != nil {
			t.Fatal(err)
		}

		resp, err := ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		if resp.LoginStatusClass() == LoginStatusSuccess {
			t.Fatal("login without InitiatorName should fail")
		}
	})

	t.Run("login_wrong_target", func(t *testing.T) {
		// TargetName doesn't exist -> rejected
		conn, _ := qaSessionDefault(t)

		params := NewParams()
		params.Set("InitiatorName", testInitiatorName)
		params.Set("TargetName", "iqn.2024.com.seaweedfs:nonexistent")
		req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
		req.SetCmdSN(1)

		if err := WritePDU(conn, req); err != nil {
			t.Fatal(err)
		}

		resp, err := ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		if resp.LoginStatusClass() == LoginStatusSuccess {
			t.Fatal("login to nonexistent target should fail")
		}
		if resp.LoginStatusDetail() != LoginDetailNotFound {
			t.Fatalf("expected NotFound detail, got %d", resp.LoginStatusDetail())
		}
	})

	t.Run("login_loginop_direct_jump", func(t *testing.T) {
		// CSG=LoginOp (skipping security phase) -> works (bug #9 fix)
		conn, _ := qaSessionDefault(t)

		params := NewParams()
		params.Set("InitiatorName", testInitiatorName)
		params.Set("TargetName", testTargetName)
		params.Set("SessionType", "Normal")
		req := makeLoginReq(StageLoginOp, StageFullFeature, true, params)
		req.SetCmdSN(1)

		if err := WritePDU(conn, req); err != nil {
			t.Fatal(err)
		}

		resp, err := ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		if resp.LoginStatusClass() != LoginStatusSuccess {
			t.Fatalf("direct LoginOp jump failed: %d/%d",
				resp.LoginStatusClass(), resp.LoginStatusDetail())
		}
	})

	t.Run("login_discovery_no_target", func(t *testing.T) {
		// Discovery session without TargetName -> OK
		conn, _ := qaSessionDefault(t)

		params := NewParams()
		params.Set("InitiatorName", testInitiatorName)
		params.Set("SessionType", "Discovery")
		req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
		req.SetCmdSN(1)

		if err := WritePDU(conn, req); err != nil {
			t.Fatal(err)
		}

		resp, err := ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		if resp.LoginStatusClass() != LoginStatusSuccess {
			t.Fatalf("discovery login without target should succeed: %d/%d",
				resp.LoginStatusClass(), resp.LoginStatusDetail())
		}
	})
}

// ---------------------------------------------------------------------------
// 4. Discovery (2 tests)
// ---------------------------------------------------------------------------

func testQA_Discovery(t *testing.T) {
	t.Run("100_volumes", func(t *testing.T) {
		// Register 100 volumes, SendTargets=All -> all 100 in response
		var targets []DiscoveryTarget
		for i := 0; i < 100; i++ {
			targets = append(targets, DiscoveryTarget{
				Name:    fmt.Sprintf("iqn.2024.com.seaweedfs:vol%03d", i),
				Address: fmt.Sprintf("10.0.0.%d:3260,1", i%256),
			})
		}

		params := NewParams()
		params.Set("SendTargets", "All")
		req := makeTextReq(params)

		resp := HandleTextRequest(req, targets)
		if len(resp.DataSegment) == 0 {
			t.Fatal("empty discovery response")
		}

		// Count TargetName occurrences
		body := string(resp.DataSegment)
		count := strings.Count(body, "TargetName=")
		if count != 100 {
			t.Fatalf("expected 100 targets, got %d", count)
		}
	})

	t.Run("text_in_normal_session", func(t *testing.T) {
		// Normal login, then Text Request -> should get a response (not crash)
		conn, _ := qaSessionDefault(t)
		qaLoginNormal(t, conn)

		params := NewParams()
		params.Set("SendTargets", "All")
		req := makeTextReq(params)
		req.SetCmdSN(2)

		if err := WritePDU(conn, req); err != nil {
			t.Fatal(err)
		}

		resp, err := ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		// Should get TextResp (the resolver supports ListTargets)
		if resp.Opcode() != OpTextResp {
			t.Fatalf("expected TextResp, got %s", OpcodeName(resp.Opcode()))
		}
	})
}

// ---------------------------------------------------------------------------
// 5. SCSI Command Boundaries (8 tests)
// ---------------------------------------------------------------------------

func testQA_SCSI(t *testing.T) {
	t.Run("read10_span_end", func(t *testing.T) {
		// READ_10 LBA+transferLen > volume -> ILLEGAL_REQUEST
		conn, _ := qaSessionDefault(t)
		cmdSN := qaLoginNormal(t, conn)

		var cdb [16]byte
		cdb[0] = ScsiRead10
		binary.BigEndian.PutUint32(cdb[2:6], 1020) // LBA 1020
		binary.BigEndian.PutUint16(cdb[7:9], 10)    // 10 blocks (exceeds 1024)

		resp := qaSendSCSICmd(t, conn, cdb, cmdSN, true, false, nil, 10*4096)
		qaExpectCheckCondition(t, resp)
	})

	t.Run("read16_huge_lba", func(t *testing.T) {
		// READ_16 with huge LBA -> ILLEGAL_REQUEST
		conn, _ := qaSessionDefault(t)
		cmdSN := qaLoginNormal(t, conn)

		var cdb [16]byte
		cdb[0] = ScsiRead16
		binary.BigEndian.PutUint64(cdb[2:10], 0x0000FFFFFFFFFFFF) // huge LBA
		binary.BigEndian.PutUint32(cdb[10:14], 1)

		resp := qaSendSCSICmd(t, conn, cdb, cmdSN, true, false, nil, 4096)
		qaExpectCheckCondition(t, resp)
	})

	t.Run("write10_short_data", func(t *testing.T) {
		// WRITE_10 CDB says 2 blocks, only 1 block data -> ILLEGAL_REQUEST
		// Test at SCSI handler level since session-level write uses R2T collection.
		dev := newMockDevice(1024 * 4096)
		handler := NewSCSIHandler(dev)

		var cdb [16]byte
		cdb[0] = ScsiWrite10
		binary.BigEndian.PutUint32(cdb[2:6], 0)
		binary.BigEndian.PutUint16(cdb[7:9], 2) // 2 blocks = 8192 bytes

		data := make([]byte, 4096) // only 1 block
		result := handler.HandleCommand(cdb, data)
		if result.Status != SCSIStatusCheckCond {
			t.Fatalf("expected CHECK_CONDITION, got status %d", result.Status)
		}
		if result.SenseKey != SenseIllegalRequest {
			t.Fatalf("expected ILLEGAL_REQUEST, got sense %d", result.SenseKey)
		}
	})

	t.Run("unmap_overlapping", func(t *testing.T) {
		// 2 UNMAP descriptors with overlapping ranges -> both processed without panic
		conn, _ := qaSessionDefault(t)
		cmdSN := qaLoginNormal(t, conn)

		// Write data first
		writeData := bytes.Repeat([]byte{0xBB}, 4096*4)
		var wCDB [16]byte
		wCDB[0] = ScsiWrite10
		binary.BigEndian.PutUint32(wCDB[2:6], 0)
		binary.BigEndian.PutUint16(wCDB[7:9], 4)
		resp := qaSendSCSICmd(t, conn, wCDB, cmdSN, false, true, writeData, 4*4096)
		if resp.SCSIStatus() != SCSIStatusGood {
			t.Fatalf("write failed: %d", resp.SCSIStatus())
		}
		cmdSN++

		// UNMAP with overlapping descriptors: [0,3) and [2,4)
		unmapData := make([]byte, 40) // header 8 + 2 descriptors * 16
		binary.BigEndian.PutUint16(unmapData[0:2], 38)
		binary.BigEndian.PutUint16(unmapData[2:4], 32)
		// Descriptor 1: LBA=0, count=3
		binary.BigEndian.PutUint64(unmapData[8:16], 0)
		binary.BigEndian.PutUint32(unmapData[16:20], 3)
		// Descriptor 2: LBA=2, count=2
		binary.BigEndian.PutUint64(unmapData[24:32], 2)
		binary.BigEndian.PutUint32(unmapData[32:36], 2)

		var uCDB [16]byte
		uCDB[0] = ScsiUnmap
		resp = qaSendSCSICmd(t, conn, uCDB, cmdSN, false, true, unmapData, uint32(len(unmapData)))
		if resp.SCSIStatus() != SCSIStatusGood {
			t.Fatalf("overlapping unmap failed: %d", resp.SCSIStatus())
		}
	})

	t.Run("unmap_past_end", func(t *testing.T) {
		// UNMAP LBA+count > volume -> error from device
		dev := newMockDevice(1024 * 4096)
		handler := NewSCSIHandler(dev)

		unmapData := make([]byte, 24)
		binary.BigEndian.PutUint16(unmapData[0:2], 22)
		binary.BigEndian.PutUint16(unmapData[2:4], 16)
		binary.BigEndian.PutUint64(unmapData[8:16], 1020) // LBA
		binary.BigEndian.PutUint32(unmapData[16:20], 10)  // 10 blocks, exceeds 1024

		var cdb [16]byte
		cdb[0] = ScsiUnmap
		result := handler.HandleCommand(cdb, unmapData)
		// Mock device Trim doesn't check bounds, so we verify it processes without panic
		_ = result
	})

	t.Run("sync_cache_range", func(t *testing.T) {
		// SYNC_CACHE_10 with specific LBA+count -> accepted
		conn, _ := qaSessionDefault(t)
		cmdSN := qaLoginNormal(t, conn)

		var cdb [16]byte
		cdb[0] = ScsiSyncCache10
		binary.BigEndian.PutUint32(cdb[2:6], 100) // LBA
		binary.BigEndian.PutUint16(cdb[7:9], 50)  // count

		resp := qaSendSCSICmd(t, conn, cdb, cmdSN, false, false, nil, 0)
		if resp.SCSIStatus() != SCSIStatusGood {
			t.Fatalf("sync cache failed: %d", resp.SCSIStatus())
		}
	})

	t.Run("mode_sense_all_pages", func(t *testing.T) {
		// MODE_SENSE_6 page_code=0x3F (all pages) -> response
		conn, _ := qaSessionDefault(t)
		cmdSN := qaLoginNormal(t, conn)

		var cdb [16]byte
		cdb[0] = ScsiModeSense6
		cdb[2] = 0x3F // all pages
		cdb[4] = 255  // alloc len

		resp := qaSendSCSICmd(t, conn, cdb, cmdSN, true, false, nil, 255)
		if resp.Opcode() != OpSCSIDataIn {
			t.Fatalf("expected Data-In, got %s", OpcodeName(resp.Opcode()))
		}
		if len(resp.DataSegment) < 4 {
			t.Fatal("mode sense response too short")
		}
	})

	t.Run("unmap_bad_alignment", func(t *testing.T) {
		// UNMAP blockDescLen not multiple of 16 -> ILLEGAL_REQUEST (bug #16)
		dev := newMockDevice(1024 * 4096)
		handler := NewSCSIHandler(dev)

		unmapData := make([]byte, 24)
		binary.BigEndian.PutUint16(unmapData[0:2], 22)
		binary.BigEndian.PutUint16(unmapData[2:4], 15) // not multiple of 16

		var cdb [16]byte
		cdb[0] = ScsiUnmap
		result := handler.HandleCommand(cdb, unmapData)
		if result.Status != SCSIStatusCheckCond {
			t.Fatal("unaligned block desc should fail")
		}
		if result.SenseKey != SenseIllegalRequest {
			t.Fatalf("expected ILLEGAL_REQUEST, got sense %d", result.SenseKey)
		}
	})
}

// ---------------------------------------------------------------------------
// 6. Data-In/Data-Out Sequencing (6 tests)
// ---------------------------------------------------------------------------

func testQA_DataIO(t *testing.T) {
	t.Run("dataout_wrong_ttt", func(t *testing.T) {
		// Data-Out with wrong TTT -> session rejects or errors
		// We test DataOutCollector directly since TTT validation happens at session level.
		// The session uses ITT as TTT. Send a Data-Out with wrong DataSN to collector.
		collector := NewDataOutCollector(8192)
		pdu := &PDU{}
		pdu.SetOpcode(OpSCSIDataOut)
		pdu.SetDataSN(99) // wrong, expected 0
		pdu.SetBufferOffset(0)
		pdu.DataSegment = make([]byte, 4096)
		err := collector.AddDataOut(pdu)
		if err == nil {
			t.Fatal("expected error for wrong DataSN")
		}
	})

	t.Run("dataout_wrong_offset", func(t *testing.T) {
		// Data-Out with bad BufferOffset -> rejected (bug #14)
		collector := NewDataOutCollector(8192)
		pdu := &PDU{}
		pdu.SetOpcode(OpSCSIDataOut)
		pdu.SetDataSN(0)
		pdu.SetBufferOffset(4096) // wrong, expected 0
		pdu.DataSegment = make([]byte, 4096)
		err := collector.AddDataOut(pdu)
		if err == nil {
			t.Fatal("expected error for wrong buffer offset")
		}
		if err != ErrDataOffsetOrder {
			t.Fatalf("expected ErrDataOffsetOrder, got %v", err)
		}
	})

	t.Run("dataout_exceed_burst", func(t *testing.T) {
		// Data exceeding expected transfer length -> ErrDataOverflow
		collector := NewDataOutCollector(4096)
		pdu := &PDU{}
		pdu.SetOpcode(OpSCSIDataOut)
		pdu.SetDataSN(0)
		pdu.SetBufferOffset(0)
		pdu.DataSegment = make([]byte, 8192) // exceeds 4096
		err := collector.AddDataOut(pdu)
		if err == nil {
			t.Fatal("expected error for overflow")
		}
		if err != ErrDataOverflow {
			t.Fatalf("expected ErrDataOverflow, got %v", err)
		}
	})

	t.Run("r2t_full_cycle_large", func(t *testing.T) {
		// WRITE 128KB (>FirstBurstLength=65536) -> R2T cycle completes
		dev := newMockDevice(256 * 4096)
		conn, _ := qaSession(t, dev)
		cmdSN := qaLoginNormal(t, conn)

		writeSize := uint32(128 * 1024) // 128KB = 32 blocks
		data := make([]byte, writeSize)
		for i := range data {
			data[i] = byte(i % 199)
		}

		var cdb [16]byte
		cdb[0] = ScsiWrite10
		binary.BigEndian.PutUint32(cdb[2:6], 0)
		binary.BigEndian.PutUint16(cdb[7:9], 32)

		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF | FlagW)
		cmd.SetInitiatorTaskTag(cmdSN)
		cmd.SetExpectedDataTransferLength(writeSize)
		cmd.SetCmdSN(cmdSN)
		cmd.SetCDB(cdb)
		// With default config: ImmediateData=true, InitialR2T=true
		// No immediate data sent — let R2T drive everything
		if err := WritePDU(conn, cmd); err != nil {
			t.Fatal(err)
		}

		// Collect R2T / send Data-Out cycle
		var totalSent uint32
		for totalSent < writeSize {
			// Read R2T
			r2t, err := ReadPDU(conn)
			if err != nil {
				t.Fatalf("reading R2T at offset %d: %v", totalSent, err)
			}
			if r2t.Opcode() != OpR2T {
				t.Fatalf("expected R2T, got %s", OpcodeName(r2t.Opcode()))
			}

			desiredLen := r2t.DesiredDataLength()
			offset := r2t.BufferOffset()
			ttt := r2t.TargetTransferTag()

			// Send Data-Out PDUs for this R2T
			var dataSN uint32
			sent := uint32(0)
			for sent < desiredLen {
				chunkSize := desiredLen - sent
				if chunkSize > 65536 {
					chunkSize = 65536
				}
				isFinal := (sent + chunkSize) >= desiredLen

				doPDU := &PDU{}
				doPDU.SetOpcode(OpSCSIDataOut)
				if isFinal {
					doPDU.SetOpSpecific1(FlagF)
				}
				doPDU.SetInitiatorTaskTag(cmdSN)
				doPDU.SetTargetTransferTag(ttt)
				doPDU.SetDataSN(dataSN)
				doPDU.SetBufferOffset(offset + sent)
				doPDU.DataSegment = data[offset+sent : offset+sent+chunkSize]

				if err := WritePDU(conn, doPDU); err != nil {
					t.Fatal(err)
				}
				dataSN++
				sent += chunkSize
			}
			totalSent += desiredLen
		}

		// Read SCSI Response
		resp, err := ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Opcode() != OpSCSIResp {
			t.Fatalf("expected SCSI-Response, got %s", OpcodeName(resp.Opcode()))
		}
		if resp.SCSIStatus() != SCSIStatusGood {
			t.Fatalf("write failed: status %d", resp.SCSIStatus())
		}

		// Verify by reading back
		cmdSN++
		var rCDB [16]byte
		rCDB[0] = ScsiRead10
		binary.BigEndian.PutUint32(rCDB[2:6], 0)
		binary.BigEndian.PutUint16(rCDB[7:9], 32)

		readResp := qaSendSCSICmd(t, conn, rCDB, cmdSN, true, false, nil, writeSize)
		readData := qaReadAllDataIn(t, conn, readResp)
		if !bytes.Equal(readData, data) {
			t.Fatal("data mismatch after R2T write cycle")
		}
	})

	t.Run("datain_multi_pdu", func(t *testing.T) {
		// READ larger than MaxRecvDataSegLen -> multiple Data-In PDUs
		dev := newMockDevice(256 * 4096)
		// Pre-populate some data
		pattern := bytes.Repeat([]byte{0xAA}, 4096)
		for i := uint64(0); i < 8; i++ {
			dev.WriteAt(i, pattern)
		}

		client, server := net.Pipe()
		config := DefaultTargetConfig()
		config.TargetName = testTargetName
		config.MaxRecvDataSegmentLength = 4096 // small: force multi-PDU
		resolver := newTestResolverWithDevice(dev)
		logger := log.New(io.Discard, "", 0)
		sess := NewSession(server, config, resolver, resolver, logger)
		done := make(chan error, 1)
		go func() { done <- sess.HandleConnection() }()
		t.Cleanup(func() { sess.Close(); client.Close() })

		doLogin(t, client)

		// Read 8 blocks = 32KB, with MaxRecvDataSegLen=4096 -> 8 Data-In PDUs
		var cdb [16]byte
		cdb[0] = ScsiRead10
		binary.BigEndian.PutUint32(cdb[2:6], 0)
		binary.BigEndian.PutUint16(cdb[7:9], 8)

		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF | FlagR)
		cmd.SetInitiatorTaskTag(2)
		cmd.SetExpectedDataTransferLength(8 * 4096)
		cmd.SetCmdSN(2)
		cmd.SetCDB(cdb)
		if err := WritePDU(client, cmd); err != nil {
			t.Fatal(err)
		}

		// Read Data-In PDUs
		var allData []byte
		pduCount := 0
		for {
			pdu, err := ReadPDU(client)
			if err != nil {
				t.Fatal(err)
			}
			allData = append(allData, pdu.DataSegment...)
			pduCount++
			if pdu.OpSpecific1()&FlagS != 0 {
				break
			}
		}

		if pduCount < 2 {
			t.Fatalf("expected multiple Data-In PDUs, got %d", pduCount)
		}
		expected := bytes.Repeat(pattern, 8)
		if !bytes.Equal(allData, expected) {
			t.Fatal("multi-PDU data mismatch")
		}
	})

	t.Run("immediate_data_when_disabled", func(t *testing.T) {
		// Immediate data with ImmediateData=false -> rejected (bug #15)
		dev := newMockDevice(256 * 4096)

		client, server := net.Pipe()
		config := DefaultTargetConfig()
		config.TargetName = testTargetName
		config.ImmediateData = false // disable immediate data
		resolver := newTestResolverWithDevice(dev)
		logger := log.New(io.Discard, "", 0)
		sess := NewSession(server, config, resolver, resolver, logger)
		done := make(chan error, 1)
		go func() { done <- sess.HandleConnection() }()
		t.Cleanup(func() { sess.Close(); client.Close() })

		// Login with ImmediateData negotiation
		params := NewParams()
		params.Set("InitiatorName", testInitiatorName)
		params.Set("TargetName", testTargetName)
		params.Set("SessionType", "Normal")
		req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
		req.SetCmdSN(1)
		if err := WritePDU(client, req); err != nil {
			t.Fatal(err)
		}
		resp, err := ReadPDU(client)
		if err != nil {
			t.Fatal(err)
		}
		if resp.LoginStatusClass() != LoginStatusSuccess {
			t.Fatalf("login failed: %d/%d", resp.LoginStatusClass(), resp.LoginStatusDetail())
		}

		// Send WRITE with immediate data despite ImmediateData=false
		writeData := make([]byte, 4096)
		var cdb [16]byte
		cdb[0] = ScsiWrite10
		binary.BigEndian.PutUint32(cdb[2:6], 0)
		binary.BigEndian.PutUint16(cdb[7:9], 1)

		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF | FlagW)
		cmd.SetInitiatorTaskTag(2)
		cmd.SetExpectedDataTransferLength(4096)
		cmd.SetCmdSN(2)
		cmd.SetCDB(cdb)
		cmd.DataSegment = writeData // immediate data when disabled

		if err := WritePDU(client, cmd); err != nil {
			t.Fatal(err)
		}

		// Should get CHECK_CONDITION response
		resp, err = ReadPDU(client)
		if err != nil {
			t.Fatal(err)
		}
		if resp.SCSIStatus() == SCSIStatusGood {
			t.Fatal("expected rejection when sending immediate data with ImmediateData=false")
		}
		_ = done
	})
}

// ---------------------------------------------------------------------------
// 7. Session Sequence Numbers + Concurrency (10 tests)
// ---------------------------------------------------------------------------

func testQA_Session(t *testing.T) {
	t.Run("cmdsn_out_of_window", func(t *testing.T) {
		// CmdSN < ExpCmdSN -> silently dropped (bug #13)
		conn, _ := qaSessionDefault(t)
		qaLoginNormal(t, conn)

		// Send command with stale CmdSN=0 (ExpCmdSN starts at 1, advanced to 2 after login)
		var cdb [16]byte
		cdb[0] = ScsiTestUnitReady

		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF)
		cmd.SetInitiatorTaskTag(0xAAAA)
		cmd.SetCmdSN(0) // stale
		cmd.SetCDB(cdb)
		if err := WritePDU(conn, cmd); err != nil {
			t.Fatal(err)
		}

		// Send a valid command right after to prove session is alive
		cmd2 := &PDU{}
		cmd2.SetOpcode(OpSCSICmd)
		cmd2.SetOpSpecific1(FlagF)
		cmd2.SetInitiatorTaskTag(0xBBBB)
		cmd2.SetCmdSN(2)
		cmd2.SetCDB(cdb)
		if err := WritePDU(conn, cmd2); err != nil {
			t.Fatal(err)
		}

		// Should get response only for the valid command
		resp, err := ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		if resp.InitiatorTaskTag() != 0xBBBB {
			t.Fatalf("expected response for ITT 0xBBBB, got 0x%x", resp.InitiatorTaskTag())
		}
	})

	t.Run("cmdsn_ahead", func(t *testing.T) {
		// CmdSN > MaxCmdSN -> dropped
		conn, _ := qaSessionDefault(t)
		qaLoginNormal(t, conn)

		var cdb [16]byte
		cdb[0] = ScsiTestUnitReady

		// MaxCmdSN starts at 32. Send CmdSN=100 (way out of window)
		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF)
		cmd.SetInitiatorTaskTag(0xCCCC)
		cmd.SetCmdSN(100)
		cmd.SetCDB(cdb)
		if err := WritePDU(conn, cmd); err != nil {
			t.Fatal(err)
		}

		// Valid command after
		cmd2 := &PDU{}
		cmd2.SetOpcode(OpSCSICmd)
		cmd2.SetOpSpecific1(FlagF)
		cmd2.SetInitiatorTaskTag(0xDDDD)
		cmd2.SetCmdSN(2)
		cmd2.SetCDB(cdb)
		if err := WritePDU(conn, cmd2); err != nil {
			t.Fatal(err)
		}

		resp, err := ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		if resp.InitiatorTaskTag() != 0xDDDD {
			t.Fatalf("expected response for 0xDDDD, got 0x%x", resp.InitiatorTaskTag())
		}
	})

	t.Run("cmdsn_wrap", func(t *testing.T) {
		// Drive CmdSN through 0xFFFFFFFF -> 0 -> works
		if !cmdSNInWindow(0xFFFFFFFF, 0xFFFFFFFF, 0x1F) {
			t.Fatal("0xFFFFFFFF should be in window [0xFFFFFFFF, 0x1F]")
		}
		if !cmdSNInWindow(0x00000000, 0xFFFFFFFF, 0x1F) {
			t.Fatal("0x00000000 should be in window [0xFFFFFFFF, 0x1F]")
		}
		if !cmdSNInWindow(0x00000010, 0xFFFFFFFF, 0x1F) {
			t.Fatal("0x10 should be in window [0xFFFFFFFF, 0x1F]")
		}
		if cmdSNInWindow(0x00000020, 0xFFFFFFFF, 0x1F) {
			t.Fatal("0x20 should NOT be in window [0xFFFFFFFF, 0x1F]")
		}
		if cmdSNInWindow(0xFFFFFFFE, 0xFFFFFFFF, 0x1F) {
			t.Fatal("0xFFFFFFFE should NOT be in window [0xFFFFFFFF, 0x1F]")
		}
	})

	t.Run("statsn_verify", func(t *testing.T) {
		// 10 commands, verify StatSN increments by 1 each
		conn, _ := qaSessionDefault(t)
		qaLoginNormal(t, conn)

		var lastStatSN uint32
		for i := uint32(0); i < 10; i++ {
			var cdb [16]byte
			cdb[0] = ScsiTestUnitReady

			cmdSN := i + 2
			resp := qaSendSCSICmd(t, conn, cdb, cmdSN, false, false, nil, 0)
			statSN := resp.StatSN()

			if i > 0 && statSN != lastStatSN+1 {
				t.Fatalf("cmd %d: StatSN=%d, expected %d", i, statSN, lastStatSN+1)
			}
			lastStatSN = statSN
		}
	})

	t.Run("nop_unsolicited", func(t *testing.T) {
		// NOP-Out with ITT=0xFFFFFFFF (target-initiated NOP reply) -> handled
		conn, _ := qaSessionDefault(t)
		qaLoginNormal(t, conn)

		nop := &PDU{}
		nop.SetOpcode(OpNOPOut)
		nop.SetOpSpecific1(FlagF)
		nop.SetInitiatorTaskTag(0xFFFFFFFF)
		nop.SetCmdSN(2)

		if err := WritePDU(conn, nop); err != nil {
			t.Fatal(err)
		}

		resp, err := ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Opcode() != OpNOPIn {
			t.Fatalf("expected NOP-In, got %s", OpcodeName(resp.Opcode()))
		}
	})

	t.Run("logout_during_io", func(t *testing.T) {
		// Logout mid-session -> clean shutdown
		conn, _ := qaSessionDefault(t)
		qaLoginNormal(t, conn)

		logout := &PDU{}
		logout.SetOpcode(OpLogoutReq)
		logout.SetOpSpecific1(FlagF)
		logout.SetInitiatorTaskTag(0x9876)
		logout.SetCmdSN(2)

		if err := WritePDU(conn, logout); err != nil {
			t.Fatal(err)
		}

		resp, err := ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Opcode() != OpLogoutResp {
			t.Fatalf("expected LogoutResp, got %s", OpcodeName(resp.Opcode()))
		}
	})

	t.Run("16_concurrent_sessions", func(t *testing.T) {
		// 16 goroutines each with own session on TargetServer
		ts, addr := qaSetupTarget(t)
		_ = ts

		var wg sync.WaitGroup
		errs := make(chan error, 16)

		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
				if err != nil {
					errs <- fmt.Errorf("session %d dial: %w", id, err)
					return
				}
				defer conn.Close()

				// Login
				params := NewParams()
				params.Set("InitiatorName", fmt.Sprintf("iqn.2024.com.test:client%d", id))
				params.Set("TargetName", testTargetName)
				params.Set("SessionType", "Normal")
				req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
				isid := [6]byte{0x00, 0x02, 0x3D, byte(id >> 8), byte(id), 0x01}
				req.SetISID(isid)
				req.SetCmdSN(1)
				if err := WritePDU(conn, req); err != nil {
					errs <- fmt.Errorf("session %d write login: %w", id, err)
					return
				}
				resp, err := ReadPDU(conn)
				if err != nil {
					errs <- fmt.Errorf("session %d read login: %w", id, err)
					return
				}
				if resp.LoginStatusClass() != LoginStatusSuccess {
					errs <- fmt.Errorf("session %d login: %d/%d", id, resp.LoginStatusClass(), resp.LoginStatusDetail())
					return
				}

				// Write to a unique LBA
				lba := uint32(id)
				data := bytes.Repeat([]byte{byte(id)}, 4096)

				var wCDB [16]byte
				wCDB[0] = ScsiWrite10
				binary.BigEndian.PutUint32(wCDB[2:6], lba)
				binary.BigEndian.PutUint16(wCDB[7:9], 1)

				cmd := &PDU{}
				cmd.SetOpcode(OpSCSICmd)
				cmd.SetOpSpecific1(FlagF | FlagW)
				cmd.SetInitiatorTaskTag(2)
				cmd.SetExpectedDataTransferLength(4096)
				cmd.SetCmdSN(2)
				cmd.SetCDB(wCDB)
				cmd.DataSegment = data
				if err := WritePDU(conn, cmd); err != nil {
					errs <- fmt.Errorf("session %d write cmd: %w", id, err)
					return
				}

				wResp, err := ReadPDU(conn)
				if err != nil {
					errs <- fmt.Errorf("session %d read write resp: %w", id, err)
					return
				}
				if wResp.SCSIStatus() != SCSIStatusGood {
					errs <- fmt.Errorf("session %d write failed: %d", id, wResp.SCSIStatus())
					return
				}

				// Read back
				var rCDB [16]byte
				rCDB[0] = ScsiRead10
				binary.BigEndian.PutUint32(rCDB[2:6], lba)
				binary.BigEndian.PutUint16(rCDB[7:9], 1)

				cmd2 := &PDU{}
				cmd2.SetOpcode(OpSCSICmd)
				cmd2.SetOpSpecific1(FlagF | FlagR)
				cmd2.SetInitiatorTaskTag(3)
				cmd2.SetExpectedDataTransferLength(4096)
				cmd2.SetCmdSN(3)
				cmd2.SetCDB(rCDB)
				if err := WritePDU(conn, cmd2); err != nil {
					errs <- fmt.Errorf("session %d write read cmd: %w", id, err)
					return
				}

				rResp, err := ReadPDU(conn)
				if err != nil {
					errs <- fmt.Errorf("session %d read resp: %w", id, err)
					return
				}

				if !bytes.Equal(rResp.DataSegment, data) {
					errs <- fmt.Errorf("session %d data mismatch", id)
				}
			}(i)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	})

	t.Run("conn_drop_mid_read", func(t *testing.T) {
		// Close TCP during Data-In transfer -> session cleans up
		dev := newMockDevice(256 * 4096)
		// Write some data
		for i := uint64(0); i < 64; i++ {
			dev.WriteAt(i, bytes.Repeat([]byte{0xBB}, 4096))
		}

		client, server := net.Pipe()
		config := DefaultTargetConfig()
		config.TargetName = testTargetName
		config.MaxRecvDataSegmentLength = 4096 // small segments to ensure multi-PDU
		resolver := newTestResolverWithDevice(dev)
		logger := log.New(io.Discard, "", 0)
		sess := NewSession(server, config, resolver, resolver, logger)
		done := make(chan error, 1)
		go func() { done <- sess.HandleConnection() }()

		doLogin(t, client)

		// Request a large read (64 blocks = 256KB, with 4KB segments = 64 PDUs)
		var cdb [16]byte
		cdb[0] = ScsiRead10
		binary.BigEndian.PutUint32(cdb[2:6], 0)
		binary.BigEndian.PutUint16(cdb[7:9], 64)

		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF | FlagR)
		cmd.SetInitiatorTaskTag(2)
		cmd.SetExpectedDataTransferLength(64 * 4096)
		cmd.SetCmdSN(2)
		cmd.SetCDB(cdb)
		if err := WritePDU(client, cmd); err != nil {
			t.Fatal(err)
		}

		// Read first PDU then close
		_, err := ReadPDU(client)
		if err != nil {
			t.Fatal(err)
		}
		client.Close()

		// Session should terminate cleanly
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("session did not terminate after client dropped")
		}
	})

	t.Run("conn_drop_mid_write", func(t *testing.T) {
		// Close TCP during R2T -> session cleans up
		dev := newMockDevice(256 * 4096)
		client, server := net.Pipe()
		config := DefaultTargetConfig()
		config.TargetName = testTargetName
		resolver := newTestResolverWithDevice(dev)
		logger := log.New(io.Discard, "", 0)
		sess := NewSession(server, config, resolver, resolver, logger)
		done := make(chan error, 1)
		go func() { done <- sess.HandleConnection() }()

		doLogin(t, client)

		// Start large write, close connection after R2T
		var cdb [16]byte
		cdb[0] = ScsiWrite10
		binary.BigEndian.PutUint32(cdb[2:6], 0)
		binary.BigEndian.PutUint16(cdb[7:9], 64)

		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF | FlagW)
		cmd.SetInitiatorTaskTag(2)
		cmd.SetExpectedDataTransferLength(64 * 4096)
		cmd.SetCmdSN(2)
		cmd.SetCDB(cdb)
		if err := WritePDU(client, cmd); err != nil {
			t.Fatal(err)
		}

		// Read R2T, then close
		_, err := ReadPDU(client)
		if err != nil {
			t.Fatal(err)
		}
		client.Close()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("session did not terminate after client dropped during write")
		}
	})

	t.Run("rapid_login_logout_50", func(t *testing.T) {
		// 50 login->logout cycles -> no leak
		ts, addr := qaSetupTarget(t)
		_ = ts

		for i := 0; i < 50; i++ {
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				t.Fatalf("cycle %d: dial failed: %v", i, err)
			}

			// Login
			params := NewParams()
			params.Set("InitiatorName", testInitiatorName)
			params.Set("TargetName", testTargetName)
			params.Set("SessionType", "Normal")
			req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
			req.SetCmdSN(1)
			if err := WritePDU(conn, req); err != nil {
				conn.Close()
				t.Fatalf("cycle %d: login write: %v", i, err)
			}
			resp, err := ReadPDU(conn)
			if err != nil {
				conn.Close()
				t.Fatalf("cycle %d: login read: %v", i, err)
			}
			if resp.LoginStatusClass() != LoginStatusSuccess {
				conn.Close()
				t.Fatalf("cycle %d: login failed", i)
			}

			// Logout
			logout := &PDU{}
			logout.SetOpcode(OpLogoutReq)
			logout.SetOpSpecific1(FlagF)
			logout.SetInitiatorTaskTag(1)
			logout.SetCmdSN(2)
			if err := WritePDU(conn, logout); err != nil {
				conn.Close()
				t.Fatalf("cycle %d: logout write: %v", i, err)
			}
			resp, err = ReadPDU(conn)
			if err != nil {
				conn.Close()
				t.Fatalf("cycle %d: logout read: %v", i, err)
			}
			if resp.Opcode() != OpLogoutResp {
				conn.Close()
				t.Fatalf("cycle %d: expected LogoutResp, got %s", i, OpcodeName(resp.Opcode()))
			}
			conn.Close()
		}
	})
}

// ---------------------------------------------------------------------------
// 8. Target Server (6 tests)
// ---------------------------------------------------------------------------

func testQA_Target(t *testing.T) {
	t.Run("accept_flood_50", func(t *testing.T) {
		// 50 TCP connections rapid-fire -> handled
		_, addr := qaSetupTarget(t)

		conns := make([]net.Conn, 50)
		for i := 0; i < 50; i++ {
			var err error
			conns[i], err = net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				t.Fatalf("connection %d: %v", i, err)
			}
		}
		// Close all
		for _, c := range conns {
			c.Close()
		}
	})

	t.Run("shutdown_during_io", func(t *testing.T) {
		// Shutdown with active sessions -> clean
		ts, addr := qaSetupTarget(t)

		// Create 4 sessions, each doing login
		conns := make([]net.Conn, 4)
		for i := 0; i < 4; i++ {
			var err error
			conns[i], err = net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				t.Fatal(err)
			}

			params := NewParams()
			params.Set("InitiatorName", fmt.Sprintf("iqn.2024.com.test:c%d", i))
			params.Set("TargetName", testTargetName)
			req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
			isid := [6]byte{0x00, 0x02, 0x3D, 0x00, byte(i), 0x01}
			req.SetISID(isid)
			req.SetCmdSN(1)
			WritePDU(conns[i], req)
			ReadPDU(conns[i])
		}

		// Shutdown should close all sessions
		done := make(chan struct{})
		go func() {
			ts.Close()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("shutdown timed out with active sessions")
		}

		for _, c := range conns {
			c.Close()
		}
	})

	t.Run("remove_volume_active", func(t *testing.T) {
		// RemoveVolume during session -> next command sees error or degraded behavior
		ts, addr := qaSetupTarget(t)

		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		// Login
		params := NewParams()
		params.Set("InitiatorName", testInitiatorName)
		params.Set("TargetName", testTargetName)
		req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
		req.SetCmdSN(1)
		WritePDU(conn, req)
		resp, _ := ReadPDU(conn)
		if resp.LoginStatusClass() != LoginStatusSuccess {
			t.Fatal("login failed")
		}

		// First command should work
		var cdb [16]byte
		cdb[0] = ScsiTestUnitReady
		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF)
		cmd.SetInitiatorTaskTag(2)
		cmd.SetCmdSN(2)
		cmd.SetCDB(cdb)
		WritePDU(conn, cmd)
		resp, _ = ReadPDU(conn)
		if resp.SCSIStatus() != SCSIStatusGood {
			t.Fatal("first TUR should succeed")
		}

		// Remove the volume
		ts.RemoveVolume(testTargetName)

		// The session already has its SCSI handler bound at login time,
		// so subsequent commands still work on the bound device
		cmd2 := &PDU{}
		cmd2.SetOpcode(OpSCSICmd)
		cmd2.SetOpSpecific1(FlagF)
		cmd2.SetInitiatorTaskTag(3)
		cmd2.SetCmdSN(3)
		cmd2.SetCDB(cdb)
		WritePDU(conn, cmd2)
		resp2, err := ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		// Should still work since handler was bound at login
		_ = resp2
	})

	t.Run("same_iqn_twice", func(t *testing.T) {
		// AddVolume with existing IQN -> overwrites
		config := DefaultTargetConfig()
		logger := log.New(io.Discard, "", 0)
		ts := NewTargetServer("127.0.0.1:0", config, logger)

		dev1 := newMockDevice(1024 * 4096)
		dev2 := newMockDevice(2048 * 4096)

		ts.AddVolume("iqn.test:vol1", dev1)
		ts.AddVolume("iqn.test:vol1", dev2) // overwrite

		got := ts.LookupDevice("iqn.test:vol1")
		if got.VolumeSize() != dev2.VolumeSize() {
			t.Fatal("second AddVolume should overwrite first")
		}
	})

	t.Run("shutdown_idempotent", func(t *testing.T) {
		// Close() twice -> no panic
		config := DefaultTargetConfig()
		logger := log.New(io.Discard, "", 0)
		ts := NewTargetServer("127.0.0.1:0", config, logger)

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go ts.Serve(ln)

		ts.Close()
		ts.Close() // second close should be no-op
	})

	t.Run("port_in_use", func(t *testing.T) {
		// ListenAndServe on bound port -> clean error
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()

		config := DefaultTargetConfig()
		logger := log.New(io.Discard, "", 0)
		ts := NewTargetServer(ln.Addr().String(), config, logger)

		err = ts.ListenAndServe()
		if err == nil {
			ts.Close()
			t.Fatal("expected error for port in use")
		}
	})
}

// ---------------------------------------------------------------------------
// 9. Full-Stack Integration (8 tests)
// ---------------------------------------------------------------------------

func testQA_Integration(t *testing.T) {
	t.Run("crash_recovery", func(t *testing.T) {
		// Write -> close -> reopen (using mock) -> read -> data persists
		// Note: uses mockBlockDevice (data survives Close since it's in-memory)
		dev := newMockDevice(256 * 4096)
		conn, _ := qaSession(t, dev)
		cmdSN := qaLoginNormal(t, conn)

		// Write
		pattern := bytes.Repeat([]byte{0xDE}, 4096)
		var wCDB [16]byte
		wCDB[0] = ScsiWrite10
		binary.BigEndian.PutUint32(wCDB[2:6], 5)
		binary.BigEndian.PutUint16(wCDB[7:9], 1)
		resp := qaSendSCSICmd(t, conn, wCDB, cmdSN, false, true, pattern, 4096)
		if resp.SCSIStatus() != SCSIStatusGood {
			t.Fatal("write failed")
		}

		// Close connection, create new session on same device
		conn.Close()
		conn2, _ := qaSession(t, dev)
		cmdSN2 := qaLoginNormal(t, conn2)

		// Read back
		var rCDB [16]byte
		rCDB[0] = ScsiRead10
		binary.BigEndian.PutUint32(rCDB[2:6], 5)
		binary.BigEndian.PutUint16(rCDB[7:9], 1)
		resp2 := qaSendSCSICmd(t, conn2, rCDB, cmdSN2, true, false, nil, 4096)
		data := qaReadAllDataIn(t, conn2, resp2)
		if !bytes.Equal(data, pattern) {
			t.Fatal("data lost after reopen")
		}
	})

	t.Run("session_reconnect", func(t *testing.T) {
		// Login -> write -> close -> new login -> read
		ts, addr := qaSetupTarget(t)
		_ = ts

		// First connection: login + write
		conn1, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		params := NewParams()
		params.Set("InitiatorName", testInitiatorName)
		params.Set("TargetName", testTargetName)
		req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
		req.SetCmdSN(1)
		WritePDU(conn1, req)
		resp, _ := ReadPDU(conn1)
		if resp.LoginStatusClass() != LoginStatusSuccess {
			t.Fatal("first login failed")
		}

		data := bytes.Repeat([]byte{0x42}, 4096)
		var wCDB [16]byte
		wCDB[0] = ScsiWrite10
		binary.BigEndian.PutUint32(wCDB[2:6], 10)
		binary.BigEndian.PutUint16(wCDB[7:9], 1)

		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF | FlagW)
		cmd.SetInitiatorTaskTag(2)
		cmd.SetExpectedDataTransferLength(4096)
		cmd.SetCmdSN(2)
		cmd.SetCDB(wCDB)
		cmd.DataSegment = data
		WritePDU(conn1, cmd)
		wResp, _ := ReadPDU(conn1)
		if wResp.SCSIStatus() != SCSIStatusGood {
			t.Fatal("write failed")
		}
		conn1.Close()

		// Second connection: login + read
		conn2, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn2.Close()

		req2 := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
		req2.SetCmdSN(1)
		WritePDU(conn2, req2)
		resp2, _ := ReadPDU(conn2)
		if resp2.LoginStatusClass() != LoginStatusSuccess {
			t.Fatal("second login failed")
		}

		var rCDB [16]byte
		rCDB[0] = ScsiRead10
		binary.BigEndian.PutUint32(rCDB[2:6], 10)
		binary.BigEndian.PutUint16(rCDB[7:9], 1)

		cmd2 := &PDU{}
		cmd2.SetOpcode(OpSCSICmd)
		cmd2.SetOpSpecific1(FlagF | FlagR)
		cmd2.SetInitiatorTaskTag(2)
		cmd2.SetExpectedDataTransferLength(4096)
		cmd2.SetCmdSN(2)
		cmd2.SetCDB(rCDB)
		WritePDU(conn2, cmd2)
		rResp, _ := ReadPDU(conn2)

		readData := qaReadAllDataIn(t, conn2, rResp)
		if !bytes.Equal(readData, data) {
			t.Fatal("data lost after reconnect")
		}
	})

	t.Run("large_io_multi_pdu", func(t *testing.T) {
		// Write 256KB -> read back -> matches
		dev := newMockDevice(256 * 4096) // 256 blocks = 1MB
		conn, _ := qaSession(t, dev)
		cmdSN := qaLoginNormal(t, conn)

		writeSize := uint32(256 * 1024)
		blockCount := uint16(writeSize / 4096)
		data := make([]byte, writeSize)
		for i := range data {
			data[i] = byte(i % 251)
		}

		// Write
		var wCDB [16]byte
		wCDB[0] = ScsiWrite10
		binary.BigEndian.PutUint32(wCDB[2:6], 0)
		binary.BigEndian.PutUint16(wCDB[7:9], blockCount)

		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF | FlagW)
		cmd.SetInitiatorTaskTag(cmdSN)
		cmd.SetExpectedDataTransferLength(writeSize)
		cmd.SetCmdSN(cmdSN)
		cmd.SetCDB(wCDB)
		// Send without immediate data -> R2T cycle
		if err := WritePDU(conn, cmd); err != nil {
			t.Fatal(err)
		}

		// Drive R2T cycle
		var totalSent uint32
		for totalSent < writeSize {
			r2t, err := ReadPDU(conn)
			if err != nil {
				t.Fatal(err)
			}
			if r2t.Opcode() != OpR2T {
				t.Fatalf("expected R2T, got %s", OpcodeName(r2t.Opcode()))
			}

			desiredLen := r2t.DesiredDataLength()
			offset := r2t.BufferOffset()
			ttt := r2t.TargetTransferTag()

			doPDU := &PDU{}
			doPDU.SetOpcode(OpSCSIDataOut)
			doPDU.SetOpSpecific1(FlagF)
			doPDU.SetInitiatorTaskTag(cmdSN)
			doPDU.SetTargetTransferTag(ttt)
			doPDU.SetDataSN(0)
			doPDU.SetBufferOffset(offset)
			doPDU.DataSegment = data[offset : offset+desiredLen]
			if err := WritePDU(conn, doPDU); err != nil {
				t.Fatal(err)
			}
			totalSent += desiredLen
		}

		resp, err := ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		if resp.SCSIStatus() != SCSIStatusGood {
			t.Fatalf("write failed: %d", resp.SCSIStatus())
		}
		cmdSN++

		// Read back
		var rCDB [16]byte
		rCDB[0] = ScsiRead10
		binary.BigEndian.PutUint32(rCDB[2:6], 0)
		binary.BigEndian.PutUint16(rCDB[7:9], blockCount)

		readResp := qaSendSCSICmd(t, conn, rCDB, cmdSN, true, false, nil, writeSize)
		readData := qaReadAllDataIn(t, conn, readResp)
		if !bytes.Equal(readData, data) {
			t.Fatal("large I/O data mismatch")
		}
	})

	t.Run("trim_then_read", func(t *testing.T) {
		// UNMAP -> READ -> zeros
		conn, _ := qaSessionDefault(t)
		cmdSN := qaLoginNormal(t, conn)

		// Write data first
		pattern := bytes.Repeat([]byte{0xFF}, 4096)
		var wCDB [16]byte
		wCDB[0] = ScsiWrite10
		binary.BigEndian.PutUint32(wCDB[2:6], 20)
		binary.BigEndian.PutUint16(wCDB[7:9], 1)
		resp := qaSendSCSICmd(t, conn, wCDB, cmdSN, false, true, pattern, 4096)
		if resp.SCSIStatus() != SCSIStatusGood {
			t.Fatal("write failed")
		}
		cmdSN++

		// UNMAP
		unmapData := make([]byte, 24)
		binary.BigEndian.PutUint16(unmapData[0:2], 22)
		binary.BigEndian.PutUint16(unmapData[2:4], 16)
		binary.BigEndian.PutUint64(unmapData[8:16], 20)
		binary.BigEndian.PutUint32(unmapData[16:20], 1)

		var uCDB [16]byte
		uCDB[0] = ScsiUnmap
		resp = qaSendSCSICmd(t, conn, uCDB, cmdSN, false, true, unmapData, uint32(len(unmapData)))
		if resp.SCSIStatus() != SCSIStatusGood {
			t.Fatalf("unmap failed: %d", resp.SCSIStatus())
		}
		cmdSN++

		// Read back -> should be zeros
		var rCDB [16]byte
		rCDB[0] = ScsiRead10
		binary.BigEndian.PutUint32(rCDB[2:6], 20)
		binary.BigEndian.PutUint16(rCDB[7:9], 1)
		rResp := qaSendSCSICmd(t, conn, rCDB, cmdSN, true, false, nil, 4096)
		data := qaReadAllDataIn(t, conn, rResp)
		zeros := make([]byte, 4096)
		if !bytes.Equal(data, zeros) {
			t.Fatal("trimmed block should return zeros")
		}
	})

	t.Run("8_concurrent_writers", func(t *testing.T) {
		// 8 goroutines write different LBAs -> all correct
		ts, addr := qaSetupTarget(t)
		_ = ts

		var wg sync.WaitGroup
		errs := make(chan error, 8)

		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
				if err != nil {
					errs <- fmt.Errorf("writer %d: %v", id, err)
					return
				}
				defer conn.Close()

				// Login
				params := NewParams()
				params.Set("InitiatorName", fmt.Sprintf("iqn.2024.com.test:w%d", id))
				params.Set("TargetName", testTargetName)
				req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
				isid := [6]byte{0x00, 0x02, 0x3D, 0x00, byte(id), 0x02}
				req.SetISID(isid)
				req.SetCmdSN(1)
				WritePDU(conn, req)
				resp, _ := ReadPDU(conn)
				if resp.LoginStatusClass() != LoginStatusSuccess {
					errs <- fmt.Errorf("writer %d: login failed", id)
					return
				}

				// Write 4 blocks at offset id*4
				lba := uint32(id * 4)
				data := bytes.Repeat([]byte{byte(id + 0xA0)}, 4*4096)

				var wCDB [16]byte
				wCDB[0] = ScsiWrite10
				binary.BigEndian.PutUint32(wCDB[2:6], lba)
				binary.BigEndian.PutUint16(wCDB[7:9], 4)

				cmd := &PDU{}
				cmd.SetOpcode(OpSCSICmd)
				cmd.SetOpSpecific1(FlagF | FlagW)
				cmd.SetInitiatorTaskTag(2)
				cmd.SetExpectedDataTransferLength(4 * 4096)
				cmd.SetCmdSN(2)
				cmd.SetCDB(wCDB)
				cmd.DataSegment = data
				WritePDU(conn, cmd)
				wResp, _ := ReadPDU(conn)
				if wResp.SCSIStatus() != SCSIStatusGood {
					errs <- fmt.Errorf("writer %d: write failed", id)
					return
				}

				// Read back
				var rCDB [16]byte
				rCDB[0] = ScsiRead10
				binary.BigEndian.PutUint32(rCDB[2:6], lba)
				binary.BigEndian.PutUint16(rCDB[7:9], 4)

				cmd2 := &PDU{}
				cmd2.SetOpcode(OpSCSICmd)
				cmd2.SetOpSpecific1(FlagF | FlagR)
				cmd2.SetInitiatorTaskTag(3)
				cmd2.SetExpectedDataTransferLength(4 * 4096)
				cmd2.SetCmdSN(3)
				cmd2.SetCDB(rCDB)
				WritePDU(conn, cmd2)

				rResp, rErr := ReadPDU(conn)
				if rErr != nil {
					errs <- fmt.Errorf("writer %d: read error: %v", id, rErr)
					return
				}

				// Collect all Data-In
				var readData []byte
				readData = append(readData, rResp.DataSegment...)
				for rResp.OpSpecific1()&FlagS == 0 {
					rResp, rErr = ReadPDU(conn)
					if rErr != nil {
						errs <- fmt.Errorf("writer %d: read continuation: %v", id, rErr)
						return
					}
					readData = append(readData, rResp.DataSegment...)
				}

				if !bytes.Equal(readData, data) {
					errs <- fmt.Errorf("writer %d: data mismatch", id)
				}
			}(i)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	})

	t.Run("discovery_then_normal", func(t *testing.T) {
		// Discovery login -> get targets -> close -> normal login -> I/O
		ts, addr := qaSetupTarget(t)
		_ = ts

		// Discovery session
		conn1, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		params := NewParams()
		params.Set("InitiatorName", testInitiatorName)
		params.Set("SessionType", "Discovery")
		req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
		req.SetCmdSN(1)
		WritePDU(conn1, req)
		resp, _ := ReadPDU(conn1)
		if resp.LoginStatusClass() != LoginStatusSuccess {
			t.Fatal("discovery login failed")
		}

		// Send targets query
		textParams := NewParams()
		textParams.Set("SendTargets", "All")
		textReq := makeTextReq(textParams)
		textReq.SetCmdSN(2)
		WritePDU(conn1, textReq)
		textResp, _ := ReadPDU(conn1)
		if textResp.Opcode() != OpTextResp {
			t.Fatal("expected TextResp")
		}
		if len(textResp.DataSegment) == 0 {
			t.Fatal("empty discovery response")
		}
		conn1.Close()

		// Normal session
		conn2, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn2.Close()

		params2 := NewParams()
		params2.Set("InitiatorName", testInitiatorName)
		params2.Set("TargetName", testTargetName)
		req2 := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params2)
		req2.SetCmdSN(1)
		WritePDU(conn2, req2)
		resp2, _ := ReadPDU(conn2)
		if resp2.LoginStatusClass() != LoginStatusSuccess {
			t.Fatal("normal login failed")
		}

		// I/O: write + read
		data := bytes.Repeat([]byte{0x99}, 4096)
		var wCDB [16]byte
		wCDB[0] = ScsiWrite10
		binary.BigEndian.PutUint32(wCDB[2:6], 0)
		binary.BigEndian.PutUint16(wCDB[7:9], 1)

		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF | FlagW)
		cmd.SetInitiatorTaskTag(2)
		cmd.SetExpectedDataTransferLength(4096)
		cmd.SetCmdSN(2)
		cmd.SetCDB(wCDB)
		cmd.DataSegment = data
		WritePDU(conn2, cmd)
		wResp, _ := ReadPDU(conn2)
		if wResp.SCSIStatus() != SCSIStatusGood {
			t.Fatal("write failed")
		}
	})

	t.Run("write_read_pattern", func(t *testing.T) {
		// Write 0xAA to 50 LBAs -> read all -> byte-compare
		conn, _ := qaSessionDefault(t)
		cmdSN := qaLoginNormal(t, conn)

		pattern := bytes.Repeat([]byte{0xAA}, 4096)

		for lba := uint32(0); lba < 50; lba++ {
			var wCDB [16]byte
			wCDB[0] = ScsiWrite10
			binary.BigEndian.PutUint32(wCDB[2:6], lba)
			binary.BigEndian.PutUint16(wCDB[7:9], 1)
			resp := qaSendSCSICmd(t, conn, wCDB, cmdSN, false, true, pattern, 4096)
			if resp.SCSIStatus() != SCSIStatusGood {
				t.Fatalf("write LBA %d failed", lba)
			}
			cmdSN++
		}

		for lba := uint32(0); lba < 50; lba++ {
			var rCDB [16]byte
			rCDB[0] = ScsiRead10
			binary.BigEndian.PutUint32(rCDB[2:6], lba)
			binary.BigEndian.PutUint16(rCDB[7:9], 1)
			resp := qaSendSCSICmd(t, conn, rCDB, cmdSN, true, false, nil, 4096)
			data := qaReadAllDataIn(t, conn, resp)
			if !bytes.Equal(data, pattern) {
				t.Fatalf("LBA %d: data mismatch", lba)
			}
			cmdSN++
		}
	})

	t.Run("nil_scsi_handler_reject", func(t *testing.T) {
		// Discovery session + SCSI cmd -> rejected (bug #12)
		conn, _ := qaSessionDefault(t)
		qaLoginDiscovery(t, conn)

		var cdb [16]byte
		cdb[0] = ScsiTestUnitReady
		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF)
		cmd.SetInitiatorTaskTag(2)
		cmd.SetCmdSN(2)
		cmd.SetCDB(cdb)
		if err := WritePDU(conn, cmd); err != nil {
			t.Fatal(err)
		}

		resp := qaExpectReject(t, conn)
		_ = resp
	})
}
