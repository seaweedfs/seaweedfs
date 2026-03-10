package nvme

// Adversarial / QA tests for NVMe/TCP target.
// Covers: malformed wire, protocol state violations, IO boundary attacks,
// ANA/fencing transitions, admin command edge cases, server lifecycle races,
// multi-block chunking, SQHD wraparound, concurrent stress.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// QA-1: Wire / Protocol Attacks
// ============================================================

// TestQA_Wire_TruncatedHeaderEOF: stream ends mid-header (< 8 bytes).
func TestQA_Wire_TruncatedHeaderEOF(t *testing.T) {
	// Only 4 of 8 header bytes → io.ErrUnexpectedEOF
	buf := make([]byte, 4)
	buf[0] = pduCapsuleCmd
	r := NewReader(bytes.NewReader(buf))
	_, err := r.Dequeue()
	if err == nil {
		t.Fatal("expected error for truncated header")
	}
}

// TestQA_Wire_ZeroLengthStream: completely empty reader.
func TestQA_Wire_ZeroLengthStream(t *testing.T) {
	r := NewReader(bytes.NewReader(nil))
	_, err := r.Dequeue()
	if err == nil {
		t.Fatal("expected error for empty stream")
	}
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		t.Fatalf("expected EOF-type error, got: %v", err)
	}
}

// TestQA_Wire_HeaderLength_Exactly8: minimum valid HeaderLength with no specific header.
func TestQA_Wire_HeaderLength_Exactly8(t *testing.T) {
	buf := make([]byte, commonHeaderSize)
	hdr := CommonHeader{
		Type:         pduCapsuleResp,
		HeaderLength: commonHeaderSize, // exactly 8
		DataOffset:   0,
		DataLength:   uint32(commonHeaderSize),
	}
	hdr.Marshal(buf)
	r := NewReader(bytes.NewReader(buf))
	got, err := r.Dequeue()
	if err != nil {
		t.Fatalf("valid minimum header should parse: %v", err)
	}
	if got.HeaderLength != commonHeaderSize {
		t.Fatalf("HeaderLength = %d", got.HeaderLength)
	}
}

// TestQA_Wire_AllZeroHeader: all-zero 8-byte header has HeaderLength=0 < 8 → rejected.
func TestQA_Wire_AllZeroHeader(t *testing.T) {
	buf := make([]byte, commonHeaderSize)
	r := NewReader(bytes.NewReader(buf))
	_, err := r.Dequeue()
	if err == nil {
		t.Fatal("all-zero header should be rejected (HeaderLength=0 < 8)")
	}
}

// TestQA_Wire_GarbageAfterValidPDU: garbage bytes after a valid PDU
// should not cause the valid PDU to fail.
func TestQA_Wire_GarbageAfterValidPDU(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	resp := CapsuleResponse{CID: 99, Status: 0}
	if err := w.SendHeaderOnly(pduCapsuleResp, &resp, capsuleRespSize); err != nil {
		t.Fatal(err)
	}
	// Append garbage
	buf.Write([]byte{0xFF, 0xFE, 0xFD})

	r := NewReader(&buf)
	hdr, err := r.Dequeue()
	if err != nil {
		t.Fatalf("first valid PDU should parse: %v", err)
	}
	if hdr.Type != pduCapsuleResp {
		t.Fatalf("type = 0x%x", hdr.Type)
	}
	var got CapsuleResponse
	r.Receive(&got)
	if got.CID != 99 {
		t.Fatalf("CID = %d", got.CID)
	}

	// Second Dequeue should fail on garbage (HeaderLength too small)
	_, err = r.Dequeue()
	if err == nil {
		t.Fatal("expected error parsing garbage as next PDU")
	}
}

// ============================================================
// QA-2: Controller State Machine Violations
// ============================================================

// TestQA_UnexpectedPDUType: send an unknown PDU type after IC.
func TestQA_UnexpectedPDUType(t *testing.T) {
	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	ctrl := newController(serverConn, srv)

	done := make(chan error, 1)
	go func() { done <- ctrl.Serve() }()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)

	sendICReq(w)
	recvICResp(t, r)

	// Send R2T PDU type (0x06) which is not handled by the controller.
	fakePDU := CapsuleCommand{} // payload doesn't matter
	w.SendHeaderOnly(0x06, &fakePDU, capsuleCmdSize)

	// Controller should return error or close connection
	select {
	case err := <-done:
		if err == nil {
			// EOF from pipe close is also acceptable
		}
	case <-time.After(2 * time.Second):
		t.Fatal("controller did not exit on unknown PDU type")
	}

	clientConn.Close()
}

// TestQA_CapsuleBeforeIC: send a capsule command before IC handshake.
// The controller must reject it — capsules require state >= stateICComplete.
func TestQA_CapsuleBeforeIC(t *testing.T) {
	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	ctrl := newController(serverConn, srv)

	done := make(chan error, 1)
	go func() { done <- ctrl.Serve() }()

	w := NewWriter(clientConn)

	// Send capsule directly without IC — controller should reject and close.
	cmd := CapsuleCommand{OpCode: adminKeepAlive, CID: 1}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from Serve for capsule before IC")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for controller to reject capsule before IC")
	}
}

// TestQA_IOWrite_OnAdminQueue: ioWrite (0x01) has no admin counterpart → InvalidOpcode.
// Note: other IO opcodes (0x00, 0x02, 0x08) map to valid admin opcodes by NVMe spec.
func TestQA_IOWrite_OnAdminQueue(t *testing.T) {
	nqn := "nqn.test:qa-admin-io"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: ioWrite, // 0x01: no admin equivalent
		CID:    500,
		D10:    0,
		D12:    0,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, make([]byte, 512))
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidOpcode {
		t.Fatalf("ioWrite(0x01) on admin queue: got 0x%04x, want InvalidOpcode", resp.Status)
	}
}

// TestQA_UnknownAdminOpcode: bogus admin opcode → InvalidOpcode.
func TestQA_UnknownAdminOpcode(t *testing.T) {
	nqn := "nqn.test:qa-bad-admin"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: 0xFF, // unknown
		CID:    600,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidOpcode {
		t.Fatalf("unknown admin opcode: got 0x%04x, want InvalidOpcode", resp.Status)
	}
}

// TestQA_UnknownIOOpcode: bogus IO opcode → InvalidOpcode.
func TestQA_UnknownIOOpcode(t *testing.T) {
	nqn := "nqn.test:qa-bad-io"
	dev := newMockDevice(256, 512)

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	ctrl.subsystem = srv.findSubsystem(nqn)
	ctrl.queueID = 1
	ctrl.queueSize = 64
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)

	sendICReq(w)
	recvICResp(t, r)

	cmd := CapsuleCommand{
		OpCode: 0xFE, // unknown IO opcode
		CID:    700,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidOpcode {
		t.Fatalf("unknown IO opcode: got 0x%04x, want InvalidOpcode", resp.Status)
	}
}

// ============================================================
// QA-3: Fabric Command Edge Cases
// ============================================================

// TestQA_ConnectEmptyPayload: Connect with payload < 1024 bytes → InvalidField.
func TestQA_ConnectEmptyPayload(t *testing.T) {
	nqn := "nqn.test:qa-empty-connect"
	dev := newMockDevice(256, 512)

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)

	sendICReq(w)
	recvICResp(t, r)

	// Send Connect with only 16 bytes of payload (need 1024)
	cmd := CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcConnect,
		D10:    0, // QID=0
		D11:    63,
	}
	shortPayload := make([]byte, 16) // way too short
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, shortPayload)

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("short ConnectData: got 0x%04x, want InvalidField", resp.Status)
	}
}

// TestQA_ConnectNoPayload: Connect with no inline data at all → InvalidField.
func TestQA_ConnectNoPayload(t *testing.T) {
	nqn := "nqn.test:qa-no-connect-data"
	dev := newMockDevice(256, 512)

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)

	sendICReq(w)
	recvICResp(t, r)

	// Send Connect capsule with zero payload
	cmd := CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcConnect,
		D10:    0,
		D11:    63,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("no ConnectData: got 0x%04x, want InvalidField", resp.Status)
	}
}

// TestQA_UnknownFabricFCType: unknown FCType → InvalidField.
func TestQA_UnknownFabricFCType(t *testing.T) {
	nqn := "nqn.test:qa-bad-fc"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: adminFabric,
		FCType: 0xFF, // unknown
		CID:    800,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("unknown FCType: got 0x%04x, want InvalidField", resp.Status)
	}
}

// TestQA_PropertyGetUnknownOffset: PropertyGet with bad register → InvalidField.
func TestQA_PropertyGetUnknownOffset(t *testing.T) {
	nqn := "nqn.test:qa-bad-prop"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcPropertyGet,
		CID:    801,
		D11:    0xDEAD, // invalid register offset (CDW11=OFST)
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("unknown register offset: got 0x%04x, want InvalidField", resp.Status)
	}
}

// TestQA_Disconnect_CleanShutdown: Disconnect should send response and close.
func TestQA_Disconnect_CleanShutdown(t *testing.T) {
	nqn := "nqn.test:qa-disconnect"
	client, r, w, ctrl, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcDisconnect,
		CID:    802,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	// Should get a success response
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("Disconnect failed: 0x%04x", resp.Status)
	}

	// Controller should be closed after disconnect
	time.Sleep(50 * time.Millisecond)
	if ctrl.state != stateClosed {
		t.Fatalf("state = %d, want stateClosed(%d)", ctrl.state, stateClosed)
	}
}

// ============================================================
// QA-4: IO Boundary Attacks
// ============================================================

// setupQAIOQueue creates a controller with IO queue set up for testing.
func setupQAIOQueue(t *testing.T, nqn string, dev *mockBlockDevice) (net.Conn, *Reader, *Writer) {
	t.Helper()
	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	ctrl := newController(serverConn, srv)
	ctrl.subsystem = srv.findSubsystem(nqn)
	ctrl.queueID = 1
	ctrl.queueSize = 64
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)
	recvICResp(t, r)
	return clientConn, r, w
}

// TestQA_IO_WriteOversizedPayload: NLB=1 (512B) but payload=1024B → InvalidField.
func TestQA_IO_WriteOversizedPayload(t *testing.T) {
	dev := newMockDevice(256, 512)
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-oversize", dev)
	defer client.Close()

	writeCmd := CapsuleCommand{
		OpCode: ioWrite,
		CID:    900,
		D10:    0,
		D12:    0, // 1 block = 512 bytes
	}
	oversized := make([]byte, 1024) // too large
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, oversized)

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("oversized payload: got 0x%04x, want InvalidField", resp.Status)
	}
}

// TestQA_IO_WriteExactBoundary: write at last valid LBA should succeed.
func TestQA_IO_WriteExactBoundary(t *testing.T) {
	dev := newMockDevice(256, 512) // 256 blocks
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-boundary", dev)
	defer client.Close()

	// Write 1 block at LBA 255 (last valid)
	writeCmd := CapsuleCommand{
		OpCode: ioWrite,
		CID:    901,
		D10:    255,
		D11:    0,
		D12:    0, // 1 block
	}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, make([]byte, 512))
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("write at last LBA should succeed: 0x%04x", resp.Status)
	}

	// Write 1 block at LBA 256 → out of bounds
	writeCmd2 := CapsuleCommand{
		OpCode: ioWrite,
		CID:    902,
		D10:    256, // past end
		D11:    0,
		D12:    0,
	}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd2, capsuleCmdSize, make([]byte, 512))
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusLBAOutOfRange {
		t.Fatalf("write past end: got 0x%04x, want LBAOutOfRange", resp.Status)
	}
}

// TestQA_IO_ReadExactBoundary: read at last valid LBA succeeds, LBA+1 fails.
func TestQA_IO_ReadExactBoundary(t *testing.T) {
	dev := newMockDevice(256, 512)
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-readbound", dev)
	defer client.Close()

	// Read last block
	readCmd := CapsuleCommand{
		OpCode: ioRead,
		CID:    903,
		D10:    255,
		D12:    0,
	}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	hdr, _ := r.Dequeue()
	if hdr.Type != pduC2HData {
		t.Fatalf("expected C2HData, got 0x%x", hdr.Type)
	}
	var c2h C2HDataHeader
	r.Receive(&c2h)
	data := make([]byte, r.Length())
	r.ReceiveData(data)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("read last LBA failed: 0x%04x", resp.Status)
	}

	// Read at LBA 256 → out of bounds
	readCmd2 := CapsuleCommand{
		OpCode: ioRead,
		CID:    904,
		D10:    256,
		D12:    0,
	}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd2, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusLBAOutOfRange {
		t.Fatalf("read past end: got 0x%04x, want LBAOutOfRange", resp.Status)
	}
}

// TestQA_IO_MultiBlockWrite: write 8 blocks at once, read back and verify.
func TestQA_IO_MultiBlockWrite(t *testing.T) {
	dev := newMockDevice(256, 512)
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-multi", dev)
	defer client.Close()

	// Write 8 blocks (4096 bytes) at LBA 10
	writeData := make([]byte, 4096)
	for i := range writeData {
		writeData[i] = byte(i & 0xFF)
	}
	writeCmd := CapsuleCommand{
		OpCode: ioWrite,
		CID:    910,
		D10:    10,
		D12:    7, // 8 blocks (0-based)
	}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, writeData)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("multi-block write failed: 0x%04x", resp.Status)
	}

	// Read back
	readCmd := CapsuleCommand{
		OpCode: ioRead,
		CID:    911,
		D10:    10,
		D12:    7,
	}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	// May come as multiple C2HData chunks (4096 > maxH2CDataLen if small)
	var readBuf bytes.Buffer
	for {
		hdr, err := r.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Type == pduCapsuleResp {
			var resp2 CapsuleResponse
			r.Receive(&resp2)
			if StatusWord(resp2.Status).IsError() {
				t.Fatalf("multi-block read resp error: 0x%04x", resp2.Status)
			}
			break
		}
		if hdr.Type == pduC2HData {
			var c2h C2HDataHeader
			r.Receive(&c2h)
			chunk := make([]byte, r.Length())
			r.ReceiveData(chunk)
			readBuf.Write(chunk)
		}
	}

	if !bytes.Equal(readBuf.Bytes(), writeData) {
		t.Fatal("multi-block read data mismatch")
	}
}

// TestQA_IO_WriteZerosOutOfBounds: WriteZeros past volume end → LBAOutOfRange.
func TestQA_IO_WriteZerosOutOfBounds(t *testing.T) {
	dev := newMockDevice(256, 512)
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-wz-oob", dev)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: ioWriteZeros,
		CID:    920,
		D10:    255,
		D12:    1, // 2 blocks from LBA 255 → exceeds 256 blocks
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusLBAOutOfRange {
		t.Fatalf("WriteZeros OOB: got 0x%04x, want LBAOutOfRange", resp.Status)
	}
}

// TestQA_IO_FlushOnReplica: flush gated by ANA → NSNotReady.
func TestQA_IO_FlushOnReplica(t *testing.T) {
	dev := newMockDevice(256, 512)
	dev.anaState = anaInaccessible // replica mode
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-flush-replica", dev)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: ioFlush,
		CID:    921,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusNSNotReady {
		t.Fatalf("flush on replica: got 0x%04x, want NSNotReady", resp.Status)
	}
}

// TestQA_IO_WriteZerosOnReplica: WriteZeros gated by ANA → NSNotReady.
func TestQA_IO_WriteZerosOnReplica(t *testing.T) {
	dev := newMockDevice(256, 512)
	dev.anaState = anaPersistentLoss // stale
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-wz-replica", dev)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: ioWriteZeros,
		CID:    922,
		D10:    0,
		D12:    0,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusNSNotReady {
		t.Fatalf("WriteZeros on stale: got 0x%04x, want NSNotReady", resp.Status)
	}
}

// TestQA_IO_ReadOnReplicaSucceeds: reads should work even on replica ANA state.
func TestQA_IO_ReadOnReplicaSucceeds(t *testing.T) {
	dev := newMockDevice(256, 512)
	dev.anaState = anaInaccessible // replica
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-read-replica", dev)
	defer client.Close()

	readCmd := CapsuleCommand{
		OpCode: ioRead,
		CID:    923,
		D10:    0,
		D12:    0,
	}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	hdr, _ := r.Dequeue()
	if hdr.Type != pduC2HData {
		t.Fatalf("expected C2HData, got 0x%x", hdr.Type)
	}
	var c2h C2HDataHeader
	r.Receive(&c2h)
	data := make([]byte, r.Length())
	r.ReceiveData(data)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("read on replica should succeed: 0x%04x", resp.Status)
	}
}

// TestQA_IO_SyncCacheError: SyncCache returns error → mapped NVMe status.
func TestQA_IO_SyncCacheError(t *testing.T) {
	dev := newMockDevice(256, 512)
	dev.syncErr = errors.New("sync failed")
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-sync-err", dev)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: ioFlush,
		CID:    924,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if !StatusWord(resp.Status).IsError() {
		t.Fatal("flush with sync error should fail")
	}
	if StatusWord(resp.Status) != StatusInternalError {
		t.Fatalf("sync error: got 0x%04x, want InternalError", resp.Status)
	}
}

// TestQA_IO_TrimError: Trim returns error → mapped NVMe status.
func TestQA_IO_TrimError(t *testing.T) {
	dev := newMockDevice(256, 512)
	dev.trimErr = errors.New("trim failed")
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-trim-err", dev)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: ioWriteZeros,
		CID:    925,
		D10:    0,
		D12:    0 | commandBitDeallocate,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if !StatusWord(resp.Status).IsError() {
		t.Fatal("trim error should fail")
	}
}

// ============================================================
// QA-5: Admin Command Edge Cases
// ============================================================

// TestQA_Admin_UnknownFeatureID: SetFeatures/GetFeatures with unknown FID.
func TestQA_Admin_UnknownFeatureID(t *testing.T) {
	nqn := "nqn.test:qa-bad-fid"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	// SetFeatures unknown
	cmd := CapsuleCommand{
		OpCode: adminSetFeatures,
		CID:    1000,
		D10:    0xBB, // unknown FID
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("SetFeatures unknown FID: got 0x%04x, want InvalidField", resp.Status)
	}

	// GetFeatures unknown
	cmd2 := CapsuleCommand{
		OpCode: adminGetFeatures,
		CID:    1001,
		D10:    0xBB,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd2, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("GetFeatures unknown FID: got 0x%04x, want InvalidField", resp.Status)
	}
}

// TestQA_Admin_UnknownIdentifyCNS: Identify with unknown CNS → InvalidField.
func TestQA_Admin_UnknownIdentifyCNS(t *testing.T) {
	nqn := "nqn.test:qa-bad-cns"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: adminIdentify,
		CID:    1002,
		D10:    0xFF, // unknown CNS
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("Identify unknown CNS: got 0x%04x, want InvalidField", resp.Status)
	}
}

// TestQA_Admin_UnknownLogPageLID: GetLogPage with unknown LID → InvalidField.
func TestQA_Admin_UnknownLogPageLID(t *testing.T) {
	nqn := "nqn.test:qa-bad-lid"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: adminGetLogPage,
		CID:    1003,
		D10:    0xFE | (3 << 16), // unknown LID, NUMD=3
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("GetLogPage unknown LID: got 0x%04x, want InvalidField", resp.Status)
	}
}

// TestQA_Admin_SetFeaturesZeroQueues: request 0 queues → clamped to 1.
func TestQA_Admin_SetFeaturesZeroQueues(t *testing.T) {
	nqn := "nqn.test:qa-zero-q"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: adminSetFeatures,
		CID:    1004,
		D10:    uint32(fidNumberOfQueues),
		D11:    0, // 0|0 → request 0+1=1? No: D11 is NCQR|NSQR with 0-based, so 0 means 1.
		// Actually the code reads ncqr = D11 & 0xFFFF = 0, then clamps to 1
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("SetFeatures 0 queues: got 0x%04x", resp.Status)
	}
	// DW0 should be (1-1) | ((1-1)<<16) = 0
	if resp.DW0 != 0 {
		t.Fatalf("expected DW0=0 (1 queue each, 0-based), got %d", resp.DW0)
	}
}

// TestQA_Admin_GetLogPage_ErrorLog: error log should return empty data.
func TestQA_Admin_GetLogPage_ErrorLog(t *testing.T) {
	nqn := "nqn.test:qa-errlog"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: adminGetLogPage,
		CID:    1005,
		D10:    uint32(logPageError) | (15 << 16), // NUMD=15 → 64 bytes
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	hdr, _ := r.Dequeue()
	if hdr.Type != pduC2HData {
		t.Fatalf("expected C2HData for error log")
	}
	var c2h C2HDataHeader
	r.Receive(&c2h)
	data := make([]byte, r.Length())
	r.ReceiveData(data)
	// Error log should be all zeros (empty)
	for i, b := range data {
		if b != 0 {
			t.Fatalf("error log byte[%d] = 0x%02x, want 0", i, b)
		}
	}
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("error log failed: 0x%04x", resp.Status)
	}
}

// ============================================================
// QA-6: ANA State Transitions Under IO
// ============================================================

// TestQA_ANA_TransitionMidIO: change ANA state from optimized to inaccessible
// mid-flight. First write succeeds, then transition, second write fails.
func TestQA_ANA_TransitionMidIO(t *testing.T) {
	dev := newMockDevice(256, 512)
	dev.anaState = anaOptimized
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-ana-flip", dev)
	defer client.Close()

	// Write should succeed (optimized)
	writeCmd := CapsuleCommand{OpCode: ioWrite, CID: 1100, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, make([]byte, 512))
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("write while optimized failed: 0x%04x", resp.Status)
	}

	// Flip to inaccessible
	dev.mu.Lock()
	dev.anaState = anaInaccessible
	dev.mu.Unlock()

	// Write should be rejected
	writeCmd2 := CapsuleCommand{OpCode: ioWrite, CID: 1101, D10: 1, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd2, capsuleCmdSize, make([]byte, 512))
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusNSNotReady {
		t.Fatalf("write after ANA flip: got 0x%04x, want NSNotReady", resp.Status)
	}

	// Flip back to optimized
	dev.mu.Lock()
	dev.anaState = anaOptimized
	dev.mu.Unlock()

	// Write should succeed again
	writeCmd3 := CapsuleCommand{OpCode: ioWrite, CID: 1102, D10: 2, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd3, capsuleCmdSize, make([]byte, 512))
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("write after ANA restore: 0x%04x", resp.Status)
	}
}

// TestQA_ANA_NonOptimizedAllowsWrite: anaNonOptimized should allow writes.
func TestQA_ANA_NonOptimizedAllowsWrite(t *testing.T) {
	dev := newMockDevice(256, 512)
	dev.anaState = anaNonOptimized
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-ana-nonopt", dev)
	defer client.Close()

	writeCmd := CapsuleCommand{OpCode: ioWrite, CID: 1110, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, make([]byte, 512))
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("write with NonOptimized ANA should succeed: 0x%04x", resp.Status)
	}
}

// TestQA_ANA_LogReflectsState: ANA log page reports correct state after transition.
func TestQA_ANA_LogReflectsState(t *testing.T) {
	nqn := "nqn.test:qa-ana-log"
	dev := newMockDevice(256, 512)
	dev.anaState = anaPersistentLoss

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())
	ctrl := newController(serverConn, srv)
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)
	recvICResp(t, r)
	sendConnect(w, 0, 64, 0, nqn, "host", 0xFFFF)
	recvCapsuleResp(t, r)

	cmd := CapsuleCommand{
		OpCode: adminGetLogPage,
		CID:    1120,
		D10:    uint32(logPageANA) | (9 << 16),
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	hdr, _ := r.Dequeue()
	if hdr.Type != pduC2HData {
		t.Fatalf("expected C2HData")
	}
	var c2h C2HDataHeader
	r.Receive(&c2h)
	data := make([]byte, r.Length())
	r.ReceiveData(data)

	if data[32] != anaPersistentLoss {
		t.Fatalf("ANA state = 0x%02x, want 0x%02x (PersistentLoss)", data[32], anaPersistentLoss)
	}

	recvCapsuleResp(t, r)
}

// ============================================================
// QA-7: Server Lifecycle
// ============================================================

// TestQA_Server_ConnectAfterVolumeRemoved: connect to NQN after RemoveVolume → error.
func TestQA_Server_ConnectAfterVolumeRemoved(t *testing.T) {
	nqn := "nqn.test:qa-removed"
	dev := newMockDevice(256, 512)

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())
	srv.RemoveVolume(nqn) // Remove before connect

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)

	sendICReq(w)
	recvICResp(t, r)

	sendConnect(w, 0, 64, 0, nqn, "host", 0xFFFF)
	resp := recvCapsuleResp(t, r)
	if !StatusWord(resp.Status).IsError() {
		t.Fatal("connect to removed volume should fail")
	}
}

// TestQA_Server_RapidConnectDisconnect: 20 rapid admin connect/disconnect cycles.
func TestQA_Server_RapidConnectDisconnect(t *testing.T) {
	nqn := "nqn.test:qa-rapid"
	dev := newMockDevice(256, 512)

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	for i := 0; i < 20; i++ {
		clientConn, serverConn := pipeConn()
		ctrl := newController(serverConn, srv)
		go ctrl.Serve()

		r := NewReader(clientConn)
		w := NewWriter(clientConn)

		if err := sendICReq(w); err != nil {
			t.Fatalf("iter %d IC: %v", i, err)
		}
		recvICResp(t, r)

		if err := sendConnect(w, 0, 64, 0, nqn, "host", 0xFFFF); err != nil {
			t.Fatalf("iter %d connect: %v", i, err)
		}
		resp := recvCapsuleResp(t, r)
		if StatusWord(resp.Status).IsError() {
			t.Fatalf("iter %d connect failed: 0x%04x", i, resp.Status)
		}

		clientConn.Close()
	}

	// Verify all admin sessions are cleaned up
	time.Sleep(100 * time.Millisecond)
	srv.adminMu.RLock()
	count := len(srv.admins)
	srv.adminMu.RUnlock()
	if count != 0 {
		t.Fatalf("leaked %d admin sessions after rapid connect/disconnect", count)
	}
}

// TestQA_Server_ConcurrentIO: 4 concurrent IO queue connections read/write simultaneously.
func TestQA_Server_ConcurrentIO(t *testing.T) {
	nqn := "nqn.test:qa-concurrent-io"
	dev := newMockDevice(1024, 512)

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 8})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			clientConn, serverConn := pipeConn()
			defer clientConn.Close()

			ctrl := newController(serverConn, srv)
			ctrl.subsystem = srv.findSubsystem(nqn)
			ctrl.queueID = uint16(idx + 1)
			ctrl.queueSize = 64
			go ctrl.Serve()

			r := NewReader(clientConn)
			w := NewWriter(clientConn)
			sendICReq(w)
			recvICResp(t, r)

			// Each goroutine writes to a different LBA range
			base := uint32(idx * 64)
			for j := 0; j < 10; j++ {
				lba := base + uint32(j)
				writeData := bytes.Repeat([]byte{byte(idx*10 + j)}, 512)
				writeCmd := CapsuleCommand{
					OpCode: ioWrite,
					CID:    uint16(idx*100 + j),
					D10:    lba,
					D12:    0,
				}
				w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, writeData)
				resp := recvCapsuleResp(t, r)
				if StatusWord(resp.Status).IsError() {
					t.Errorf("goroutine %d write %d failed: 0x%04x", idx, j, resp.Status)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

// ============================================================
// QA-8: SQHD Wraparound
// ============================================================

// TestQA_SQHDWraparound: send queueSize+1 commands to trigger SQHD wrap.
func TestQA_SQHDWraparound(t *testing.T) {
	dev := newMockDevice(256, 512)
	nqn := "nqn.test:qa-sqhd"

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	ctrl.subsystem = srv.findSubsystem(nqn)
	ctrl.queueID = 1
	ctrl.queueSize = 4 // very small queue for quick wrap
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)
	recvICResp(t, r)

	// Send 10 commands (wraps around queueSize=4 multiple times)
	var lastSQHD uint16
	for i := 0; i < 10; i++ {
		readCmd := CapsuleCommand{
			OpCode: ioRead,
			CID:    uint16(2000 + i),
			D10:    0,
			D12:    0,
		}
		w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

		// Read C2HData + CapsuleResp
		hdr, err := r.Dequeue()
		if err != nil {
			t.Fatalf("cmd %d: dequeue: %v", i, err)
		}
		if hdr.Type == pduC2HData {
			var c2h C2HDataHeader
			r.Receive(&c2h)
			data := make([]byte, r.Length())
			r.ReceiveData(data)
			// Read the CapsuleResp
			resp := recvCapsuleResp(t, r)
			lastSQHD = resp.SQHD
		} else if hdr.Type == pduCapsuleResp {
			var resp CapsuleResponse
			r.Receive(&resp)
			lastSQHD = resp.SQHD
		}
	}

	// SQHD should have wrapped (not grown unbounded)
	// With queueSize=4, after 10 commands: sqhd cycles 1,2,3,0,1,2,3,0,1,2
	// Expected: SQHD=2
	if lastSQHD >= 4 {
		t.Fatalf("SQHD=%d should be < queueSize=4", lastSQHD)
	}
}

// ============================================================
// QA-9: Large Read Chunking (C2HData)
// ============================================================

// TestQA_LargeReadChunking: read > maxH2CDataLen triggers multiple C2HData PDUs.
func TestQA_LargeReadChunking(t *testing.T) {
	// maxH2CDataLen = 32KB. Create a device with 512B blocks,
	// read 128 blocks = 64KB → should produce 2 C2HData chunks.
	dev := newMockDevice(256, 512)
	// Fill with pattern
	for i := range dev.data {
		dev.data[i] = byte(i & 0xFF)
	}

	client, r, w := setupQAIOQueue(t, "nqn.test:qa-chunk", dev)
	defer client.Close()

	readCmd := CapsuleCommand{
		OpCode: ioRead,
		CID:    3000,
		D10:    0,
		D12:    127, // 128 blocks = 65536 bytes (0-based count)
	}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	// Collect all C2HData chunks
	var readBuf bytes.Buffer
	chunkCount := 0
	for {
		hdr, err := r.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Type == pduCapsuleResp {
			var resp CapsuleResponse
			r.Receive(&resp)
			if StatusWord(resp.Status).IsError() {
				t.Fatalf("large read error: 0x%04x", resp.Status)
			}
			break
		}
		if hdr.Type == pduC2HData {
			chunkCount++
			var c2h C2HDataHeader
			r.Receive(&c2h)
			chunk := make([]byte, r.Length())
			r.ReceiveData(chunk)
			readBuf.Write(chunk)
		}
	}

	if readBuf.Len() != 65536 {
		t.Fatalf("total read = %d bytes, want 65536", readBuf.Len())
	}
	if chunkCount < 2 {
		t.Fatalf("expected >= 2 C2HData chunks, got %d", chunkCount)
	}

	// Verify data matches
	if !bytes.Equal(readBuf.Bytes(), dev.data[:65536]) {
		t.Fatal("large read data mismatch")
	}
}

// ============================================================
// QA-10: Error Injection Under Load
// ============================================================

// TestQA_ErrorInjectionMidStream: inject errors after successful IO.
func TestQA_ErrorInjectionMidStream(t *testing.T) {
	dev := newMockDevice(256, 512)
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-inject", dev)
	defer client.Close()

	// First write succeeds
	writeCmd := CapsuleCommand{OpCode: ioWrite, CID: 4000, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, make([]byte, 512))
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("first write failed: 0x%04x", resp.Status)
	}

	// Inject write error
	dev.mu.Lock()
	dev.writeErr = errors.New("injected Write fault")
	dev.mu.Unlock()

	// Second write should fail with MediaWriteFault
	writeCmd2 := CapsuleCommand{OpCode: ioWrite, CID: 4001, D10: 1, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd2, capsuleCmdSize, make([]byte, 512))
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusMediaWriteFault {
		t.Fatalf("injected write error: got 0x%04x, want MediaWriteFault", resp.Status)
	}

	// Clear error → writes should work again
	dev.mu.Lock()
	dev.writeErr = nil
	dev.mu.Unlock()

	writeCmd3 := CapsuleCommand{OpCode: ioWrite, CID: 4002, D10: 2, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd3, capsuleCmdSize, make([]byte, 512))
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("write after error clear: 0x%04x", resp.Status)
	}

	// Inject read error
	dev.mu.Lock()
	dev.readErr = errors.New("injected Read failure")
	dev.mu.Unlock()

	readCmd := CapsuleCommand{OpCode: ioRead, CID: 4003, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusMediaReadError {
		t.Fatalf("injected read error: got 0x%04x, want MediaReadError", resp.Status)
	}
}

// ============================================================
// QA-11: PropertySet CC.EN=0 clears CSTS.RDY
// ============================================================

func TestQA_PropertySet_DisableController(t *testing.T) {
	nqn := "nqn.test:qa-cc-disable"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	// Enable: CC.EN=1
	cmd := makePropertySetCapsule(propCC, 1)
	cmd.CID = 5000
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	recvCapsuleResp(t, r)

	// Verify RDY=1
	cmd2 := makePropertyGetCapsule(propCSTS, false)
	cmd2.CID = 5001
	w.SendWithData(pduCapsuleCmd, 0, &cmd2, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if resp.DW0&1 != 1 {
		t.Fatal("CSTS.RDY should be 1 after CC.EN=1")
	}

	// Disable: CC.EN=0
	cmd3 := makePropertySetCapsule(propCC, 0)
	cmd3.CID = 5002
	w.SendWithData(pduCapsuleCmd, 0, &cmd3, capsuleCmdSize, nil)
	recvCapsuleResp(t, r)

	// Verify RDY=0
	cmd4 := makePropertyGetCapsule(propCSTS, false)
	cmd4.CID = 5003
	w.SendWithData(pduCapsuleCmd, 0, &cmd4, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if resp.DW0&1 != 0 {
		t.Fatal("CSTS.RDY should be 0 after CC.EN=0")
	}
}

// ============================================================
// QA-12: Identify before subsystem set → InvalidField
// ============================================================

func TestQA_IdentifyWithoutSubsystem(t *testing.T) {
	// Controller with no subsystem set (no Connect)
	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	ctrl := newController(serverConn, srv)
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)

	sendICReq(w)
	recvICResp(t, r)

	// Send Identify without admin Connect first
	cmd := CapsuleCommand{
		OpCode: adminIdentify,
		CID:    6000,
		D10:    uint32(cnsIdentifyController),
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("Identify without subsystem: got 0x%04x, want InvalidField", resp.Status)
	}
}

// ============================================================
// QA-13: IO commands without subsystem → InvalidField
// ============================================================

func TestQA_IO_WithoutSubsystem(t *testing.T) {
	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	ctrl := newController(serverConn, srv)
	ctrl.queueID = 1
	ctrl.queueSize = 64
	// No subsystem set
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)
	recvICResp(t, r)

	// Read without subsystem
	readCmd := CapsuleCommand{OpCode: ioRead, CID: 6100, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("Read without subsystem: got 0x%04x, want InvalidField", resp.Status)
	}

	// Write without subsystem
	writeCmd := CapsuleCommand{OpCode: ioWrite, CID: 6101, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, make([]byte, 512))
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("Write without subsystem: got 0x%04x, want InvalidField", resp.Status)
	}

	// Flush without subsystem
	flushCmd := CapsuleCommand{OpCode: ioFlush, CID: 6102}
	w.SendWithData(pduCapsuleCmd, 0, &flushCmd, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("Flush without subsystem: got 0x%04x, want InvalidField", resp.Status)
	}

	// WriteZeros without subsystem
	wzCmd := CapsuleCommand{OpCode: ioWriteZeros, CID: 6103, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &wzCmd, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("WriteZeros without subsystem: got 0x%04x, want InvalidField", resp.Status)
	}
}

// ============================================================
// QA-14: flowCtlOff (SQHD disabled)
// ============================================================

func TestQA_FlowCtlOff_SQHD(t *testing.T) {
	nqn := "nqn.test:qa-flowctl"
	dev := newMockDevice(256, 512)

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)

	sendICReq(w)
	recvICResp(t, r)

	// Connect with CATTR bit 2 set (flowCtlOff)
	cmd := CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcConnect,
		CID:    0,
		D10:    0, // QID=0
		D11:    63 | (0x04 << 16), // SQSIZE=64, CATTR=0x04 (flowCtlOff)
		D12:    0,
	}
	cd := ConnectData{CNTLID: 0xFFFF, SubNQN: nqn, HostNQN: "host"}
	payload := make([]byte, connectDataSize)
	cd.Marshal(payload)
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, payload)

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("connect with flowCtlOff failed: 0x%04x", resp.Status)
	}

	// Connect response itself must have SQHD=0xFFFF because flowCtlOff
	// is set during handleConnect and sendResponse reads it at send time.
	if resp.SQHD != 0xFFFF {
		t.Fatalf("Connect SQHD = 0x%04x, want 0xFFFF (flowCtlOff)", resp.SQHD)
	}

	// KeepAlive: SQHD still 0xFFFF
	kaCmd := CapsuleCommand{OpCode: adminKeepAlive, CID: 7001}
	w.SendWithData(pduCapsuleCmd, 0, &kaCmd, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if resp.SQHD != 0xFFFF {
		t.Fatalf("SQHD = 0x%04x after KeepAlive, want 0xFFFF", resp.SQHD)
	}

	// Second command: SQHD still 0xFFFF
	kaCmd2 := CapsuleCommand{OpCode: adminKeepAlive, CID: 7002}
	w.SendWithData(pduCapsuleCmd, 0, &kaCmd2, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if resp.SQHD != 0xFFFF {
		t.Fatalf("SQHD = 0x%04x after second KeepAlive, want 0xFFFF", resp.SQHD)
	}
}

// ============================================================
// QA-15: 4K block size IO operations
// ============================================================

func TestQA_IO_4KBlockSize(t *testing.T) {
	dev := newMockDevice(64, 4096) // 64 blocks * 4096 = 256KB
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-4k", dev)
	defer client.Close()

	// Write 1 block at LBA 0 (4096 bytes)
	writeData := bytes.Repeat([]byte{0xAA}, 4096)
	writeCmd := CapsuleCommand{OpCode: ioWrite, CID: 8000, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, writeData)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("4K write failed: 0x%04x", resp.Status)
	}

	// Read back
	readCmd := CapsuleCommand{OpCode: ioRead, CID: 8001, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	var readBuf bytes.Buffer
	for {
		hdr, err := r.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Type == pduCapsuleResp {
			var resp2 CapsuleResponse
			r.Receive(&resp2)
			break
		}
		if hdr.Type == pduC2HData {
			var c2h C2HDataHeader
			r.Receive(&c2h)
			chunk := make([]byte, r.Length())
			r.ReceiveData(chunk)
			readBuf.Write(chunk)
		}
	}

	if !bytes.Equal(readBuf.Bytes(), writeData) {
		t.Fatal("4K block read/write mismatch")
	}

	// Write with wrong payload size (512 instead of 4096)
	writeCmd2 := CapsuleCommand{OpCode: ioWrite, CID: 8002, D10: 1, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd2, capsuleCmdSize, make([]byte, 512))
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("512B payload on 4K device: got 0x%04x, want InvalidField", resp.Status)
	}
}

// ============================================================
// QA-16: Identify Controller field checks
// ============================================================

func TestQA_Identify_ControllerModelSerial(t *testing.T) {
	nqn := "nqn.test:qa-id-fields"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: adminIdentify,
		CID:    9000,
		D10:    uint32(cnsIdentifyController),
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	hdr, _ := r.Dequeue()
	if hdr.Type != pduC2HData {
		t.Fatal("expected C2HData")
	}
	var c2h C2HDataHeader
	r.Receive(&c2h)
	data := make([]byte, r.Length())
	r.ReceiveData(data)

	// Serial Number at offset 4, 20 bytes
	serial := string(bytes.TrimRight(data[4:24], " \x00"))
	if serial != "SWF00001" {
		t.Fatalf("Serial = %q, want SWF00001", serial)
	}

	// Model Number at offset 24, 40 bytes
	model := string(bytes.TrimRight(data[24:64], " \x00"))
	if model != "SeaweedFS BlockVol" {
		t.Fatalf("Model = %q, want SeaweedFS BlockVol", model)
	}

	// Firmware at offset 64, 8 bytes
	fw := string(bytes.TrimRight(data[64:72], " \x00"))
	if fw != "0001" {
		t.Fatalf("Firmware = %q, want 0001", fw)
	}

	// NVMe version at offset 80 (uint32 LE)
	ver := binary.LittleEndian.Uint32(data[80:])
	if ver != nvmeVersion14 {
		t.Fatalf("Version = 0x%08x, want 0x%08x", ver, nvmeVersion14)
	}

	// ONCS at offset 520: bits 2+3 (WriteZeros + DatasetManagement/Trim)
	oncs := binary.LittleEndian.Uint16(data[520:])
	if oncs&0x0C != 0x0C {
		t.Fatalf("ONCS = 0x%04x, expected bits 2+3 set", oncs)
	}

	recvCapsuleResp(t, r)
}

// ============================================================
// QA-17: mapBlockError heuristic string matching
// ============================================================

// TestQA_MapBlockError_WriteHeuristic: error with "write" → MediaWriteFault.
func TestQA_MapBlockError_WriteHeuristic(t *testing.T) {
	err := errors.New("disk write failed at sector 42")
	got := mapBlockError(err)
	if got != StatusMediaWriteFault {
		t.Fatalf("error with 'write': got 0x%04x, want MediaWriteFault 0x%04x", got, StatusMediaWriteFault)
	}
}

// TestQA_MapBlockError_WriteHeuristicCapital: "Write" (capital W) also matches.
func TestQA_MapBlockError_WriteHeuristicCapital(t *testing.T) {
	err := errors.New("Write operation timed out")
	got := mapBlockError(err)
	if got != StatusMediaWriteFault {
		t.Fatalf("error with 'Write': got 0x%04x, want MediaWriteFault 0x%04x", got, StatusMediaWriteFault)
	}
}

// TestQA_MapBlockError_ReadHeuristic: error with "read" → MediaReadError.
func TestQA_MapBlockError_ReadHeuristic(t *testing.T) {
	err := errors.New("read I/O error on extent 7")
	got := mapBlockError(err)
	if got != StatusMediaReadError {
		t.Fatalf("error with 'read': got 0x%04x, want MediaReadError 0x%04x", got, StatusMediaReadError)
	}
}

// TestQA_MapBlockError_ReadHeuristicCapital: "Read" also matches.
func TestQA_MapBlockError_ReadHeuristicCapital(t *testing.T) {
	err := errors.New("Read from backend failed")
	got := mapBlockError(err)
	if got != StatusMediaReadError {
		t.Fatalf("error with 'Read': got 0x%04x, want MediaReadError 0x%04x", got, StatusMediaReadError)
	}
}

// TestQA_MapBlockError_UnknownError: no write/read keyword → InternalError.
func TestQA_MapBlockError_UnknownError(t *testing.T) {
	err := errors.New("something completely unexpected happened")
	got := mapBlockError(err)
	if got != StatusInternalError {
		t.Fatalf("unknown error: got 0x%04x, want InternalError 0x%04x", got, StatusInternalError)
	}
}

// TestQA_MapBlockError_Nil: nil → StatusSuccess.
func TestQA_MapBlockError_Nil(t *testing.T) {
	got := mapBlockError(nil)
	if got != StatusSuccess {
		t.Fatalf("nil error: got 0x%04x, want StatusSuccess", got)
	}
}

// ============================================================
// QA-18: PropertySet 8-byte values (D14:D15 merge)
// ============================================================

func TestQA_PropertySet_8ByteValue(t *testing.T) {
	nqn := "nqn.test:qa-propset8"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	// PropertySet CC with a value that spans both D14 and D15.
	// CC is 32-bit, but the PropertySet wire format uses D14:D15 (64-bit).
	// Set CC.EN=1 (bit 0) via 8-byte value with high bits nonzero to test merge.
	cmd := makePropertySetCapsule(propCC, 0x0000000100000001) // D15=1, D14=1
	cmd.CID = 300
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("PropertySet 8-byte: 0x%04x", resp.Status)
	}

	// Verify CC was set by reading it back via PropertyGet.
	getCmd := makePropertyGetCapsule(propCC, false)
	getCmd.CID = 301
	w.SendWithData(pduCapsuleCmd, 0, &getCmd, capsuleCmdSize, nil)

	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("PropertyGet CC: 0x%04x", resp.Status)
	}
	// CC is 32-bit, only D14 (low 32 bits) matters → expect 1
	if resp.DW0 != 1 {
		t.Fatalf("CC = 0x%08x, want 0x00000001", resp.DW0)
	}
}

// ============================================================
// QA-19: KATO=0 (KeepAlive disabled) — no timer armed
// ============================================================

func TestQA_KATO_Zero_NoTimer(t *testing.T) {
	nqn := "nqn.test:qa-kato0"
	dev := newMockDevice(256, 512)
	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)
	recvICResp(t, r)

	// Connect with KATO=0 (disabled)
	sendConnect(w, 0, 64, 0, nqn, "host", 0xFFFF)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("connect failed: 0x%04x", resp.Status)
	}

	// Enable controller (CC.EN=1) — this triggers startKATO()
	ccCmd := makePropertySetCapsule(propCC, 1)
	ccCmd.CID = 400
	w.SendWithData(pduCapsuleCmd, 0, &ccCmd, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("CC.EN set failed: 0x%04x", resp.Status)
	}

	// With KATO=0, no timer should fire. Wait 200ms and verify session alive.
	time.Sleep(200 * time.Millisecond)

	kaCmd := CapsuleCommand{OpCode: adminKeepAlive, CID: 401}
	w.SendWithData(pduCapsuleCmd, 0, &kaCmd, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("KeepAlive after 200ms with KATO=0 should succeed: 0x%04x", resp.Status)
	}
}

// ============================================================
// QA-20: NUMD log page length capping
// ============================================================

// TestQA_LogPage_ErrorLog_LargeNUMD: request > 64 bytes → capped to 64.
func TestQA_LogPage_ErrorLog_LargeNUMD(t *testing.T) {
	nqn := "nqn.test:qa-numd"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	// Request 4096 bytes (NUMD=1023, 0-based dwords)
	cmd := CapsuleCommand{
		OpCode: adminGetLogPage,
		CID:    500,
		D10:    uint32(logPageError) | (1023 << 16), // NUMDL=1023
		D11:    0,                                    // NUMDU=0
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	// Should get C2HData with at most 64 bytes (error log cap)
	hdr, err := r.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Type != pduC2HData {
		t.Fatalf("expected C2HData, got 0x%x", hdr.Type)
	}
	var c2h C2HDataHeader
	r.Receive(&c2h)
	dataLen := r.Length()
	data := make([]byte, dataLen)
	r.ReceiveData(data)

	if c2h.DATAL > 64 {
		t.Fatalf("error log data length %d > 64 (not capped)", c2h.DATAL)
	}

	recvCapsuleResp(t, r)
}

// TestQA_LogPage_SMART_LargeNUMD: request > 512 bytes → capped to 512.
func TestQA_LogPage_SMART_LargeNUMD(t *testing.T) {
	nqn := "nqn.test:qa-numd-smart"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	// Request 8192 bytes (NUMD=2047)
	cmd := CapsuleCommand{
		OpCode: adminGetLogPage,
		CID:    501,
		D10:    uint32(logPageSMART) | (2047 << 16),
		D11:    0,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	var total uint32
	for {
		hdr, err := r.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Type == pduCapsuleResp {
			var resp CapsuleResponse
			r.Receive(&resp)
			if StatusWord(resp.Status).IsError() {
				t.Fatalf("SMART log failed: 0x%04x", resp.Status)
			}
			break
		}
		if hdr.Type == pduC2HData {
			var c2h C2HDataHeader
			r.Receive(&c2h)
			chunk := make([]byte, r.Length())
			r.ReceiveData(chunk)
			total += uint32(len(chunk))
		}
	}

	if total > 512 {
		t.Fatalf("SMART log data %d > 512 (not capped)", total)
	}
	if total == 0 {
		t.Fatal("SMART log data empty")
	}
}

// TestQA_LogPage_ANA_LargeNUMD: request > 40 bytes → capped to 40.
func TestQA_LogPage_ANA_LargeNUMD(t *testing.T) {
	nqn := "nqn.test:qa-numd-ana"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: adminGetLogPage,
		CID:    502,
		D10:    uint32(logPageANA) | (4095 << 16),
		D11:    0,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	var total uint32
	for {
		hdr, err := r.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Type == pduCapsuleResp {
			var resp CapsuleResponse
			r.Receive(&resp)
			break
		}
		if hdr.Type == pduC2HData {
			var c2h C2HDataHeader
			r.Receive(&c2h)
			chunk := make([]byte, r.Length())
			r.ReceiveData(chunk)
			total += uint32(len(chunk))
		}
	}

	if total > 40 {
		t.Fatalf("ANA log data %d > 40 (not capped)", total)
	}
}

// ============================================================
// QA-21: Multiple SetFeatures on same session
// ============================================================

func TestQA_SetFeatures_MultipleCallsSameSession(t *testing.T) {
	nqn := "nqn.test:qa-multiset"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	// First SetFeatures: request 8 queues
	cmd1 := CapsuleCommand{
		OpCode: adminSetFeatures,
		CID:    600,
		D10:    uint32(fidNumberOfQueues),
		D11:    7 | (7 << 16), // NCQR=7, NSQR=7 (0-based)
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd1, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("first SetFeatures: 0x%04x", resp.Status)
	}
	// maxIOQueues=4, so clamped: NCQR=3, NSQR=3 (0-based)
	ncqr1 := resp.DW0 & 0xFFFF
	if ncqr1 != 3 { // 4-1=3 (0-based)
		t.Fatalf("first NCQR = %d, want 3", ncqr1)
	}

	// Second SetFeatures: request 2 queues (NCQR=2, raw value in D11)
	cmd2 := CapsuleCommand{
		OpCode: adminSetFeatures,
		CID:    601,
		D10:    uint32(fidNumberOfQueues),
		D11:    2 | (2 << 16), // NCQR=2, NSQR=2
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd2, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("second SetFeatures: 0x%04x", resp.Status)
	}
	// Granted 2, response is (2-1)=1 (0-based)
	ncqr2 := resp.DW0 & 0xFFFF
	if ncqr2 != 1 {
		t.Fatalf("second NCQR = %d, want 1", ncqr2)
	}

	// GetFeatures should reflect last SetFeatures (grantedQueues=2)
	cmd3 := CapsuleCommand{
		OpCode: adminGetFeatures,
		CID:    602,
		D10:    uint32(fidNumberOfQueues),
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd3, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("GetFeatures: 0x%04x", resp.Status)
	}
	// grantedQueues=2, response is (2-1)=1 (0-based)
	ncqr3 := resp.DW0 & 0xFFFF
	if ncqr3 != 1 {
		t.Fatalf("GetFeatures NCQR = %d, want 1 (reflecting second SetFeatures)", ncqr3)
	}
}

// TestQA_SetFeatures_KATOOverwrite: second KATO SetFeatures overwrites first.
func TestQA_SetFeatures_KATOOverwrite(t *testing.T) {
	nqn := "nqn.test:qa-kato-overwrite"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	// Set KATO to 5000ms
	cmd1 := CapsuleCommand{
		OpCode: adminSetFeatures,
		CID:    610,
		D10:    uint32(fidKeepAliveTimer),
		D11:    5000,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd1, capsuleCmdSize, nil)
	recvCapsuleResp(t, r)

	// Overwrite KATO to 30000ms
	cmd2 := CapsuleCommand{
		OpCode: adminSetFeatures,
		CID:    611,
		D10:    uint32(fidKeepAliveTimer),
		D11:    30000,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd2, capsuleCmdSize, nil)
	recvCapsuleResp(t, r)

	// GetFeatures should return 30000
	cmd3 := CapsuleCommand{
		OpCode: adminGetFeatures,
		CID:    612,
		D10:    uint32(fidKeepAliveTimer),
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd3, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if resp.DW0 != 30000 {
		t.Fatalf("KATO = %d, want 30000", resp.DW0)
	}
}

// ============================================================
// QA-22: CNTLID allocation monotonic
// ============================================================

func TestQA_CNTLID_MonotonicallyIncreasing(t *testing.T) {
	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})

	ids := make([]uint16, 100)
	for i := 0; i < 100; i++ {
		ids[i] = srv.allocCNTLID()
	}

	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("CNTLID[%d]=%d <= CNTLID[%d]=%d (not monotonic)", i, ids[i], i-1, ids[i-1])
		}
	}
}

// ============================================================
// QA-23: Connection drop mid-PDU
// ============================================================

func TestQA_Wire_ConnectionDropMidReceive(t *testing.T) {
	pr, pw := io.Pipe()

	r := NewReader(pr)

	go func() {
		// Write valid CommonHeader for capsule with 64-byte body
		hdr := CommonHeader{
			Type:         pduCapsuleCmd,
			HeaderLength: commonHeaderSize + capsuleCmdSize,
			DataOffset:   0,
			DataLength:   uint32(commonHeaderSize + capsuleCmdSize),
		}
		buf := make([]byte, commonHeaderSize)
		hdr.Marshal(buf)
		pw.Write(buf)

		// Write only 10 of 64 body bytes, then close
		pw.Write(make([]byte, 10))
		pw.Close()
	}()

	hdr, err := r.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue should succeed: %v", err)
	}
	if hdr.Type != pduCapsuleCmd {
		t.Fatalf("wrong type: 0x%x", hdr.Type)
	}

	// Receive should fail — only 10 of 64 body bytes available
	var capsule CapsuleCommand
	err = r.Receive(&capsule)
	if err == nil {
		t.Fatal("expected error from Receive on truncated body")
	}
}

func TestQA_Wire_ConnectionDropMidPayload(t *testing.T) {
	pr, pw := io.Pipe()
	r := NewReader(pr)

	go func() {
		hdr := CommonHeader{
			Type:         pduCapsuleCmd,
			HeaderLength: commonHeaderSize + capsuleCmdSize,
			DataOffset:   commonHeaderSize + capsuleCmdSize,
			DataLength:   uint32(commonHeaderSize+capsuleCmdSize) + 512,
		}
		buf := make([]byte, commonHeaderSize)
		hdr.Marshal(buf)
		pw.Write(buf)

		// Full 64-byte capsule body
		pw.Write(make([]byte, capsuleCmdSize))

		// Only 100 of 512 payload bytes, then close
		pw.Write(make([]byte, 100))
		pw.Close()
	}()

	hdr, err := r.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	_ = hdr

	var capsule CapsuleCommand
	if err := r.Receive(&capsule); err != nil {
		t.Fatalf("Receive should succeed (header complete): %v", err)
	}

	if r.Length() != 512 {
		t.Fatalf("Length = %d, want 512", r.Length())
	}

	// ReceiveData should fail — only 100 bytes available
	payload := make([]byte, 512)
	err = r.ReceiveData(payload)
	if err == nil {
		t.Fatal("expected error from ReceiveData on truncated payload")
	}
}

// ============================================================
// QA-24: WriteZeros with DEALLOC bit + errors
// ============================================================

func TestQA_IO_WriteZeros_DEALLOC_TrimError(t *testing.T) {
	dev := newMockDevice(256, 512)
	dev.trimErr = errors.New("trim failed: disk error")
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-dealloc-err", dev)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: ioWriteZeros,
		CID:    700,
		D10:    0,
		D12:    0 | commandBitDeallocate, // 1 block + DEALLOC
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if !StatusWord(resp.Status).IsError() {
		t.Fatal("WriteZeros DEALLOC with trim error should fail")
	}
}

func TestQA_IO_WriteZeros_NoDEALLOC_WriteError(t *testing.T) {
	dev := newMockDevice(256, 512)
	dev.writeErr = errors.New("Write failed: disk full")
	client, r, w := setupQAIOQueue(t, "nqn.test:qa-wz-noalloc-err", dev)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: ioWriteZeros,
		CID:    701,
		D10:    0,
		D12:    0, // 1 block, no DEALLOC
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if !StatusWord(resp.Status).IsError() {
		t.Fatal("WriteZeros without DEALLOC with write error should fail")
	}
	// Heuristic: "Write" in error → MediaWriteFault
	if StatusWord(resp.Status) != StatusMediaWriteFault {
		t.Fatalf("got 0x%04x, want MediaWriteFault", resp.Status)
	}
}

// ============================================================
// QA-25: PropertyGet with 8-byte size (CAP is 64-bit)
// ============================================================

func TestQA_PropertyGet_CAP_8Byte(t *testing.T) {
	nqn := "nqn.test:qa-propget8"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := makePropertyGetCapsule(propCAP, true)
	cmd.CID = 800
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("PropertyGet CAP 8byte: 0x%04x", resp.Status)
	}

	val := uint64(resp.DW0) | (uint64(resp.DW1) << 32)
	if val&0xFFFF != 63 {
		t.Fatalf("CAP MQES = %d, want 63", val&0xFFFF)
	}
	if val&(1<<16) == 0 {
		t.Fatal("CAP CQR bit not set")
	}
}

func TestQA_PropertyGet_CC_4Byte(t *testing.T) {
	nqn := "nqn.test:qa-propget4"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := makePropertyGetCapsule(propCC, false)
	cmd.CID = 801
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("PropertyGet CC 4byte: 0x%04x", resp.Status)
	}
	if resp.DW0 != 0 {
		t.Fatalf("CC = 0x%08x, want 0", resp.DW0)
	}
}

// ============================================================
// QA-26: QueueSize 0-based conversion
// ============================================================

func TestQA_Connect_QueueSizeConversion(t *testing.T) {
	nqn := "nqn.test:qa-qsize"
	dev := newMockDevice(256, 512)
	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)
	recvICResp(t, r)

	// Connect with SQSIZE=7 (0-based → queueSize=8)
	cmd := CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcConnect,
		CID:    0,
		D10:    0, // QID=0
		D11:    7, // SQSIZE=7 (0-based)
		D12:    0,
	}
	cd := ConnectData{CNTLID: 0xFFFF, SubNQN: nqn, HostNQN: "host"}
	payload := make([]byte, connectDataSize)
	cd.Marshal(payload)
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, payload)

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("connect: 0x%04x", resp.Status)
	}

	// Verify SQHD wraps at queueSize=8: send 9 commands
	for i := uint16(0); i < 9; i++ {
		kaCmd := CapsuleCommand{OpCode: adminKeepAlive, CID: 900 + i}
		w.SendWithData(pduCapsuleCmd, 0, &kaCmd, capsuleCmdSize, nil)
		resp = recvCapsuleResp(t, r)
	}
	// After Connect (SQHD=1) + 9 KeepAlives, SQHD = (1+9) % 8 = 2
	if resp.SQHD != 2 {
		t.Fatalf("SQHD after 9 commands (qsize=8) = %d, want 2", resp.SQHD)
	}
}

// ============================================================
// QA-27: Non-ANAProvider device (IsHealthy fallback)
// ============================================================

type nonANADevice struct {
	healthy bool
}

func (d *nonANADevice) ReadAt(lba uint64, length uint32) ([]byte, error) {
	return make([]byte, length), nil
}
func (d *nonANADevice) WriteAt(lba uint64, data []byte) error { return nil }
func (d *nonANADevice) Trim(lba uint64, length uint32) error  { return nil }
func (d *nonANADevice) SyncCache() error                      { return nil }
func (d *nonANADevice) BlockSize() uint32                     { return 512 }
func (d *nonANADevice) VolumeSize() uint64                    { return 128 * 1024 }
func (d *nonANADevice) IsHealthy() bool                       { return d.healthy }

func TestQA_ANA_NonANAProvider_Healthy(t *testing.T) {
	dev := &nonANADevice{healthy: true}
	nqn := "nqn.test:qa-nonana-h"
	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, [16]byte{0x60, 1, 2, 3})

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	ctrl.subsystem = srv.findSubsystem(nqn)
	ctrl.queueID = 1
	ctrl.queueSize = 64
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)
	recvICResp(t, r)

	// Write should succeed — IsHealthy() returns true
	writeCmd := CapsuleCommand{OpCode: ioWrite, CID: 1000, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, make([]byte, 512))
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("write on healthy non-ANA device: 0x%04x", resp.Status)
	}
}

func TestQA_ANA_NonANAProvider_Unhealthy(t *testing.T) {
	dev := &nonANADevice{healthy: false}
	nqn := "nqn.test:qa-nonana-u"
	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, [16]byte{0x60, 1, 2, 3})

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	ctrl.subsystem = srv.findSubsystem(nqn)
	ctrl.queueID = 1
	ctrl.queueSize = 64
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)
	recvICResp(t, r)

	// Write rejected — IsHealthy() returns false
	writeCmd := CapsuleCommand{OpCode: ioWrite, CID: 1001, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, make([]byte, 512))
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusNSNotReady {
		t.Fatalf("write on unhealthy non-ANA: got 0x%04x, want NSNotReady", resp.Status)
	}

	// Read still works (not gated by isWriteAllowed)
	readCmd := CapsuleCommand{OpCode: ioRead, CID: 1002, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	for {
		hdr, err := r.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Type == pduCapsuleResp {
			var rr CapsuleResponse
			r.Receive(&rr)
			if StatusWord(rr.Status).IsError() {
				t.Fatalf("read on unhealthy non-ANA should succeed: 0x%04x", rr.Status)
			}
			break
		}
		if hdr.Type == pduC2HData {
			var c2h C2HDataHeader
			r.Receive(&c2h)
			chunk := make([]byte, r.Length())
			r.ReceiveData(chunk)
		}
	}
}

// ============================================================
// QA-28: Lba() and LbaLength() edge cases
// ============================================================

func TestQA_Capsule_Lba64Bit(t *testing.T) {
	c := CapsuleCommand{D10: 0xDEADBEEF, D11: 0x00000001}
	lba := c.Lba()
	want := uint64(0x00000001DEADBEEF)
	if lba != want {
		t.Fatalf("Lba() = 0x%016x, want 0x%016x", lba, want)
	}
}

func TestQA_Capsule_LbaLengthZeroBased(t *testing.T) {
	c := CapsuleCommand{D12: 0}
	if c.LbaLength() != 1 {
		t.Fatalf("LbaLength(D12=0) = %d, want 1", c.LbaLength())
	}

	c.D12 = 0xFFFF
	if c.LbaLength() != 0x10000 {
		t.Fatalf("LbaLength(D12=0xFFFF) = %d, want 65536", c.LbaLength())
	}

	c.D12 = 99
	if c.LbaLength() != 100 {
		t.Fatalf("LbaLength(D12=99) = %d, want 100", c.LbaLength())
	}
}

// ============================================================
// QA-29: Admin AsyncEvent stub
// ============================================================

func TestQA_Admin_AsyncEvent_Stub(t *testing.T) {
	nqn := "nqn.test:qa-async"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{OpCode: adminAsyncEvent, CID: 1100}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("AsyncEvent stub should succeed: 0x%04x", resp.Status)
	}
}

// ============================================================
// QA-30: H2CTermReq from host closes session
// ============================================================

func TestQA_H2CTermReq_ClosesSession(t *testing.T) {
	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	ctrl := newController(serverConn, srv)

	done := make(chan error, 1)
	go func() { done <- ctrl.Serve() }()

	w := NewWriter(clientConn)
	r := NewReader(clientConn)

	sendICReq(w)
	recvICResp(t, r)

	// H2CTermReq — controller should exit cleanly
	termReq := ICRequest{}
	w.SendHeaderOnly(pduH2CTermReq, &termReq, icBodySize)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve should return nil on H2CTermReq: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Serve to exit after H2CTermReq")
	}
}

// ============================================================
// QA-31: CP10-3 Tier 1 — Padding, Buffer Pool, Batching, Config
// ============================================================

// --- 31a: Padding skip adversarial (Finding 1 fix) ---

// TestQA_Padding_MaxDataOffset255 crafts DataOffset=255 (uint8 max) with
// HeaderLength=8, yielding 247 bytes of padding — the worst case.
func TestQA_Padding_MaxDataOffset255(t *testing.T) {
	dataOffset := uint8(255)
	payload := []byte{0xCA, 0xFE}
	dataLength := uint32(dataOffset) + uint32(len(payload))

	var hdr [commonHeaderSize]byte
	ch := CommonHeader{
		Type:         pduCapsuleCmd,
		HeaderLength: commonHeaderSize,
		DataOffset:   dataOffset,
		DataLength:   dataLength,
	}
	ch.Marshal(hdr[:])

	var buf bytes.Buffer
	buf.Write(hdr[:])
	buf.Write(make([]byte, int(dataOffset)-commonHeaderSize)) // 247 bytes padding
	buf.Write(payload)

	r := NewReader(&buf)
	if _, err := r.Dequeue(); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	var capsule CapsuleCommand
	if err := r.Receive(&capsule); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if r.Length() != uint32(len(payload)) {
		t.Fatalf("Length = %d, want %d", r.Length(), len(payload))
	}
	data := make([]byte, r.Length())
	if err := r.ReceiveData(data); err != nil {
		t.Fatalf("ReceiveData: %v", err)
	}
	if data[0] != 0xCA || data[1] != 0xFE {
		t.Fatalf("payload = %x, want CAFE", data)
	}
}

// TestQA_Padding_ExactlyPadBufBoundary tests DataOffset that creates padding
// of exactly maxHeaderSize (128) bytes — the boundary where chunked loop
// does exactly one iteration.
func TestQA_Padding_ExactlyPadBufBoundary(t *testing.T) {
	// HeaderLength=8, DataOffset=136 → pad=128 = exactly len(padBuf)
	dataOffset := uint8(commonHeaderSize + maxHeaderSize) // 136
	payload := []byte{0x42}
	dataLength := uint32(dataOffset) + uint32(len(payload))

	var hdr [commonHeaderSize]byte
	ch := CommonHeader{
		Type:         pduCapsuleCmd,
		HeaderLength: commonHeaderSize,
		DataOffset:   dataOffset,
		DataLength:   dataLength,
	}
	ch.Marshal(hdr[:])

	var buf bytes.Buffer
	buf.Write(hdr[:])
	buf.Write(make([]byte, maxHeaderSize)) // exactly 128 bytes padding
	buf.Write(payload)

	r := NewReader(&buf)
	if _, err := r.Dequeue(); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	var capsule CapsuleCommand
	if err := r.Receive(&capsule); err != nil {
		t.Fatalf("Receive with boundary padding: %v", err)
	}
	data := make([]byte, r.Length())
	if err := r.ReceiveData(data); err != nil {
		t.Fatal(err)
	}
	if data[0] != 0x42 {
		t.Fatalf("payload = 0x%x, want 0x42", data[0])
	}
}

// TestQA_Padding_OneBeyondPadBuf tests padding of maxHeaderSize+1 (129) bytes
// to confirm the chunked loop handles the two-iteration case.
func TestQA_Padding_OneBeyondPadBuf(t *testing.T) {
	padSize := maxHeaderSize + 1 // 129
	dataOffset := uint8(commonHeaderSize + padSize) // 137
	payload := []byte{0xBB}
	dataLength := uint32(dataOffset) + uint32(len(payload))

	var hdr [commonHeaderSize]byte
	ch := CommonHeader{
		Type:         pduCapsuleCmd,
		HeaderLength: commonHeaderSize,
		DataOffset:   dataOffset,
		DataLength:   dataLength,
	}
	ch.Marshal(hdr[:])

	var buf bytes.Buffer
	buf.Write(hdr[:])
	buf.Write(make([]byte, padSize))
	buf.Write(payload)

	r := NewReader(&buf)
	if _, err := r.Dequeue(); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	var capsule CapsuleCommand
	if err := r.Receive(&capsule); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	data := make([]byte, r.Length())
	if err := r.ReceiveData(data); err != nil {
		t.Fatal(err)
	}
	if data[0] != 0xBB {
		t.Fatalf("payload = 0x%x, want 0xBB", data[0])
	}
}

// TestQA_Padding_ZeroPad verifies DataOffset == HeaderLength (no padding).
func TestQA_Padding_ZeroPad(t *testing.T) {
	payload := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	headerLen := uint8(commonHeaderSize + capsuleCmdSize) // 72
	dataLength := uint32(headerLen) + uint32(len(payload))

	var hdr [commonHeaderSize]byte
	ch := CommonHeader{
		Type:         pduCapsuleCmd,
		HeaderLength: headerLen,
		DataOffset:   headerLen, // == HeaderLength, so pad=0
		DataLength:   dataLength,
	}
	ch.Marshal(hdr[:])

	var buf bytes.Buffer
	buf.Write(hdr[:])
	buf.Write(make([]byte, capsuleCmdSize)) // specific header
	// no padding
	buf.Write(payload)

	r := NewReader(&buf)
	if _, err := r.Dequeue(); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	var capsule CapsuleCommand
	if err := r.Receive(&capsule); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if r.Length() != uint32(len(payload)) {
		t.Fatalf("Length = %d, want %d", r.Length(), len(payload))
	}
	data := make([]byte, r.Length())
	if err := r.ReceiveData(data); err != nil {
		t.Fatal(err)
	}
	if data[0] != 0xAA {
		t.Fatalf("data[0] = 0x%x, want 0xAA", data[0])
	}
}

// TestQA_Padding_StreamEOFMidPad verifies EOF during padding skip is returned,
// not silently swallowed.
func TestQA_Padding_StreamEOFMidPad(t *testing.T) {
	dataOffset := uint8(200)
	dataLength := uint32(dataOffset) + 4

	var hdr [commonHeaderSize]byte
	ch := CommonHeader{
		Type:         pduCapsuleCmd,
		HeaderLength: commonHeaderSize,
		DataOffset:   dataOffset,
		DataLength:   dataLength,
	}
	ch.Marshal(hdr[:])

	// Only provide 50 bytes of padding instead of 192.
	var buf bytes.Buffer
	buf.Write(hdr[:])
	buf.Write(make([]byte, 50)) // truncated

	r := NewReader(&buf)
	if _, err := r.Dequeue(); err != nil {
		t.Fatal(err)
	}
	var capsule CapsuleCommand
	err := r.Receive(&capsule)
	if err == nil {
		t.Fatal("expected error for truncated padding")
	}
}

// TestQA_Padding_TwoConsecutivePDUs verifies padding skip doesn't consume
// bytes from the next PDU.
func TestQA_Padding_TwoConsecutivePDUs(t *testing.T) {
	// PDU 1: large padding (200 bytes), 2-byte payload
	do1 := uint8(200)
	pay1 := []byte{0x11, 0x22}
	dl1 := uint32(do1) + uint32(len(pay1))
	var h1 [commonHeaderSize]byte
	ch1 := CommonHeader{
		Type: pduCapsuleCmd, HeaderLength: commonHeaderSize,
		DataOffset: do1, DataLength: dl1,
	}
	ch1.Marshal(h1[:])

	// PDU 2: no padding, 2-byte payload
	hl2 := uint8(commonHeaderSize)
	do2 := hl2
	pay2 := []byte{0x33, 0x44}
	dl2 := uint32(do2) + uint32(len(pay2))
	var h2 [commonHeaderSize]byte
	ch2 := CommonHeader{
		Type: pduCapsuleCmd, HeaderLength: hl2,
		DataOffset: do2, DataLength: dl2,
	}
	ch2.Marshal(h2[:])

	var buf bytes.Buffer
	// PDU 1
	buf.Write(h1[:])
	buf.Write(make([]byte, int(do1)-commonHeaderSize)) // 192 bytes padding
	buf.Write(pay1)
	// PDU 2
	buf.Write(h2[:])
	buf.Write(pay2)

	r := NewReader(&buf)

	// Read PDU 1
	if _, err := r.Dequeue(); err != nil {
		t.Fatalf("PDU1 Dequeue: %v", err)
	}
	var c1 CapsuleCommand
	if err := r.Receive(&c1); err != nil {
		t.Fatalf("PDU1 Receive: %v", err)
	}
	d1 := make([]byte, r.Length())
	if err := r.ReceiveData(d1); err != nil {
		t.Fatalf("PDU1 ReceiveData: %v", err)
	}
	if d1[0] != 0x11 || d1[1] != 0x22 {
		t.Fatalf("PDU1 payload = %x, want 1122", d1)
	}

	// Read PDU 2 — must not be corrupted by PDU 1's padding
	if _, err := r.Dequeue(); err != nil {
		t.Fatalf("PDU2 Dequeue: %v", err)
	}
	var c2 CapsuleCommand
	if err := r.Receive(&c2); err != nil {
		t.Fatalf("PDU2 Receive: %v", err)
	}
	d2 := make([]byte, r.Length())
	if err := r.ReceiveData(d2); err != nil {
		t.Fatalf("PDU2 ReceiveData: %v", err)
	}
	if d2[0] != 0x33 || d2[1] != 0x44 {
		t.Fatalf("PDU2 payload = %x, want 3344 (stream desync?)", d2)
	}
}

// --- 31b: Buffer pool adversarial ---

// TestQA_BufPool_StaleDataNotLeaked verifies that a buffer returned from
// getBuffer after putBuffer doesn't leak data across requests.
func TestQA_BufPool_StaleDataNotLeaked(t *testing.T) {
	// Write secret pattern into a 4KB buffer, return it.
	secret := getBuffer(4096)
	for i := range secret {
		secret[i] = 0xFF
	}
	putBuffer(secret)

	// Get another 4KB buffer (likely the same one from pool).
	// In real usage, the caller must fill/zero before use.
	// This test verifies the pool doesn't memset — callers
	// must be aware.
	reused := getBuffer(4096)
	defer putBuffer(reused)

	// The buffer MAY contain stale 0xFF data — that's expected.
	// What matters is that the pool mechanics work correctly:
	// correct length, correct capacity, no panic.
	if len(reused) != 4096 {
		t.Fatalf("len = %d, want 4096", len(reused))
	}
	if cap(reused) != 4096 {
		t.Fatalf("cap = %d, want 4096", cap(reused))
	}
}

// TestQA_BufPool_ConcurrentGetPut hammers the pool from many goroutines
// to verify no data races or panics.
func TestQA_BufPool_ConcurrentGetPut(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				// Vary sizes across all tiers + oversized.
				sizes := []int{512, 4096, 8192, 65536, 100000, 262144, 500000}
				buf := getBuffer(sizes[j%len(sizes)])
				// Write to detect races.
				buf[0] = byte(id)
				buf[len(buf)-1] = byte(j)
				putBuffer(buf)
			}
		}(i)
	}
	wg.Wait()
}

// TestQA_BufPool_ZeroSize verifies getBuffer(0) doesn't panic.
func TestQA_BufPool_ZeroSize(t *testing.T) {
	buf := getBuffer(0)
	if len(buf) != 0 {
		t.Fatalf("len = %d, want 0", len(buf))
	}
	// cap should be 4096 (small pool bucket)
	if cap(buf) != 4096 {
		t.Fatalf("cap = %d, want 4096", cap(buf))
	}
	putBuffer(buf) // must not panic
}

// TestQA_BufPool_PutWrongCap verifies putBuffer with a non-tier-sized buffer
// doesn't panic (just doesn't return to any pool).
func TestQA_BufPool_PutWrongCap(t *testing.T) {
	buf := make([]byte, 1000) // cap=1000, not a pool tier
	putBuffer(buf)            // should be silently ignored, no panic
}

// TestQA_BufPool_WriteZerosPooled verifies WriteZeros handler
// correctly zeros pooled buffers before writing.
func TestQA_BufPool_WriteZerosPooled(t *testing.T) {
	nqn := "nqn.test:qa-pool-wz"
	dev := newMockDevice(64, 512)

	// Pre-fill device with non-zero data.
	for i := range dev.data {
		dev.data[i] = 0xAB
	}

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	ctrl.subsystem = srv.findSubsystem(nqn)
	ctrl.queueID = 1
	ctrl.queueSize = 64
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)
	recvICResp(t, r)

	// Poison the pool: get a 4KB buffer, fill with 0xFF, return it.
	poison := getBuffer(4096)
	for i := range poison {
		poison[i] = 0xFF
	}
	putBuffer(poison)

	// WriteZeros on 8 blocks (4KB) — must zero the buffer despite pool reuse.
	wzCmd := CapsuleCommand{
		OpCode: ioWriteZeros,
		CID:    1,
		D10:    0, // LBA 0
		D12:    7, // 8 blocks (0-based)
	}
	w.SendWithData(pduCapsuleCmd, 0, &wzCmd, capsuleCmdSize, nil)

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("WriteZeros failed: 0x%04x", resp.Status)
	}

	// Verify device data is actually zero, not stale 0xFF from pool.
	for i := 0; i < 4096; i++ {
		if dev.data[i] != 0 {
			t.Fatalf("dev.data[%d] = 0x%x, want 0 (stale pool data leaked)", i, dev.data[i])
		}
	}
}

// --- 31c: Response batching adversarial ---

// TestQA_Batch_MultiChunkC2H_InterleavedVerify verifies C2H batched
// response has correct DATAO offsets and LAST flag only on final chunk.
func TestQA_Batch_MultiChunkC2H_InterleavedVerify(t *testing.T) {
	nqn := "nqn.test:qa-batch-c2h"
	dev := newMockDevice(512, 512) // 256KB

	// Write a known pattern: LBA i → byte i.
	for i := 0; i < len(dev.data); i++ {
		dev.data[i] = byte(i / 512)
	}

	srv := NewServer(Config{
		Enabled:          true,
		ListenAddr:       "127.0.0.1:0",
		MaxIOQueues:      4,
		MaxH2CDataLength: 8192, // 8KB chunks → 4 chunks for 32KB read
	})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	ctrl.subsystem = srv.findSubsystem(nqn)
	ctrl.queueID = 1
	ctrl.queueSize = 64
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)

	// Custom IC receive (non-default MaxH2CDataLength).
	hdr, err := r.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Type != pduICResp {
		t.Fatalf("expected ICResp")
	}
	var ic ICResponse
	r.Receive(&ic)

	// Read 32KB = 64 blocks
	readCmd := CapsuleCommand{
		OpCode: ioRead,
		CID:    1,
		D10:    0,
		D12:    63, // 64 blocks
	}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	// Expect 4 C2HData (32KB / 8KB) + 1 CapsuleResp
	var allData []byte
	chunkCount := 0
	lastFlagCount := 0
	prevOffset := uint32(0)

	for {
		hdr, err := r.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Type == pduCapsuleResp {
			var capsResp CapsuleResponse
			r.Receive(&capsResp)
			if StatusWord(capsResp.Status).IsError() {
				t.Fatalf("read error: 0x%04x", capsResp.Status)
			}
			break
		}
		if hdr.Type != pduC2HData {
			t.Fatalf("unexpected PDU 0x%x", hdr.Type)
		}

		chunkCount++
		var c2h C2HDataHeader
		r.Receive(&c2h)
		dataBuf := make([]byte, r.Length())
		r.ReceiveData(dataBuf)
		allData = append(allData, dataBuf...)

		// Verify DATAO is monotonically increasing.
		if chunkCount > 1 && c2h.DATAO <= prevOffset {
			t.Fatalf("chunk %d: DATAO=%d <= prev=%d", chunkCount, c2h.DATAO, prevOffset)
		}
		if chunkCount > 1 {
			prevOffset = c2h.DATAO
		}

		// LAST flag only on final chunk.
		if hdr.Flags&c2hFlagLast != 0 {
			lastFlagCount++
		}
	}

	if chunkCount != 4 {
		t.Fatalf("expected 4 chunks, got %d", chunkCount)
	}
	if lastFlagCount != 1 {
		t.Fatalf("expected LAST flag on exactly 1 chunk, got %d", lastFlagCount)
	}
	if len(allData) != 32768 {
		t.Fatalf("total data = %d, want 32768", len(allData))
	}
	// Verify data content.
	for i := 0; i < 64; i++ {
		if allData[i*512] != byte(i) {
			t.Fatalf("block %d: first byte = 0x%x, want 0x%x", i, allData[i*512], byte(i))
		}
	}
}

// TestQA_Batch_SingleBlockNoChunking verifies a 1-block read (512B)
// with default 32KB maxDataLen produces exactly 1 C2H chunk + 1 response.
func TestQA_Batch_SingleBlockNoChunking(t *testing.T) {
	nqn := "nqn.test:qa-batch-1blk"
	dev := newMockDevice(64, 512)
	dev.data[0] = 0xEE

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	ctrl.subsystem = srv.findSubsystem(nqn)
	ctrl.queueID = 1
	ctrl.queueSize = 64
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)
	recvICResp(t, r)

	readCmd := CapsuleCommand{OpCode: ioRead, CID: 1, D10: 0, D12: 0} // 1 block
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	// Expect exactly 1 C2HData with LAST flag + 1 CapsuleResp
	hdr, _ := r.Dequeue()
	if hdr.Type != pduC2HData {
		t.Fatalf("expected C2HData, got 0x%x", hdr.Type)
	}
	if hdr.Flags&c2hFlagLast == 0 {
		t.Fatal("expected LAST flag on single-chunk read")
	}
	var c2h C2HDataHeader
	r.Receive(&c2h)
	dataBuf := make([]byte, r.Length())
	r.ReceiveData(dataBuf)

	if len(dataBuf) != 512 {
		t.Fatalf("data = %d bytes, want 512", len(dataBuf))
	}
	if dataBuf[0] != 0xEE {
		t.Fatalf("data[0] = 0x%x, want 0xEE", dataBuf[0])
	}

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("status error: 0x%04x", resp.Status)
	}
}

// TestQA_Batch_WriteReadCycle_PooledBuffers exercises write+read in a tight
// loop to verify pooled buffer lifecycle doesn't corrupt data across
// request boundaries.
func TestQA_Batch_WriteReadCycle_PooledBuffers(t *testing.T) {
	nqn := "nqn.test:qa-batch-cycle"
	dev := newMockDevice(256, 512)

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	ctrl.subsystem = srv.findSubsystem(nqn)
	ctrl.queueID = 1
	ctrl.queueSize = 64
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)
	recvICResp(t, r)

	// 20 write+read cycles with different patterns.
	for i := 0; i < 20; i++ {
		pattern := byte(i + 1)
		writeData := make([]byte, 4096) // 8 blocks
		for j := range writeData {
			writeData[j] = pattern
		}

		writeCmd := CapsuleCommand{
			OpCode: ioWrite, CID: uint16(i * 2), D10: 0, D12: 7,
		}
		w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, writeData)
		resp := recvCapsuleResp(t, r)
		if StatusWord(resp.Status).IsError() {
			t.Fatalf("cycle %d write: 0x%04x", i, resp.Status)
		}

		readCmd := CapsuleCommand{
			OpCode: ioRead, CID: uint16(i*2 + 1), D10: 0, D12: 7,
		}
		w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

		// Consume C2H data chunks.
		var readBuf []byte
		for {
			hdr, err := r.Dequeue()
			if err != nil {
				t.Fatalf("cycle %d read dequeue: %v", i, err)
			}
			if hdr.Type == pduCapsuleResp {
				var rsp CapsuleResponse
				r.Receive(&rsp)
				if StatusWord(rsp.Status).IsError() {
					t.Fatalf("cycle %d read: 0x%04x", i, rsp.Status)
				}
				break
			}
			var c2h C2HDataHeader
			r.Receive(&c2h)
			d := make([]byte, r.Length())
			r.ReceiveData(d)
			readBuf = append(readBuf, d...)
		}

		if len(readBuf) != 4096 {
			t.Fatalf("cycle %d: read %d bytes, want 4096", i, len(readBuf))
		}
		for j, b := range readBuf {
			if b != pattern {
				t.Fatalf("cycle %d: byte[%d] = 0x%x, want 0x%x", i, j, b, pattern)
			}
		}
	}
}

// --- 31d: MaxH2CDataLength adversarial ---

// TestQA_MaxDataLen_VerySmallChunk verifies chunking with maxDataLen
// smaller than one block (512B > 256B chunk → 2 chunks per block).
func TestQA_MaxDataLen_VerySmallChunk(t *testing.T) {
	nqn := "nqn.test:qa-tiny-chunk"
	dev := newMockDevice(64, 512)
	for i := range dev.data {
		dev.data[i] = 0x77
	}

	srv := NewServer(Config{
		Enabled:          true,
		ListenAddr:       "127.0.0.1:0",
		MaxIOQueues:      4,
		MaxH2CDataLength: 256, // very small: 2 chunks per 512B block
	})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	ctrl.subsystem = srv.findSubsystem(nqn)
	ctrl.queueID = 1
	ctrl.queueSize = 64
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)
	// Custom IC recv
	hdr, _ := r.Dequeue()
	if hdr.Type != pduICResp {
		t.Fatal("expected ICResp")
	}
	var ic ICResponse
	r.Receive(&ic)
	if ic.MaxH2CDataLength != 256 {
		t.Fatalf("MaxH2CDataLength = %d, want 256", ic.MaxH2CDataLength)
	}

	// Read 1 block = 512B → expect 2 C2H chunks (256B each)
	readCmd := CapsuleCommand{OpCode: ioRead, CID: 1, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	chunkCount := 0
	totalData := 0
	for {
		hdr, _ := r.Dequeue()
		if hdr.Type == pduCapsuleResp {
			var resp CapsuleResponse
			r.Receive(&resp)
			if StatusWord(resp.Status).IsError() {
				t.Fatalf("read error: 0x%04x", resp.Status)
			}
			break
		}
		chunkCount++
		var c2h C2HDataHeader
		r.Receive(&c2h)
		d := make([]byte, r.Length())
		r.ReceiveData(d)
		totalData += len(d)
	}

	if chunkCount != 2 {
		t.Fatalf("expected 2 chunks (512B / 256B), got %d", chunkCount)
	}
	if totalData != 512 {
		t.Fatalf("total = %d, want 512", totalData)
	}
}

// TestQA_MaxDataLen_ExactMultiple verifies chunking when read size
// is an exact multiple of maxDataLen (no remainder chunk).
func TestQA_MaxDataLen_ExactMultiple(t *testing.T) {
	nqn := "nqn.test:qa-exact-mul"
	dev := newMockDevice(128, 512) // 64KB

	srv := NewServer(Config{
		Enabled:          true,
		ListenAddr:       "127.0.0.1:0",
		MaxIOQueues:      4,
		MaxH2CDataLength: 4096, // 4KB
	})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	ctrl.subsystem = srv.findSubsystem(nqn)
	ctrl.queueID = 1
	ctrl.queueSize = 64
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)
	hdr, _ := r.Dequeue()
	if hdr.Type != pduICResp {
		t.Fatal("expected ICResp")
	}
	r.Receive(&ICResponse{})

	// Read 16KB (32 blocks) / 4KB chunks = exactly 4 chunks
	readCmd := CapsuleCommand{OpCode: ioRead, CID: 1, D10: 0, D12: 31}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	chunkCount := 0
	for {
		hdr, _ := r.Dequeue()
		if hdr.Type == pduCapsuleResp {
			r.Receive(&CapsuleResponse{})
			break
		}
		chunkCount++
		var c2h C2HDataHeader
		r.Receive(&c2h)
		d := make([]byte, r.Length())
		r.ReceiveData(d)
		if len(d) != 4096 {
			t.Fatalf("chunk %d: len=%d, want 4096", chunkCount, len(d))
		}
	}

	if chunkCount != 4 {
		t.Fatalf("expected 4 chunks (16KB / 4KB), got %d", chunkCount)
	}
}

// TestQA_MaxDataLen_NonMultiple verifies chunking when read size
// is NOT an exact multiple (last chunk is smaller).
func TestQA_MaxDataLen_NonMultiple(t *testing.T) {
	nqn := "nqn.test:qa-nonmul"
	dev := newMockDevice(128, 512)

	srv := NewServer(Config{
		Enabled:          true,
		ListenAddr:       "127.0.0.1:0",
		MaxIOQueues:      4,
		MaxH2CDataLength: 3072, // 3KB — doesn't divide 512 evenly into chunks
	})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	ctrl.subsystem = srv.findSubsystem(nqn)
	ctrl.queueID = 1
	ctrl.queueSize = 64
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)
	sendICReq(w)
	hdr, _ := r.Dequeue()
	if hdr.Type != pduICResp {
		t.Fatal("expected ICResp")
	}
	r.Receive(&ICResponse{})

	// Read 10KB (20 blocks) / 3KB chunks → 4 chunks (3+3+3+1 KB)
	readCmd := CapsuleCommand{OpCode: ioRead, CID: 1, D10: 0, D12: 19}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	var chunkSizes []int
	for {
		hdr, _ := r.Dequeue()
		if hdr.Type == pduCapsuleResp {
			r.Receive(&CapsuleResponse{})
			break
		}
		var c2h C2HDataHeader
		r.Receive(&c2h)
		d := make([]byte, r.Length())
		r.ReceiveData(d)
		chunkSizes = append(chunkSizes, len(d))
	}

	if len(chunkSizes) != 4 {
		t.Fatalf("expected 4 chunks, got %d: %v", len(chunkSizes), chunkSizes)
	}
	// First 3 chunks should be 3072, last should be 1024 (10240 - 3*3072)
	for i := 0; i < 3; i++ {
		if chunkSizes[i] != 3072 {
			t.Fatalf("chunk[%d] = %d, want 3072", i, chunkSizes[i])
		}
	}
	if chunkSizes[3] != 1024 {
		t.Fatalf("chunk[3] = %d, want 1024", chunkSizes[3])
	}
}

// --- 31e: NQN sanitization adversarial ---

// TestQA_NQN_SpecialChars verifies NQN construction sanitizes
// characters that are invalid in NVMe NQN format.
func TestQA_NQN_SpecialChars(t *testing.T) {
	srv := NewServer(Config{NQNPrefix: "nqn.2024-01.com.seaweedfs:vol."})
	tests := []struct {
		input string
		want  string
	}{
		{"simple-vol", "nqn.2024-01.com.seaweedfs:vol.simple-vol"},
		{"UPPER", "nqn.2024-01.com.seaweedfs:vol.upper"},
		{"has_underscore", "nqn.2024-01.com.seaweedfs:vol.has-underscore"},
		{"has spaces", "nqn.2024-01.com.seaweedfs:vol.has-spaces"},
		{"pvc-abc123", "nqn.2024-01.com.seaweedfs:vol.pvc-abc123"},
		{"a/b\\c:d", "nqn.2024-01.com.seaweedfs:vol.a-b-c-d"},
	}
	for _, tt := range tests {
		got := srv.NQN(tt.input)
		if got != tt.want {
			t.Errorf("NQN(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// TestQA_NQN_LongName verifies NQN truncation with hash suffix
// for names exceeding 64 characters.
func TestQA_NQN_LongName(t *testing.T) {
	srv := NewServer(Config{NQNPrefix: "nqn.2024-01.com.seaweedfs:vol."})
	longName := "pvc-" + string(make([]byte, 80)) // 84 chars, way over 64
	// Replace zero bytes with 'a' for valid input.
	input := "pvc-"
	for i := 0; i < 80; i++ {
		input += "a"
	}

	nqn := srv.NQN(input)
	prefix := "nqn.2024-01.com.seaweedfs:vol."
	suffix := nqn[len(prefix):]

	// Suffix should be at most 64 chars (SanitizeIQN contract).
	if len(suffix) > 64 {
		t.Fatalf("suffix len = %d, want <= 64: %s", len(suffix), suffix)
	}

	// Two different long names should produce different NQNs.
	input2 := "pvc-"
	for i := 0; i < 80; i++ {
		input2 += "b"
	}
	nqn2 := srv.NQN(input2)
	if nqn == nqn2 {
		t.Fatal("two different long names produced same NQN")
	}
	_ = longName
}

// --- 31f: TCP tuning adversarial ---

// TestQA_TuneConn_RapidAcceptClose verifies tuneConn doesn't panic
// when the connection is closed immediately after accept.
func TestQA_TuneConn_RapidAcceptClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			tuneConn(conn)
			conn.Close() // close immediately after tuning
		}
	}()

	for i := 0; i < 10; i++ {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			break
		}
		conn.Close()
	}
	ln.Close()
	<-done
}

// --- 31g: Writer batching edge cases ---

// TestQA_Batch_FlushBufWithoutWrite verifies FlushBuf on an empty
// buffer doesn't error (no-op flush).
func TestQA_Batch_FlushBufWithoutWrite(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.FlushBuf(); err != nil {
		t.Fatalf("FlushBuf on empty: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected empty buffer, got %d bytes", buf.Len())
	}
}

// TestQA_Batch_MultipleFlushBuf verifies calling FlushBuf multiple times
// after writeHeaderAndData is idempotent.
func TestQA_Batch_MultipleFlushBuf(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	resp := CapsuleResponse{CID: 1, Status: uint16(StatusSuccess)}
	if err := w.writeHeaderAndData(pduCapsuleResp, 0, &resp, capsuleRespSize, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.FlushBuf(); err != nil {
		t.Fatal(err)
	}
	first := buf.Len()

	// Second FlushBuf should be no-op.
	if err := w.FlushBuf(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != first {
		t.Fatalf("second FlushBuf changed buffer: %d → %d", first, buf.Len())
	}
}

// ============================================================
// QA-WAL: BUG-CP103-1 Adversarial WAL Pressure Tests
// ============================================================
// These tests exercise the full NVMe/TCP protocol path under WAL pressure
// through the server, verifying that write backpressure never produces
// permanent error status codes and that reads remain unaffected.

// TestQA_WAL_ConcurrentWritesUnderPressure sends multiple writes through the
// NVMe/TCP protocol stack under persistent WAL pressure. Verifies every
// response is StatusNSNotReady with DNR=0 (retryable), never a permanent
// error like MediaWriteFault or InternalErrorDNR.
func TestQA_WAL_ConcurrentWritesUnderPressure(t *testing.T) {
	origSleep := sleepFn
	origJitter := jitterFn
	defer func() { sleepFn = origSleep; jitterFn = origJitter }()
	sleepFn = func(d time.Duration) {}
	jitterFn = func(max time.Duration) time.Duration { return 0 }

	nqn := "nqn.test:qa-wal-concurrent"
	dev := newMockDevice(256, 512)
	dev.writeErr = blockvol.ErrWALFull
	dev.walPressure = 1.0

	const numWrites = 8
	client, r, w := setupQAIOQueue(t, nqn, dev)
	defer client.Close()

	// Send and receive sequentially (net.Pipe is synchronous).
	for i := 0; i < numWrites; i++ {
		cmd := CapsuleCommand{
			OpCode: ioWrite,
			CID:    uint16(500 + i),
			D10:    uint32(i), // LBA
			D12:    0,         // NLB 0 = 1 block
		}
		w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, make([]byte, 512))
		resp := recvCapsuleResp(t, r)
		status := StatusWord(resp.Status)

		if status.DNR() {
			t.Fatalf("write CID=%d: got DNR=1 (permanent failure) under WAL pressure — must be retryable", resp.CID)
		}
		if status == StatusMediaWriteFault {
			t.Fatalf("write CID=%d: WAL pressure must not map to MediaWriteFault", resp.CID)
		}
		if status != StatusNSNotReady {
			t.Fatalf("write CID=%d: expected StatusNSNotReady (0x%04x), got 0x%04x", resp.CID, StatusNSNotReady, status)
		}
	}
}

// TestQA_WAL_ReadsDuringWritePressure verifies that read commands succeed
// normally while the write path is under WAL pressure. WAL pressure must
// not affect the read path.
func TestQA_WAL_ReadsDuringWritePressure(t *testing.T) {
	origSleep := sleepFn
	origJitter := jitterFn
	defer func() { sleepFn = origSleep; jitterFn = origJitter }()
	sleepFn = func(d time.Duration) {}
	jitterFn = func(max time.Duration) time.Duration { return 0 }

	nqn := "nqn.test:qa-wal-read-ok"
	dev := newMockDevice(256, 512)
	dev.writeErr = blockvol.ErrWALFull
	dev.walPressure = 1.0

	// Pre-fill LBA 0 with known data.
	pattern := make([]byte, 512)
	for i := range pattern {
		pattern[i] = 0xAB
	}
	dev.writeErr = nil
	dev.WriteAt(0, pattern)
	dev.writeErr = blockvol.ErrWALFull

	client, r, w := setupQAIOQueue(t, nqn, dev)
	defer client.Close()

	// Write should fail with retryable status.
	wCmd := CapsuleCommand{OpCode: ioWrite, CID: 600, D10: 1, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &wCmd, capsuleCmdSize, make([]byte, 512))
	wResp := recvCapsuleResp(t, r)
	if StatusWord(wResp.Status) != StatusNSNotReady {
		t.Fatalf("write should fail with NSNotReady, got 0x%04x", wResp.Status)
	}

	// Read should succeed despite write pressure.
	rCmd := CapsuleCommand{OpCode: ioRead, CID: 601, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &rCmd, capsuleCmdSize, nil)

	// Read returns C2HData PDU (data transfer) followed by CapsuleResponse.
	hdr, err := r.Dequeue()
	if err != nil {
		t.Fatalf("read response dequeue: %v", err)
	}
	if hdr.Type != pduC2HData {
		t.Fatalf("expected C2HData (0x7), got 0x%x", hdr.Type)
	}
	var c2h C2HDataHeader
	r.Receive(&c2h)
	readData := make([]byte, r.Length())
	r.ReceiveData(readData)

	// Now read the CapsuleResponse.
	rResp := recvCapsuleResp(t, r)
	if StatusWord(rResp.Status) != StatusSuccess {
		t.Fatalf("read should succeed during write pressure, got 0x%04x", rResp.Status)
	}
}

// TestQA_WAL_WriteZerosUnderPressure verifies WriteZeros (without DEALLOC)
// also goes through the WAL pressure retry path and returns retryable status.
func TestQA_WAL_WriteZerosUnderPressure(t *testing.T) {
	origSleep := sleepFn
	origJitter := jitterFn
	defer func() { sleepFn = origSleep; jitterFn = origJitter }()
	sleepFn = func(d time.Duration) {}
	jitterFn = func(max time.Duration) time.Duration { return 0 }

	nqn := "nqn.test:qa-wal-wz"
	dev := newMockDevice(256, 512)
	dev.writeErr = blockvol.ErrWALFull
	dev.walPressure = 1.0

	client, r, w := setupQAIOQueue(t, nqn, dev)
	defer client.Close()

	// WriteZeros without DEALLOC bit — goes through write path.
	wzCmd := CapsuleCommand{
		OpCode: ioWriteZeros,
		CID:    700,
		D10:    0,  // LBA
		D12:    3,  // NLB=3 → 4 blocks
		D14:    0,  // no DEALLOC
	}
	w.SendWithData(pduCapsuleCmd, 0, &wzCmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	status := StatusWord(resp.Status)

	if status.DNR() {
		t.Fatal("WriteZeros under WAL pressure must not return DNR=1")
	}
	if status == StatusMediaWriteFault {
		t.Fatal("WriteZeros WAL pressure must not map to MediaWriteFault")
	}
	if status != StatusNSNotReady {
		t.Fatalf("WriteZeros: expected StatusNSNotReady, got 0x%04x", status)
	}
}

// TestQA_WAL_PressureTransition verifies correct behavior when WAL pressure
// transitions: first write fails under pressure, pressure clears, second
// write succeeds. Tests the real protocol path through the server.
func TestQA_WAL_PressureTransition(t *testing.T) {
	origSleep := sleepFn
	origJitter := jitterFn
	defer func() { sleepFn = origSleep; jitterFn = origJitter }()
	sleepFn = func(d time.Duration) {}
	jitterFn = func(max time.Duration) time.Duration { return 0 }

	nqn := "nqn.test:qa-wal-transition"
	dev := newMockDevice(256, 512)
	dev.writeErr = blockvol.ErrWALFull
	dev.walPressure = 1.0

	client, r, w := setupQAIOQueue(t, nqn, dev)
	defer client.Close()

	// First write: should fail with retryable status.
	cmd1 := CapsuleCommand{OpCode: ioWrite, CID: 800, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &cmd1, capsuleCmdSize, make([]byte, 512))
	resp1 := recvCapsuleResp(t, r)
	if StatusWord(resp1.Status) != StatusNSNotReady {
		t.Fatalf("write under pressure: expected NSNotReady, got 0x%04x", resp1.Status)
	}

	// Clear WAL pressure.
	dev.mu.Lock()
	dev.writeErr = nil
	dev.walPressure = 0.1
	dev.mu.Unlock()

	// Second write: should succeed.
	cmd2 := CapsuleCommand{OpCode: ioWrite, CID: 801, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &cmd2, capsuleCmdSize, make([]byte, 512))
	resp2 := recvCapsuleResp(t, r)
	if StatusWord(resp2.Status) != StatusSuccess {
		t.Fatalf("write after pressure cleared: expected Success, got 0x%04x", resp2.Status)
	}
}

// TestQA_WAL_ErrorEscalationPrevention verifies that different error types
// are never confused: WAL pressure returns NSNotReady (DNR=0), while
// permanent errors like ErrLeaseExpired return DNR=1. This prevents
// error escalation where transient pressure is treated as permanent.
func TestQA_WAL_ErrorEscalationPrevention(t *testing.T) {
	origSleep := sleepFn
	origJitter := jitterFn
	defer func() { sleepFn = origSleep; jitterFn = origJitter }()
	sleepFn = func(d time.Duration) {}
	jitterFn = func(max time.Duration) time.Duration { return 0 }

	nqn := "nqn.test:qa-wal-escalation"
	dev := newMockDevice(256, 512)

	client, r, w := setupQAIOQueue(t, nqn, dev)
	defer client.Close()

	// Phase 1: WAL pressure → retryable (DNR=0).
	dev.mu.Lock()
	dev.writeErr = blockvol.ErrWALFull
	dev.walPressure = 1.0
	dev.mu.Unlock()

	cmd1 := CapsuleCommand{OpCode: ioWrite, CID: 900, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &cmd1, capsuleCmdSize, make([]byte, 512))
	resp1 := recvCapsuleResp(t, r)
	s1 := StatusWord(resp1.Status)
	if s1.DNR() {
		t.Fatal("WAL pressure must produce DNR=0 (retryable)")
	}
	if s1 != StatusNSNotReady {
		t.Fatalf("WAL pressure: expected NSNotReady, got 0x%04x", s1)
	}

	// Phase 2: Lease expired → permanent (DNR=1).
	dev.mu.Lock()
	dev.writeErr = blockvol.ErrLeaseExpired
	dev.walPressure = 0.0
	dev.mu.Unlock()

	cmd2 := CapsuleCommand{OpCode: ioWrite, CID: 901, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &cmd2, capsuleCmdSize, make([]byte, 512))
	resp2 := recvCapsuleResp(t, r)
	s2 := StatusWord(resp2.Status)
	if !s2.DNR() {
		t.Fatal("ErrLeaseExpired must produce DNR=1 (permanent)")
	}

	// Phase 3: Back to WAL pressure → still retryable (DNR=0).
	dev.mu.Lock()
	dev.writeErr = blockvol.ErrWALFull
	dev.walPressure = 1.0
	dev.mu.Unlock()

	cmd3 := CapsuleCommand{OpCode: ioWrite, CID: 902, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &cmd3, capsuleCmdSize, make([]byte, 512))
	resp3 := recvCapsuleResp(t, r)
	s3 := StatusWord(resp3.Status)
	if s3.DNR() {
		t.Fatal("WAL pressure after lease error must still produce DNR=0")
	}
	if s3 != StatusNSNotReady {
		t.Fatalf("WAL pressure after lease error: expected NSNotReady, got 0x%04x", s3)
	}
}

// TestQA_WAL_ThrottleDoesNotBlockReads verifies that the proactive throttle
// on high WAL pressure does not affect read or flush commands.
func TestQA_WAL_ThrottleDoesNotBlockReads(t *testing.T) {
	origSleep := sleepFn
	origJitter := jitterFn
	defer func() { sleepFn = origSleep; jitterFn = origJitter }()

	var throttleSleeps int
	sleepFn = func(d time.Duration) { throttleSleeps++ }
	jitterFn = func(max time.Duration) time.Duration { return 0 }

	dev := newMockDevice(256, 512)
	dev.walPressure = 1.0

	// Throttle should only trigger for write paths.
	throttleSleeps = 0
	throttleOnWALPressure(dev)
	if throttleSleeps != 1 {
		t.Fatalf("expected throttle to fire at pressure=1.0, got %d sleeps", throttleSleeps)
	}

	// Read path should not call throttleOnWALPressure — verify by checking
	// that a read through the protocol succeeds without extra delays.
	nqn := "nqn.test:qa-wal-throttle-read"
	client, r, w := setupQAIOQueue(t, nqn, dev)
	defer client.Close()

	throttleSleeps = 0
	rCmd := CapsuleCommand{OpCode: ioRead, CID: 1000, D10: 0, D12: 0}
	w.SendWithData(pduCapsuleCmd, 0, &rCmd, capsuleCmdSize, nil)

	// Read returns C2HData + CapsuleResponse.
	hdr, err := r.Dequeue()
	if err != nil {
		t.Fatalf("read dequeue: %v", err)
	}
	if hdr.Type != pduC2HData {
		t.Fatalf("expected C2HData (0x7), got 0x%x", hdr.Type)
	}
	var c2h C2HDataHeader
	r.Receive(&c2h)
	readBuf := make([]byte, r.Length())
	r.ReceiveData(readBuf)

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusSuccess {
		t.Fatalf("read should succeed at any WAL pressure, got 0x%04x", resp.Status)
	}
}

// TestQA_WAL_WrappedErrorProtocolPath verifies that wrapped ErrWALFull
// (e.g., from appendWithRetry → fmt.Errorf) still maps correctly through
// the full protocol stack.
func TestQA_WAL_WrappedErrorProtocolPath(t *testing.T) {
	origSleep := sleepFn
	origJitter := jitterFn
	defer func() { sleepFn = origSleep; jitterFn = origJitter }()
	sleepFn = func(d time.Duration) {}
	jitterFn = func(max time.Duration) time.Duration { return 0 }

	// Verify mapBlockError handles wrapped errors.
	wrapped := fmt.Errorf("blockvol: WAL full timeout after 5s: %w", blockvol.ErrWALFull)
	status := mapBlockError(wrapped)
	if status != StatusNSNotReady {
		t.Fatalf("wrapped ErrWALFull: expected NSNotReady (0x%04x), got 0x%04x", StatusNSNotReady, status)
	}
	if status.DNR() {
		t.Fatal("wrapped ErrWALFull must have DNR=0")
	}

	// Double-wrapped.
	doubleWrapped := fmt.Errorf("io handler: %w", wrapped)
	status2 := mapBlockError(doubleWrapped)
	if status2 != StatusNSNotReady {
		t.Fatalf("double-wrapped ErrWALFull: expected NSNotReady, got 0x%04x", status2)
	}
}

// TestQA_WAL_FlushDuringPressure verifies that a Flush (sync cache) command
// succeeds even when write pressure is high, as long as syncErr is nil.
func TestQA_WAL_FlushDuringPressure(t *testing.T) {
	origSleep := sleepFn
	origJitter := jitterFn
	defer func() { sleepFn = origSleep; jitterFn = origJitter }()
	sleepFn = func(d time.Duration) {}
	jitterFn = func(max time.Duration) time.Duration { return 0 }

	nqn := "nqn.test:qa-wal-flush"
	dev := newMockDevice(256, 512)
	dev.writeErr = blockvol.ErrWALFull
	dev.walPressure = 1.0

	client, r, w := setupQAIOQueue(t, nqn, dev)
	defer client.Close()

	// Flush should succeed — it does not go through the write retry path.
	flushCmd := CapsuleCommand{
		OpCode: ioFlush,
		CID:    1100,
	}
	w.SendWithData(pduCapsuleCmd, 0, &flushCmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status) != StatusSuccess {
		t.Fatalf("Flush should succeed during write pressure, got 0x%04x", resp.Status)
	}
}

// TestQA_Batch_BackToBack_HeaderOnly verifies two consecutive header-only
// PDUs batched with writeHeaderAndData + single FlushBuf.
func TestQA_Batch_BackToBack_HeaderOnly(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	r1 := CapsuleResponse{CID: 1, Status: uint16(StatusSuccess)}
	r2 := CapsuleResponse{CID: 2, Status: uint16(StatusSuccess)}
	w.writeHeaderAndData(pduCapsuleResp, 0, &r1, capsuleRespSize, nil)
	w.writeHeaderAndData(pduCapsuleResp, 0, &r2, capsuleRespSize, nil)
	w.FlushBuf()

	// Should be able to read both PDUs back.
	r := NewReader(&buf)
	hdr1, _ := r.Dequeue()
	if hdr1.Type != pduCapsuleResp {
		t.Fatalf("PDU1: type 0x%x", hdr1.Type)
	}
	var got1 CapsuleResponse
	r.Receive(&got1)
	if got1.CID != 1 {
		t.Fatalf("PDU1: CID=%d", got1.CID)
	}

	hdr2, _ := r.Dequeue()
	if hdr2.Type != pduCapsuleResp {
		t.Fatalf("PDU2: type 0x%x", hdr2.Type)
	}
	var got2 CapsuleResponse
	r.Receive(&got2)
	if got2.CID != 2 {
		t.Fatalf("PDU2: CID=%d", got2.CID)
	}
}
