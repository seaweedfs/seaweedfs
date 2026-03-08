package nvme

// Adversarial / QA tests for NVMe/TCP target.
// Covers: malformed wire, protocol state violations, IO boundary attacks,
// ANA/fencing transitions, admin command edge cases, server lifecycle races,
// multi-block chunking, SQHD wraparound, concurrent stress.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
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
		D10:    0xDEAD, // invalid register offset
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
