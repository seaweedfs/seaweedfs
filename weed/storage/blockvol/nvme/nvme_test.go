package nvme

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockerr"
)

// ============================================================
// Mock BlockDevice
// ============================================================

type mockBlockDevice struct {
	mu        sync.Mutex
	data      []byte
	blockSize uint32
	healthy   bool
	anaState  uint8
	readErr   error
	writeErr  error
	syncErr   error
	trimErr   error
}

func newMockDevice(blocks int, blockSize uint32) *mockBlockDevice {
	return &mockBlockDevice{
		data:      make([]byte, int(blockSize)*blocks),
		blockSize: blockSize,
		healthy:   true,
		anaState:  anaOptimized,
	}
}

func (m *mockBlockDevice) ReadAt(lba uint64, length uint32) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.readErr != nil {
		return nil, m.readErr
	}
	off := lba * uint64(m.blockSize)
	if off+uint64(length) > uint64(len(m.data)) {
		return nil, errors.New("read out of range")
	}
	buf := make([]byte, length)
	copy(buf, m.data[off:off+uint64(length)])
	return buf, nil
}

func (m *mockBlockDevice) WriteAt(lba uint64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return m.writeErr
	}
	off := lba * uint64(m.blockSize)
	if off+uint64(len(data)) > uint64(len(m.data)) {
		return errors.New("write out of range")
	}
	copy(m.data[off:], data)
	return nil
}

func (m *mockBlockDevice) Trim(lba uint64, length uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.trimErr != nil {
		return m.trimErr
	}
	off := lba * uint64(m.blockSize)
	if off+uint64(length) > uint64(len(m.data)) {
		return errors.New("trim out of range")
	}
	for i := uint64(0); i < uint64(length); i++ {
		m.data[off+i] = 0
	}
	return nil
}

func (m *mockBlockDevice) SyncCache() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.syncErr
}

func (m *mockBlockDevice) BlockSize() uint32   { return m.blockSize }
func (m *mockBlockDevice) VolumeSize() uint64   { return uint64(len(m.data)) }
func (m *mockBlockDevice) IsHealthy() bool      { return m.healthy }
func (m *mockBlockDevice) ANAState() uint8      { return m.anaState }
func (m *mockBlockDevice) ANAGroupID() uint16   { return 1 }
func (m *mockBlockDevice) DeviceNGUID() [16]byte { return [16]byte{0x60, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15} }

// ============================================================
// Protocol Marshal/Unmarshal Tests
// ============================================================

func TestCommonHeader_MarshalRoundTrip(t *testing.T) {
	hdr := CommonHeader{
		Type:         pduCapsuleCmd,
		Flags:        0x04,
		HeaderLength: 72,
		DataOffset:   72,
		DataLength:   200,
	}
	buf := make([]byte, commonHeaderSize)
	hdr.Marshal(buf)

	var got CommonHeader
	got.Unmarshal(buf)
	if got != hdr {
		t.Fatalf("got %+v, want %+v", got, hdr)
	}
}

func TestCapsuleCommand_MarshalRoundTrip(t *testing.T) {
	cmd := CapsuleCommand{
		OpCode: ioRead,
		CID:    42,
		NSID:   1,
		D10:    0x100,
		D11:    0,
		D12:    7, // 8 blocks (0-based)
	}
	buf := make([]byte, capsuleCmdSize)
	cmd.Marshal(buf)

	var got CapsuleCommand
	got.Unmarshal(buf)
	if got.OpCode != cmd.OpCode || got.CID != cmd.CID || got.D10 != cmd.D10 || got.D12 != cmd.D12 {
		t.Fatalf("got %+v", got)
	}
	if got.Lba() != 0x100 {
		t.Fatalf("Lba() = %d, want 256", got.Lba())
	}
	if got.LbaLength() != 8 {
		t.Fatalf("LbaLength() = %d, want 8", got.LbaLength())
	}
}

func TestCapsuleResponse_MarshalRoundTrip(t *testing.T) {
	resp := CapsuleResponse{
		DW0:     0x12345678,
		DW1:     0xABCD,
		SQHD:    5,
		QueueID: 0,
		CID:     42,
		Status:  uint16(StatusSuccess),
	}
	buf := make([]byte, capsuleRespSize)
	resp.Marshal(buf)

	var got CapsuleResponse
	got.Unmarshal(buf)
	if got.DW0 != resp.DW0 || got.SQHD != resp.SQHD || got.CID != resp.CID || got.Status != resp.Status {
		t.Fatalf("got %+v, want %+v", got, resp)
	}
}

func TestICRequest_MarshalRoundTrip(t *testing.T) {
	req := ICRequest{
		PDUFormatVersion: 0x0100,
		PDUDataAlignment: 2,
		PDUMaxR2T:        4,
	}
	buf := make([]byte, icBodySize)
	req.Marshal(buf)

	var got ICRequest
	got.Unmarshal(buf)
	if got.PDUFormatVersion != req.PDUFormatVersion || got.PDUMaxR2T != req.PDUMaxR2T {
		t.Fatalf("got %+v, want %+v", got, req)
	}
}

func TestICResponse_MarshalRoundTrip(t *testing.T) {
	resp := ICResponse{MaxH2CDataLength: maxH2CDataLen}
	buf := make([]byte, icBodySize)
	resp.Marshal(buf)

	var got ICResponse
	got.Unmarshal(buf)
	if got.MaxH2CDataLength != maxH2CDataLen {
		t.Fatalf("MaxH2CDataLength = %d, want %d", got.MaxH2CDataLength, maxH2CDataLen)
	}
}

func TestC2HDataHeader_MarshalRoundTrip(t *testing.T) {
	hdr := C2HDataHeader{
		CCCID: 7,
		DATAO: 1024,
		DATAL: 4096,
	}
	buf := make([]byte, c2hDataHdrSize)
	hdr.Marshal(buf)

	var got C2HDataHeader
	got.Unmarshal(buf)
	if got.CCCID != 7 || got.DATAO != 1024 || got.DATAL != 4096 {
		t.Fatalf("got %+v", got)
	}
}

func TestConnectData_MarshalRoundTrip(t *testing.T) {
	cd := ConnectData{
		HostID:  [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		CNTLID:  0xFFFF,
		SubNQN:  "nqn.2024-01.com.seaweedfs:vol.test",
		HostNQN: "nqn.2024-01.com.seaweedfs:host",
	}
	buf := make([]byte, connectDataSize)
	cd.Marshal(buf)

	var got ConnectData
	got.Unmarshal(buf)
	if got.SubNQN != cd.SubNQN || got.HostNQN != cd.HostNQN || got.CNTLID != cd.CNTLID {
		t.Fatalf("got SubNQN=%q HostNQN=%q CNTLID=%d", got.SubNQN, got.HostNQN, got.CNTLID)
	}
	if got.HostID != cd.HostID {
		t.Fatalf("HostID mismatch")
	}
}

func TestStatusWord_Encoding(t *testing.T) {
	tests := []struct {
		name string
		sct  uint8
		sc   uint8
		dnr  bool
		want StatusWord
	}{
		{"Success", 0, 0, false, StatusSuccess},
		{"InvalidOpcode_DNR", 0, 0x01, true, StatusInvalidOpcode},
		{"InvalidField_DNR", 0, 0x02, true, StatusInvalidField},
		{"InternalError", 0, 0x06, false, StatusInternalError},
		{"InternalError_DNR", 0, 0x06, true, StatusInternalErrorDNR},
		{"NSNotReady", 0, 0x82, false, StatusNSNotReady},
		{"NSNotReady_DNR", 0, 0x82, true, StatusNSNotReadyDNR},
		{"LBAOutOfRange", 0, 0x80, true, StatusLBAOutOfRange},
		{"MediaWriteFault", 2, 0x80, false, StatusMediaWriteFault},
		{"MediaReadError", 2, 0x81, false, StatusMediaReadError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MakeStatus(tt.sct, tt.sc, tt.dnr)
			if got != tt.want {
				t.Fatalf("MakeStatus(%d,%d,%v) = 0x%04x, want 0x%04x", tt.sct, tt.sc, tt.dnr, got, tt.want)
			}
			if got.SCT() != tt.sct {
				t.Fatalf("SCT() = %d, want %d", got.SCT(), tt.sct)
			}
			if got.SC() != tt.sc {
				t.Fatalf("SC() = 0x%02x, want 0x%02x", got.SC(), tt.sc)
			}
			if got.DNR() != tt.dnr {
				t.Fatalf("DNR() = %v, want %v", got.DNR(), tt.dnr)
			}
		})
	}
}

func TestStatusWord_IsError(t *testing.T) {
	if StatusSuccess.IsError() {
		t.Fatal("Success should not be error")
	}
	if !StatusInvalidOpcode.IsError() {
		t.Fatal("InvalidOpcode should be error")
	}
}

// ============================================================
// Wire Reader/Writer Tests
// ============================================================

func TestWire_WriteReadRoundTrip_HeaderOnly(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	r := NewReader(&buf)

	resp := ICResponse{MaxH2CDataLength: maxH2CDataLen}
	if err := w.SendHeaderOnly(pduICResp, &resp, icBodySize); err != nil {
		t.Fatal(err)
	}

	hdr, err := r.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Type != pduICResp {
		t.Fatalf("type = 0x%x, want 0x%x", hdr.Type, pduICResp)
	}
	if hdr.HeaderLength != icHdrLen {
		t.Fatalf("HeaderLength = %d, want %d", hdr.HeaderLength, icHdrLen)
	}
	if hdr.DataOffset != 0 {
		t.Fatalf("DataOffset = %d, want 0", hdr.DataOffset)
	}

	var got ICResponse
	if err := r.Receive(&got); err != nil {
		t.Fatal(err)
	}
	if got.MaxH2CDataLength != maxH2CDataLen {
		t.Fatalf("MaxH2CDataLength = %d", got.MaxH2CDataLength)
	}
}

func TestWire_WriteReadRoundTrip_WithData(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	r := NewReader(&buf)

	c2h := C2HDataHeader{CCCID: 5, DATAO: 0, DATAL: 4096}
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i & 0xFF)
	}

	if err := w.SendWithData(pduC2HData, c2hFlagLast, &c2h, c2hDataHdrSize, payload); err != nil {
		t.Fatal(err)
	}

	hdr, err := r.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Type != pduC2HData {
		t.Fatalf("type = 0x%x", hdr.Type)
	}
	if hdr.Flags != c2hFlagLast {
		t.Fatalf("flags = 0x%x", hdr.Flags)
	}
	if hdr.DataOffset != c2hDataHdrLen {
		t.Fatalf("DataOffset = %d, want %d", hdr.DataOffset, c2hDataHdrLen)
	}

	var gotHdr C2HDataHeader
	if err := r.Receive(&gotHdr); err != nil {
		t.Fatal(err)
	}
	if gotHdr.CCCID != 5 || gotHdr.DATAL != 4096 {
		t.Fatalf("got %+v", gotHdr)
	}

	dataLen := r.Length()
	if dataLen != 4096 {
		t.Fatalf("Length() = %d", dataLen)
	}

	gotData := make([]byte, dataLen)
	if err := r.ReceiveData(gotData); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotData, payload) {
		t.Fatal("payload mismatch")
	}
}

func TestWire_MultiPDU(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	r := NewReader(&buf)

	// Write 3 header-only PDUs
	for i := 0; i < 3; i++ {
		resp := CapsuleResponse{CID: uint16(i), Status: 0}
		if err := w.SendHeaderOnly(pduCapsuleResp, &resp, capsuleRespSize); err != nil {
			t.Fatal(err)
		}
	}

	// Read all 3
	for i := 0; i < 3; i++ {
		hdr, err := r.Dequeue()
		if err != nil {
			t.Fatalf("pdu %d: %v", i, err)
		}
		if hdr.Type != pduCapsuleResp {
			t.Fatalf("pdu %d: type 0x%x", i, hdr.Type)
		}
		var resp CapsuleResponse
		if err := r.Receive(&resp); err != nil {
			t.Fatalf("pdu %d: %v", i, err)
		}
		if resp.CID != uint16(i) {
			t.Fatalf("pdu %d: CID=%d", i, resp.CID)
		}
	}
}

func TestWire_PayloadSize(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	r := NewReader(&buf)

	// Header-only: Length() should be 0
	resp := CapsuleResponse{}
	w.SendHeaderOnly(pduCapsuleResp, &resp, capsuleRespSize)
	r.Dequeue()
	r.Receive(&resp)
	if r.Length() != 0 {
		t.Fatalf("expected 0 length for header-only, got %d", r.Length())
	}
}

func TestWire_CapsuleCmdWithData(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	r := NewReader(&buf)

	cmd := CapsuleCommand{OpCode: ioWrite, CID: 10, D10: 0, D12: 0}
	data := []byte("hello world block data 123")

	if err := w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, data); err != nil {
		t.Fatal(err)
	}

	hdr, err := r.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.DataOffset != capsuleCmdHdrLen {
		t.Fatalf("DataOffset = %d", hdr.DataOffset)
	}

	var gotCmd CapsuleCommand
	r.Receive(&gotCmd)
	if gotCmd.CID != 10 {
		t.Fatalf("CID = %d", gotCmd.CID)
	}

	payloadLen := r.Length()
	if payloadLen != uint32(len(data)) {
		t.Fatalf("Length() = %d, want %d", payloadLen, len(data))
	}

	gotData := make([]byte, payloadLen)
	r.ReceiveData(gotData)
	if !bytes.Equal(gotData, data) {
		t.Fatal("data mismatch")
	}
}

// ============================================================
// Controller + Fabric Tests (using pipe connections)
// ============================================================

func pipeConn() (client, server net.Conn) {
	s, c := net.Pipe()
	return c, s
}

// sendICReq writes an ICRequest PDU to the writer.
func sendICReq(w *Writer) error {
	req := ICRequest{}
	return w.SendHeaderOnly(pduICReq, &req, icBodySize)
}

// recvICResp reads and validates an ICResponse PDU.
func recvICResp(t *testing.T, r *Reader) {
	t.Helper()
	hdr, err := r.Dequeue()
	if err != nil {
		t.Fatalf("ICResp dequeue: %v", err)
	}
	if hdr.Type != pduICResp {
		t.Fatalf("expected ICResp, got 0x%x", hdr.Type)
	}
	var resp ICResponse
	if err := r.Receive(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.MaxH2CDataLength != maxH2CDataLen {
		t.Fatalf("MaxH2CDataLength = %d", resp.MaxH2CDataLength)
	}
}

// sendConnect sends a Fabric Connect capsule with inline ConnectData.
func sendConnect(w *Writer, queueID, queueSize uint16, kato uint32, subNQN, hostNQN string, cntlID uint16) error {
	cmd := CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcConnect,
		CID:    0,
		D10:    uint32(queueID) << 16,
		D11:    uint32(queueSize - 1),
		D12:    kato,
	}
	cd := ConnectData{
		CNTLID:  cntlID,
		SubNQN:  subNQN,
		HostNQN: hostNQN,
	}
	payload := make([]byte, connectDataSize)
	cd.Marshal(payload)
	return w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, payload)
}

// recvCapsuleResp reads a CapsuleResponse and returns it.
func recvCapsuleResp(t *testing.T, r *Reader) CapsuleResponse {
	t.Helper()
	hdr, err := r.Dequeue()
	if err != nil {
		t.Fatalf("CapsuleResp dequeue: %v", err)
	}
	if hdr.Type != pduCapsuleResp {
		t.Fatalf("expected CapsuleResp (0x5), got 0x%x", hdr.Type)
	}
	var resp CapsuleResponse
	if err := r.Receive(&resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

// setupAdminSession performs IC + admin Connect on a pipe.
func setupAdminSession(t *testing.T, subNQN string) (client net.Conn, clientR *Reader, clientW *Writer, ctrl *Controller, cntlID uint16) {
	t.Helper()

	clientConn, serverConn := pipeConn()
	dev := newMockDevice(1024, 512)
	srv := NewServer(Config{
		Enabled:     true,
		ListenAddr:  "127.0.0.1:0",
		MaxIOQueues: 4,
	})
	srv.AddVolume(subNQN, dev, dev.DeviceNGUID())

	ctrl = newController(serverConn, srv)

	// Run controller in background
	go ctrl.Serve()

	clientR = NewReader(clientConn)
	clientW = NewWriter(clientConn)

	// IC handshake
	if err := sendICReq(clientW); err != nil {
		t.Fatal(err)
	}
	recvICResp(t, clientR)

	// Admin Connect
	if err := sendConnect(clientW, 0, 64, 60000, subNQN, "host-nqn", 0xFFFF); err != nil {
		t.Fatal(err)
	}
	resp := recvCapsuleResp(t, clientR)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("Connect failed: 0x%04x", resp.Status)
	}
	cntlID = uint16(resp.DW0)

	return clientConn, clientR, clientW, ctrl, cntlID
}

func TestController_ICHandshake(t *testing.T) {
	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	ctrl := newController(serverConn, srv)

	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)

	if err := sendICReq(w); err != nil {
		t.Fatal(err)
	}
	recvICResp(t, r)

	clientConn.Close()
}

func TestController_AdminConnect(t *testing.T) {
	nqn := "nqn.test:vol.t1"
	client, _, _, _, cntlID := setupAdminSession(t, nqn)
	defer client.Close()

	if cntlID == 0 {
		t.Fatal("expected non-zero CNTLID")
	}
}

func TestController_ConnectUnknownNQN(t *testing.T) {
	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	ctrl := newController(serverConn, srv)
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)

	sendICReq(w)
	recvICResp(t, r)

	sendConnect(w, 0, 64, 0, "nqn.unknown", "host", 0xFFFF)
	resp := recvCapsuleResp(t, r)
	if !StatusWord(resp.Status).IsError() {
		t.Fatal("expected error for unknown NQN")
	}

	clientConn.Close()
}

func TestController_PropertyGetCAP(t *testing.T) {
	nqn := "nqn.test:propget"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	// PropertyGet CAP (8 bytes)
	cmd := CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcPropertyGet,
		CID:    1,
		D10:    propCAP,
		D11:    1, // 8-byte
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("PropertyGet CAP failed: 0x%04x", resp.Status)
	}
	cap := uint64(resp.DW0) | uint64(resp.DW1)<<32
	// MQES should be 63
	if cap&0xFFFF != 63 {
		t.Fatalf("MQES = %d, want 63", cap&0xFFFF)
	}

	client.Close()
}

func TestController_PropertySetCC_EN(t *testing.T) {
	nqn := "nqn.test:propset"
	client, r, w, ctrl, _ := setupAdminSession(t, nqn)
	defer client.Close()

	// PropertySet CC.EN=1
	cmd := CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcPropertySet,
		CID:    2,
		D10:    propCC,
		D14:    1, // CC.EN=1
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("PropertySet CC failed: 0x%04x", resp.Status)
	}

	// Verify CSTS.RDY via PropertyGet
	cmd2 := CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcPropertyGet,
		CID:    3,
		D10:    propCSTS,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd2, capsuleCmdSize, nil)
	resp2 := recvCapsuleResp(t, r)
	if resp2.DW0&1 != 1 {
		t.Fatal("CSTS.RDY not set after CC.EN=1")
	}

	_ = ctrl
	client.Close()
}

// ============================================================
// Identify Tests
// ============================================================

func TestIdentify_Controller(t *testing.T) {
	nqn := "nqn.test:id-ctrl"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: adminIdentify,
		CID:    10,
		D10:    uint32(cnsIdentifyController),
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	// Expect C2HData + CapsuleResp
	hdr, err := r.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Type != pduC2HData {
		t.Fatalf("expected C2HData, got 0x%x", hdr.Type)
	}

	var c2h C2HDataHeader
	r.Receive(&c2h)
	data := make([]byte, r.Length())
	r.ReceiveData(data)

	if len(data) != identifySize {
		t.Fatalf("identify data size = %d, want %d", len(data), identifySize)
	}

	// SQES
	if data[512] != 0x66 {
		t.Fatalf("SQES = 0x%02x, want 0x66", data[512])
	}
	// CQES
	if data[513] != 0x44 {
		t.Fatalf("CQES = 0x%02x, want 0x44", data[513])
	}
	// VWC
	if data[525] != 0x01 {
		t.Fatalf("VWC = 0x%02x, want 0x01", data[525])
	}
	// MDTS
	if data[77] != 3 {
		t.Fatalf("MDTS = %d, want 3", data[77])
	}
	// SubNQN check
	subNQN := string(bytes.TrimRight(data[768:1024], " "))
	if subNQN != nqn {
		t.Fatalf("SubNQN = %q, want %q", subNQN, nqn)
	}
	// IOCCSZ
	ioccsz := binary.LittleEndian.Uint32(data[1792:])
	if ioccsz != 4 {
		t.Fatalf("IOCCSZ = %d, want 4", ioccsz)
	}
	// IORCSZ
	iorcsz := binary.LittleEndian.Uint32(data[1796:])
	if iorcsz != 1 {
		t.Fatalf("IORCSZ = %d, want 1", iorcsz)
	}
	// NN
	nn := binary.LittleEndian.Uint32(data[516:])
	if nn != 1 {
		t.Fatalf("NN = %d, want 1", nn)
	}

	// Read trailing CapsuleResp
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("Identify Ctrl response error: 0x%04x", resp.Status)
	}

	client.Close()
}

func TestIdentify_Namespace_512B(t *testing.T) {
	testIdentifyNS(t, 512)
}

func TestIdentify_Namespace_4K(t *testing.T) {
	testIdentifyNS(t, 4096)
}

func testIdentifyNS(t *testing.T, blockSize uint32) {
	t.Helper()
	nqn := "nqn.test:id-ns"

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	dev := newMockDevice(1024, blockSize)
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
		OpCode: adminIdentify,
		CID:    11,
		D10:    uint32(cnsIdentifyNamespace),
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

	expectedNSZE := dev.VolumeSize() / uint64(blockSize)
	nsze := binary.LittleEndian.Uint64(data[0:])
	if nsze != expectedNSZE {
		t.Fatalf("NSZE = %d, want %d", nsze, expectedNSZE)
	}

	// LBAF[0]: bits 23:16 = log2(blockSize)
	lbaf0 := binary.LittleEndian.Uint32(data[128:])
	var expectedLBADS uint8
	switch blockSize {
	case 512:
		expectedLBADS = 9
	case 4096:
		expectedLBADS = 12
	}
	gotLBADS := uint8((lbaf0 >> 16) & 0xFF)
	if gotLBADS != expectedLBADS {
		t.Fatalf("LBADS = %d, want %d", gotLBADS, expectedLBADS)
	}

	recvCapsuleResp(t, r)
	clientConn.Close()
}

func TestIdentify_ActiveNSList(t *testing.T) {
	nqn := "nqn.test:nslist"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: adminIdentify,
		CID:    12,
		D10:    uint32(cnsActiveNSList),
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

	nsid := binary.LittleEndian.Uint32(data[0:])
	if nsid != 1 {
		t.Fatalf("NSID = %d, want 1", nsid)
	}

	recvCapsuleResp(t, r)
	client.Close()
}

func TestIdentify_NSDescriptors(t *testing.T) {
	nqn := "nqn.test:nsdesc"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: adminIdentify,
		CID:    13,
		D10:    uint32(cnsNSDescriptorList),
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

	// Type = 0x02 (NGUID), Length = 16
	if data[0] != 0x02 || data[1] != 16 {
		t.Fatalf("NS descriptor: type=0x%02x len=%d", data[0], data[1])
	}

	recvCapsuleResp(t, r)
	client.Close()
}

// ============================================================
// Admin Command Tests
// ============================================================

func TestAdmin_SetFeatures_NumQueues(t *testing.T) {
	nqn := "nqn.test:numq"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	// Request 8 queues
	cmd := CapsuleCommand{
		OpCode: adminSetFeatures,
		CID:    20,
		D10:    uint32(fidNumberOfQueues),
		D11:    7 | (7 << 16), // NCQR=7, NSQR=7 (both request 8, 0-based)
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("SetFeatures failed: 0x%04x", resp.Status)
	}

	// MaxIOQueues=4, so granted should be 4
	ncqr := resp.DW0 & 0xFFFF
	nsqr := resp.DW0 >> 16
	if ncqr != 3 || nsqr != 3 { // 0-based: 3 means 4 queues
		t.Fatalf("NCQR=%d NSQR=%d, want 3,3", ncqr, nsqr)
	}

	client.Close()
}

func TestAdmin_GetLogPage_SMART(t *testing.T) {
	nqn := "nqn.test:smart"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	// Request 512 bytes of SMART log (NUMD = 512/4 - 1 = 127)
	cmd := CapsuleCommand{
		OpCode: adminGetLogPage,
		CID:    21,
		D10:    uint32(logPageSMART) | (127 << 16),
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)

	hdr, _ := r.Dequeue()
	if hdr.Type != pduC2HData {
		t.Fatalf("expected C2HData, got 0x%x", hdr.Type)
	}
	var c2h C2HDataHeader
	r.Receive(&c2h)
	data := make([]byte, r.Length())
	r.ReceiveData(data)

	// Available Spare = 100%
	if data[3] != 100 {
		t.Fatalf("Available Spare = %d, want 100", data[3])
	}

	recvCapsuleResp(t, r)
	client.Close()
}

func TestAdmin_GetLogPage_ANA(t *testing.T) {
	nqn := "nqn.test:ana"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	// Request ANA log
	cmd := CapsuleCommand{
		OpCode: adminGetLogPage,
		CID:    22,
		D10:    uint32(logPageANA) | (9 << 16), // NUMD=9 → 40 bytes
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

	// ANA state at offset 32 should be Optimized (0x01)
	if data[32] != anaOptimized {
		t.Fatalf("ANA state = 0x%02x, want 0x%02x", data[32], anaOptimized)
	}

	// NSID at offset 36
	nsid := binary.LittleEndian.Uint32(data[36:])
	if nsid != 1 {
		t.Fatalf("NSID = %d", nsid)
	}

	recvCapsuleResp(t, r)
	client.Close()
}

func TestAdmin_KeepAlive(t *testing.T) {
	nqn := "nqn.test:ka"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: adminKeepAlive,
		CID:    23,
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("KeepAlive failed: 0x%04x", resp.Status)
	}

	client.Close()
}

func TestAdmin_GetFeatures(t *testing.T) {
	nqn := "nqn.test:getfeat"
	client, r, w, _, _ := setupAdminSession(t, nqn)
	defer client.Close()

	cmd := CapsuleCommand{
		OpCode: adminGetFeatures,
		CID:    24,
		D10:    uint32(fidNumberOfQueues),
	}
	w.SendWithData(pduCapsuleCmd, 0, &cmd, capsuleCmdSize, nil)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("GetFeatures failed: 0x%04x", resp.Status)
	}
	// Default granted = maxIOQueues=4, so response DW0 = 3 | (3<<16)
	ncqr := resp.DW0 & 0xFFFF
	nsqr := resp.DW0 >> 16
	if ncqr != 3 || nsqr != 3 {
		t.Fatalf("NCQR=%d NSQR=%d", ncqr, nsqr)
	}

	client.Close()
}

// ============================================================
// IO Command Tests
// ============================================================

// setupIOSession creates an admin session, sets up IO queue, and returns IO conn.
func setupIOSession(t *testing.T) (client net.Conn, r *Reader, w *Writer, dev *mockBlockDevice, cleanup func()) {
	t.Helper()
	nqn := "nqn.test:io"

	clientConn, serverConn := pipeConn()
	dev = newMockDevice(256, 512) // 256 blocks * 512 bytes = 128 KB
	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	ctrl := newController(serverConn, srv)
	go ctrl.Serve()

	r = NewReader(clientConn)
	w = NewWriter(clientConn)

	// IC
	sendICReq(w)
	recvICResp(t, r)

	// Admin Connect
	sendConnect(w, 0, 64, 0, nqn, "host", 0xFFFF)
	recvCapsuleResp(t, r)

	cleanup = func() { clientConn.Close() }
	return clientConn, r, w, dev, cleanup
}

func TestIO_ReadWrite(t *testing.T) {
	_, r, w, dev, cleanup := setupIOSession(t)
	defer cleanup()

	// Switch to IO queue by changing the controller's queueID.
	// In a real scenario, this would be a separate TCP connection.
	// For unit testing, we directly set the state after admin setup.

	// Write 1 block at LBA 0
	writeData := make([]byte, 512)
	for i := range writeData {
		writeData[i] = 0xAB
	}
	writeCapsule := CapsuleCommand{
		OpCode: ioWrite,
		CID:    100,
		NSID:   1,
		D10:    0, // LBA 0
		D12:    0, // 1 block (0-based)
	}
	w.SendWithData(pduCapsuleCmd, 0, &writeCapsule, capsuleCmdSize, writeData)

	// The controller dispatches to IO if queueID > 0.
	// Since we're on admin queue (QID=0), this will go through dispatchAdmin
	// and get "invalid opcode". For a proper IO test, we need to test the
	// IO functions directly.

	// Let's test the IO handlers directly instead:
	resp := recvCapsuleResp(t, r)
	// On admin queue, IO opcodes are invalid
	if !StatusWord(resp.Status).IsError() {
		t.Fatal("expected error for IO opcode on admin queue")
	}

	// Direct handler tests below...
	_ = dev
}

// TestIO_HandleRead tests the Read handler directly.
func TestIO_HandleRead(t *testing.T) {
	nqn := "nqn.test:io-read"
	dev := newMockDevice(256, 512)
	// Write known data
	for i := 0; i < 512; i++ {
		dev.data[i] = byte(i & 0xFF)
	}

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	ctrl.subsystem = srv.findSubsystem(nqn)
	ctrl.queueID = 1 // IO queue
	ctrl.queueSize = 64
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)

	// IC
	sendICReq(w)
	recvICResp(t, r)

	// Read 1 block at LBA 0
	readCmd := CapsuleCommand{
		OpCode: ioRead,
		CID:    200,
		D10:    0,
		D12:    0, // 1 block
	}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	// Expect C2HData + CapsuleResp
	hdr, err := r.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Type != pduC2HData {
		t.Fatalf("expected C2HData, got 0x%x", hdr.Type)
	}
	var c2h C2HDataHeader
	r.Receive(&c2h)
	data := make([]byte, r.Length())
	r.ReceiveData(data)

	if len(data) != 512 {
		t.Fatalf("read data len = %d", len(data))
	}
	for i := 0; i < 512; i++ {
		if data[i] != byte(i&0xFF) {
			t.Fatalf("data[%d] = 0x%02x, want 0x%02x", i, data[i], byte(i&0xFF))
		}
	}

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("Read response error: 0x%04x", resp.Status)
	}

	clientConn.Close()
}

func TestIO_HandleWrite(t *testing.T) {
	nqn := "nqn.test:io-write"
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

	// Write 1 block at LBA 5
	writeData := bytes.Repeat([]byte{0xCD}, 512)
	writeCmd := CapsuleCommand{
		OpCode: ioWrite,
		CID:    201,
		D10:    5,
		D12:    0,
	}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, writeData)

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("Write failed: 0x%04x", resp.Status)
	}

	// Verify data was written
	for i := 0; i < 512; i++ {
		if dev.data[5*512+i] != 0xCD {
			t.Fatalf("data at LBA 5 offset %d = 0x%02x", i, dev.data[5*512+i])
		}
	}

	clientConn.Close()
}

func TestIO_HandleFlush(t *testing.T) {
	nqn := "nqn.test:io-flush"
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

	flushCmd := CapsuleCommand{
		OpCode: ioFlush,
		CID:    202,
	}
	w.SendWithData(pduCapsuleCmd, 0, &flushCmd, capsuleCmdSize, nil)

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("Flush failed: 0x%04x", resp.Status)
	}

	clientConn.Close()
}

func TestIO_HandleWriteZeros_Trim(t *testing.T) {
	nqn := "nqn.test:io-wz"
	dev := newMockDevice(256, 512)
	// Fill LBA 10 with data
	for i := 0; i < 512; i++ {
		dev.data[10*512+i] = 0xFF
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

	// WriteZeros with DEALLOC bit at LBA 10, 1 block
	wzCmd := CapsuleCommand{
		OpCode: ioWriteZeros,
		CID:    203,
		D10:    10,
		D12:    0 | commandBitDeallocate, // 1 block + DEALLOC
	}
	w.SendWithData(pduCapsuleCmd, 0, &wzCmd, capsuleCmdSize, nil)

	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("WriteZeros failed: 0x%04x", resp.Status)
	}

	// Verify data was zeroed
	for i := 0; i < 512; i++ {
		if dev.data[10*512+i] != 0 {
			t.Fatalf("data at LBA 10 offset %d = 0x%02x, expected 0", i, dev.data[10*512+i])
		}
	}

	clientConn.Close()
}

func TestIO_ReadOutOfBounds(t *testing.T) {
	nqn := "nqn.test:io-oob"
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

	// Read past end: LBA 255, 2 blocks (only 256 blocks total)
	readCmd := CapsuleCommand{
		OpCode: ioRead,
		CID:    204,
		D10:    255,
		D12:    1, // 2 blocks (0-based)
	}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	resp := recvCapsuleResp(t, r)
	status := StatusWord(resp.Status)
	if status != StatusLBAOutOfRange {
		t.Fatalf("expected LBAOutOfRange, got 0x%04x", resp.Status)
	}
	if !status.DNR() {
		t.Fatal("LBAOutOfRange should have DNR=1")
	}

	clientConn.Close()
}

func TestIO_WriteNoInlineData(t *testing.T) {
	nqn := "nqn.test:io-noinline"
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

	// Write with no inline data (DataOffset=0)
	writeCmd := CapsuleCommand{
		OpCode: ioWrite,
		CID:    205,
		D10:    0,
		D12:    0,
	}
	// Send header-only (no data)
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, nil)

	resp := recvCapsuleResp(t, r)
	status := StatusWord(resp.Status)
	if status != StatusInvalidField {
		t.Fatalf("expected InvalidField for R2T write, got 0x%04x", resp.Status)
	}
	if !status.DNR() {
		t.Fatal("InvalidField should have DNR=1")
	}

	clientConn.Close()
}

func TestIO_WriteUnhealthy(t *testing.T) {
	nqn := "nqn.test:io-unhealthy"
	dev := newMockDevice(256, 512)
	dev.anaState = anaInaccessible // not writable

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

	writeData := make([]byte, 512)
	writeCmd := CapsuleCommand{
		OpCode: ioWrite,
		CID:    206,
		D10:    0,
		D12:    0,
	}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, writeData)

	resp := recvCapsuleResp(t, r)
	status := StatusWord(resp.Status)
	if status != StatusNSNotReady {
		t.Fatalf("expected NSNotReady for unhealthy, got 0x%04x", resp.Status)
	}

	clientConn.Close()
}

func TestIO_ReadError(t *testing.T) {
	nqn := "nqn.test:io-readerr"
	dev := newMockDevice(256, 512)
	dev.readErr = errors.New("simulated Read error")

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

	readCmd := CapsuleCommand{
		OpCode: ioRead,
		CID:    207,
		D10:    0,
		D12:    0,
	}
	w.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	resp := recvCapsuleResp(t, r)
	status := StatusWord(resp.Status)
	if status != StatusMediaReadError {
		t.Fatalf("expected MediaReadError, got 0x%04x (SCT=%d SC=0x%02x)", resp.Status, status.SCT(), status.SC())
	}

	clientConn.Close()
}

func TestIO_WriteError(t *testing.T) {
	nqn := "nqn.test:io-writeerr"
	dev := newMockDevice(256, 512)
	dev.writeErr = errors.New("simulated Write fault")

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

	writeData := make([]byte, 512)
	writeCmd := CapsuleCommand{
		OpCode: ioWrite,
		CID:    208,
		D10:    0,
		D12:    0,
	}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, writeData)

	resp := recvCapsuleResp(t, r)
	status := StatusWord(resp.Status)
	if status != StatusMediaWriteFault {
		t.Fatalf("expected MediaWriteFault, got 0x%04x", resp.Status)
	}

	clientConn.Close()
}

// ============================================================
// Error Mapping Tests
// ============================================================

func TestErrorMapping_AllSentinels(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		want   StatusWord
	}{
		{"nil", nil, StatusSuccess},
		{"LeaseExpired", blockvol.ErrLeaseExpired, StatusNSNotReadyDNR},
		{"EpochRegression", blockvol.ErrEpochRegression, StatusInternalErrorDNR},
		{"DurabilityBarrier", blockerr.ErrDurabilityBarrierFailed, StatusInternalError},
		{"DurabilityQuorum", blockerr.ErrDurabilityQuorumLost, StatusInternalError},
		{"WALFull", blockvol.ErrWALFull, StatusNSNotReady},
		{"NotPrimary", blockvol.ErrNotPrimary, StatusNSNotReady},
		{"GenericError", errors.New("something else"), StatusInternalError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapBlockError(tt.err)
			if got != tt.want {
				t.Fatalf("mapBlockError(%v) = 0x%04x, want 0x%04x", tt.err, got, tt.want)
			}
		})
	}
}

func TestErrorMapping_DNR(t *testing.T) {
	// LeaseExpired: DNR=1
	s := mapBlockError(blockvol.ErrLeaseExpired)
	if !s.DNR() {
		t.Fatal("LeaseExpired should have DNR=1")
	}
	// WALFull: DNR=0 (retryable)
	s = mapBlockError(blockvol.ErrWALFull)
	if s.DNR() {
		t.Fatal("WALFull should have DNR=0")
	}
}

// ============================================================
// ANA State Tests
// ============================================================

func TestANAState_AllRoles(t *testing.T) {
	tests := []struct {
		role blockvol.Role
		want uint8
	}{
		{blockvol.RolePrimary, anaOptimized},
		{blockvol.RoleNone, anaOptimized},
		{blockvol.RoleReplica, anaInaccessible},
		{blockvol.RoleStale, anaPersistentLoss},
		{blockvol.RoleRebuilding, anaInaccessible},
		{blockvol.RoleDraining, anaInaccessible},
	}
	for _, tt := range tests {
		got := RoleToANAState(tt.role)
		if got != tt.want {
			t.Fatalf("RoleToANAState(%v) = 0x%02x, want 0x%02x", tt.role, got, tt.want)
		}
	}
}

// ============================================================
// Adapter Tests
// ============================================================

func TestNGUID_Generation(t *testing.T) {
	uuid := [16]byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 1, 2, 3, 4, 5, 6, 7, 8}
	nguid := UUIDToNGUID(uuid)

	// NAA-6 prefix: first nibble = 6
	if (nguid[0] >> 4) != 0x06 {
		t.Fatalf("NAA prefix = 0x%x, want 0x06", nguid[0]>>4)
	}
	// Lower nibble from uuid[0]
	if (nguid[0] & 0x0F) != (uuid[0] & 0x0F) {
		t.Fatalf("lower nibble mismatch")
	}
	// Bytes 1-7 from uuid
	for i := 1; i < 8; i++ {
		if nguid[i] != uuid[i] {
			t.Fatalf("nguid[%d] = 0x%02x, want 0x%02x", i, nguid[i], uuid[i])
		}
	}
	// Bytes 8-15 from uuid
	for i := 8; i < 16; i++ {
		if nguid[i] != uuid[i] {
			t.Fatalf("nguid[%d] = 0x%02x, want 0x%02x", i, nguid[i], uuid[i])
		}
	}
}

// ============================================================
// Server Lifecycle Tests
// ============================================================

func TestServer_StartStop(t *testing.T) {
	srv := NewServer(Config{
		Enabled:     true,
		ListenAddr:  "127.0.0.1:0",
		MaxIOQueues: 4,
	})

	dev := newMockDevice(256, 512)
	srv.AddVolume("nqn.test:srv", dev, dev.DeviceNGUID())

	if err := srv.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)

	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestServer_DisabledNoOp(t *testing.T) {
	srv := NewServer(Config{Enabled: false})
	if err := srv.ListenAndServe(); err != nil {
		t.Fatal("disabled server should return nil")
	}
	if err := srv.Close(); err != nil {
		t.Fatal("disabled close should return nil")
	}
}

func TestServer_AddRemoveVolume(t *testing.T) {
	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	dev := newMockDevice(256, 512)

	nqn := "nqn.test:vol1"
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	if sub := srv.findSubsystem(nqn); sub == nil {
		t.Fatal("subsystem not found after add")
	}

	srv.RemoveVolume(nqn)
	if sub := srv.findSubsystem(nqn); sub != nil {
		t.Fatal("subsystem still found after remove")
	}
}

func TestServer_ConcurrentAccept(t *testing.T) {
	srv := NewServer(Config{
		Enabled:     true,
		ListenAddr:  "127.0.0.1:0",
		MaxIOQueues: 4,
	})
	dev := newMockDevice(256, 512)
	srv.AddVolume("nqn.test:concurrent", dev, dev.DeviceNGUID())

	if err := srv.ListenAndServe(); err != nil {
		t.Fatal(err)
	}

	addr := srv.listener.Addr().String()

	// Connect 3 clients concurrently
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, time.Second)
			if err != nil {
				return
			}
			defer conn.Close()

			w := NewWriter(conn)
			r := NewReader(conn)
			sendICReq(w)
			hdr, err := r.Dequeue()
			if err != nil {
				return
			}
			if hdr.Type != pduICResp {
				t.Errorf("expected ICResp, got 0x%x", hdr.Type)
			}
			var resp ICResponse
			r.Receive(&resp)
		}()
	}
	wg.Wait()

	srv.Close()
}

// ============================================================
// KATO Timeout Test
// ============================================================

func TestController_KATOTimeout(t *testing.T) {
	nqn := "nqn.test:kato"

	clientConn, serverConn := pipeConn()
	dev := newMockDevice(256, 512)
	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	ctrl := newController(serverConn, srv)

	done := make(chan error, 1)
	go func() {
		done <- ctrl.Serve()
	}()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)

	// IC
	sendICReq(w)
	recvICResp(t, r)

	// Connect with very short KATO (100ms)
	sendConnect(w, 0, 64, 100, nqn, "host", 0xFFFF)
	recvCapsuleResp(t, r)

	// Enable controller (which starts KATO timer)
	propSet := CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcPropertySet,
		CID:    1,
		D10:    propCC,
		D14:    1, // CC.EN=1
	}
	w.SendWithData(pduCapsuleCmd, 0, &propSet, capsuleCmdSize, nil)
	recvCapsuleResp(t, r)

	// Wait for KATO to expire (100ms + 50% margin = 150ms, wait 300ms)
	time.Sleep(300 * time.Millisecond)

	// Connection should be closed by KATO
	_, err := r.Dequeue()
	if err == nil || err == io.EOF {
		// EOF is expected when connection is closed
	}

	clientConn.Close()
}

// ============================================================
// Full Protocol Sequence Test
// ============================================================

func TestFullSequence_ICConnectIdentifyReadWrite(t *testing.T) {
	nqn := "nqn.test:fullseq"
	dev := newMockDevice(256, 512)

	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	clientConn, serverConn := pipeConn()
	defer clientConn.Close()

	ctrl := newController(serverConn, srv)
	go ctrl.Serve()

	r := NewReader(clientConn)
	w := NewWriter(clientConn)

	// 1. IC Handshake
	sendICReq(w)
	recvICResp(t, r)

	// 2. Admin Connect
	sendConnect(w, 0, 64, 60000, nqn, "host-nqn", 0xFFFF)
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("Connect failed: 0x%04x", resp.Status)
	}

	// 3. SetFeatures NumQueues
	sfCmd := CapsuleCommand{
		OpCode: adminSetFeatures,
		CID:    5,
		D10:    uint32(fidNumberOfQueues),
		D11:    3 | (3 << 16), // 4 queues each
	}
	w.SendWithData(pduCapsuleCmd, 0, &sfCmd, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("SetFeatures NumQueues failed: 0x%04x", resp.Status)
	}

	// 4. PropertySet CC.EN=1
	propCmd := CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcPropertySet,
		CID:    6,
		D10:    propCC,
		D14:    1,
	}
	w.SendWithData(pduCapsuleCmd, 0, &propCmd, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("PropertySet CC.EN failed: 0x%04x", resp.Status)
	}

	// 5. Identify Controller
	idCmd := CapsuleCommand{
		OpCode: adminIdentify,
		CID:    7,
		D10:    uint32(cnsIdentifyController),
	}
	w.SendWithData(pduCapsuleCmd, 0, &idCmd, capsuleCmdSize, nil)

	hdr, _ := r.Dequeue()
	if hdr.Type != pduC2HData {
		t.Fatalf("expected C2HData for identify, got 0x%x", hdr.Type)
	}
	var c2h C2HDataHeader
	r.Receive(&c2h)
	identData := make([]byte, r.Length())
	r.ReceiveData(identData)

	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("Identify failed: 0x%04x", resp.Status)
	}

	// 6. Identify Namespace
	idNsCmd := CapsuleCommand{
		OpCode: adminIdentify,
		CID:    8,
		D10:    uint32(cnsIdentifyNamespace),
	}
	w.SendWithData(pduCapsuleCmd, 0, &idNsCmd, capsuleCmdSize, nil)

	hdr, _ = r.Dequeue()
	if hdr.Type != pduC2HData {
		t.Fatalf("expected C2HData for identify ns")
	}
	r.Receive(&c2h)
	nsData := make([]byte, r.Length())
	r.ReceiveData(nsData)
	nsze := binary.LittleEndian.Uint64(nsData[0:])
	if nsze != 256 {
		t.Fatalf("NSZE = %d, want 256", nsze)
	}
	recvCapsuleResp(t, r)

	// 7. KeepAlive
	kaCmd := CapsuleCommand{
		OpCode: adminKeepAlive,
		CID:    9,
	}
	w.SendWithData(pduCapsuleCmd, 0, &kaCmd, capsuleCmdSize, nil)
	resp = recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("KeepAlive failed: 0x%04x", resp.Status)
	}

	clientConn.Close()
}

func TestServer_NQN(t *testing.T) {
	srv := NewServer(Config{
		NQNPrefix: "nqn.2024-01.com.seaweedfs:vol.",
	})
	got := srv.NQN("test-vol")
	want := "nqn.2024-01.com.seaweedfs:vol.test-vol"
	if got != want {
		t.Fatalf("NQN() = %q, want %q", got, want)
	}
}

// ============================================================
// Cross-Connection IO Queue Tests (Finding #1)
// ============================================================

// TestIOQueue_CrossConnection verifies that IO queues on separate TCP
// connections can validate CNTLID against the admin session registry.
func TestIOQueue_CrossConnection(t *testing.T) {
	nqn := "nqn.test:cross-conn"
	dev := newMockDevice(256, 512)
	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	// --- Admin queue connection (QID=0) ---
	adminClient, adminServer := pipeConn()
	defer adminClient.Close()

	adminCtrl := newController(adminServer, srv)
	go adminCtrl.Serve()

	ar := NewReader(adminClient)
	aw := NewWriter(adminClient)

	sendICReq(aw)
	recvICResp(t, ar)

	sendConnect(aw, 0, 64, 60000, nqn, "host-nqn", 0xFFFF)
	resp := recvCapsuleResp(t, ar)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("Admin Connect failed: 0x%04x", resp.Status)
	}
	cntlID := uint16(resp.DW0)
	if cntlID == 0 {
		t.Fatal("expected non-zero CNTLID")
	}

	// --- IO queue connection (QID=1, separate TCP conn) ---
	ioClient, ioServer := pipeConn()
	defer ioClient.Close()

	ioCtrl := newController(ioServer, srv)
	go ioCtrl.Serve()

	ir := NewReader(ioClient)
	iw := NewWriter(ioClient)

	sendICReq(iw)
	recvICResp(t, ir)

	// IO Connect with CNTLID from admin session
	sendConnect(iw, 1, 64, 0, nqn, "host-nqn", cntlID)
	resp = recvCapsuleResp(t, ir)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("IO Connect failed: 0x%04x", resp.Status)
	}
	if uint16(resp.DW0) != cntlID {
		t.Fatalf("IO Connect returned CNTLID=%d, want %d", resp.DW0, cntlID)
	}

	// Verify IO commands work on the IO queue
	writeData := bytes.Repeat([]byte{0xEE}, 512)
	writeCmd := CapsuleCommand{
		OpCode: ioWrite,
		CID:    300,
		D10:    0,
		D12:    0, // 1 block
	}
	iw.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, writeData)
	resp = recvCapsuleResp(t, ir)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("IO Write failed: 0x%04x", resp.Status)
	}

	// Read back
	readCmd := CapsuleCommand{
		OpCode: ioRead,
		CID:    301,
		D10:    0,
		D12:    0,
	}
	iw.SendWithData(pduCapsuleCmd, 0, &readCmd, capsuleCmdSize, nil)

	hdr, err := ir.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Type != pduC2HData {
		t.Fatalf("expected C2HData, got 0x%x", hdr.Type)
	}
	var c2h C2HDataHeader
	ir.Receive(&c2h)
	data := make([]byte, ir.Length())
	ir.ReceiveData(data)

	if !bytes.Equal(data, writeData) {
		t.Fatal("read data doesn't match written data")
	}

	resp = recvCapsuleResp(t, ir)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("IO Read failed: 0x%04x", resp.Status)
	}

	adminClient.Close()
	ioClient.Close()
}

// TestIOQueue_InvalidCNTLID verifies that IO queue connect with wrong CNTLID fails.
func TestIOQueue_InvalidCNTLID(t *testing.T) {
	nqn := "nqn.test:bad-cntlid"
	dev := newMockDevice(256, 512)
	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	// IO queue connection with CNTLID that doesn't exist (no admin session)
	ioClient, ioServer := pipeConn()
	defer ioClient.Close()

	ioCtrl := newController(ioServer, srv)
	go ioCtrl.Serve()

	ir := NewReader(ioClient)
	iw := NewWriter(ioClient)

	sendICReq(iw)
	recvICResp(t, ir)

	// Try IO Connect with bogus CNTLID
	sendConnect(iw, 1, 64, 0, nqn, "host-nqn", 9999)
	resp := recvCapsuleResp(t, ir)
	if !StatusWord(resp.Status).IsError() {
		t.Fatal("expected error for invalid CNTLID")
	}

	ioClient.Close()
}

// TestIOQueue_NQNMismatch verifies IO queue connect fails when SubNQN
// doesn't match the admin session's SubNQN.
func TestIOQueue_NQNMismatch(t *testing.T) {
	nqn1 := "nqn.test:nqn-match"
	nqn2 := "nqn.test:nqn-other"
	dev := newMockDevice(256, 512)
	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn1, dev, dev.DeviceNGUID())
	srv.AddVolume(nqn2, dev, dev.DeviceNGUID())

	// Admin queue connect to nqn1
	adminClient, adminServer := pipeConn()
	defer adminClient.Close()

	adminCtrl := newController(adminServer, srv)
	go adminCtrl.Serve()

	ar := NewReader(adminClient)
	aw := NewWriter(adminClient)

	sendICReq(aw)
	recvICResp(t, ar)

	sendConnect(aw, 0, 64, 0, nqn1, "host", 0xFFFF)
	resp := recvCapsuleResp(t, ar)
	cntlID := uint16(resp.DW0)

	// IO queue connect with same CNTLID but different NQN
	ioClient, ioServer := pipeConn()
	defer ioClient.Close()

	ioCtrl := newController(ioServer, srv)
	go ioCtrl.Serve()

	ir := NewReader(ioClient)
	iw := NewWriter(ioClient)

	sendICReq(iw)
	recvICResp(t, ir)

	sendConnect(iw, 1, 64, 0, nqn2, "host", cntlID)
	resp = recvCapsuleResp(t, ir)
	if !StatusWord(resp.Status).IsError() {
		t.Fatal("expected error for NQN mismatch on IO queue connect")
	}

	adminClient.Close()
	ioClient.Close()
}

// TestAdminSession_UnregisteredOnShutdown verifies admin sessions are
// cleaned up when the admin controller shuts down.
func TestAdminSession_UnregisteredOnShutdown(t *testing.T) {
	nqn := "nqn.test:unreg"
	dev := newMockDevice(256, 512)
	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	adminClient, adminServer := pipeConn()

	adminCtrl := newController(adminServer, srv)
	go adminCtrl.Serve()

	ar := NewReader(adminClient)
	aw := NewWriter(adminClient)

	sendICReq(aw)
	recvICResp(t, ar)

	sendConnect(aw, 0, 64, 0, nqn, "host", 0xFFFF)
	resp := recvCapsuleResp(t, ar)
	cntlID := uint16(resp.DW0)

	// Admin session should be registered
	if srv.lookupAdmin(cntlID) == nil {
		t.Fatal("admin session not registered")
	}

	// Close admin connection → triggers shutdown → unregister
	adminClient.Close()
	time.Sleep(50 * time.Millisecond) // give goroutine time to cleanup

	if srv.lookupAdmin(cntlID) != nil {
		t.Fatal("admin session should be unregistered after shutdown")
	}
}

// ============================================================
// Header Bounds Validation Tests (Finding #2)
// ============================================================

func TestReader_MalformedHeader_TooSmall(t *testing.T) {
	// HeaderLength < 8 (common header size)
	buf := make([]byte, commonHeaderSize)
	hdr := CommonHeader{
		Type:         pduCapsuleCmd,
		HeaderLength: 4, // invalid: < 8
		DataLength:   4,
	}
	hdr.Marshal(buf)
	r := NewReader(bytes.NewReader(buf))
	_, err := r.Dequeue()
	if err == nil {
		t.Fatal("expected error for HeaderLength < 8")
	}
}

func TestReader_MalformedHeader_TooLarge(t *testing.T) {
	buf := make([]byte, commonHeaderSize)
	hdr := CommonHeader{
		Type:         pduCapsuleCmd,
		HeaderLength: 255, // larger than maxHeaderSize
		DataLength:   255,
	}
	hdr.Marshal(buf)
	r := NewReader(bytes.NewReader(buf))
	_, err := r.Dequeue()
	if err == nil {
		t.Fatal("expected error for HeaderLength > maxHeaderSize")
	}
}

func TestReader_MalformedHeader_DataOffsetLessThanHeaderLength(t *testing.T) {
	buf := make([]byte, commonHeaderSize)
	hdr := CommonHeader{
		Type:         pduCapsuleCmd,
		HeaderLength: 72,
		DataOffset:   32, // invalid: < HeaderLength
		DataLength:   100,
	}
	hdr.Marshal(buf)
	r := NewReader(bytes.NewReader(buf))
	_, err := r.Dequeue()
	if err == nil {
		t.Fatal("expected error for DataOffset < HeaderLength")
	}
}

func TestReader_MalformedHeader_DataOffsetGtDataLength(t *testing.T) {
	buf := make([]byte, commonHeaderSize)
	hdr := CommonHeader{
		Type:         pduCapsuleCmd,
		HeaderLength: 72,
		DataOffset:   72,
		DataLength:   50, // invalid: DataOffset > DataLength
	}
	hdr.Marshal(buf)
	r := NewReader(bytes.NewReader(buf))
	_, err := r.Dequeue()
	if err == nil {
		t.Fatal("expected error for DataOffset > DataLength")
	}
}

func TestReader_MalformedHeader_DataLengthLtHeaderLength(t *testing.T) {
	buf := make([]byte, commonHeaderSize)
	hdr := CommonHeader{
		Type:         pduCapsuleCmd,
		HeaderLength: 72,
		DataOffset:   0,
		DataLength:   40, // invalid: DataLength < HeaderLength
	}
	hdr.Marshal(buf)
	r := NewReader(bytes.NewReader(buf))
	_, err := r.Dequeue()
	if err == nil {
		t.Fatal("expected error for DataLength < HeaderLength")
	}
}

func TestReader_MalformedHeader_DataOffsetZero_ExtraDataLength(t *testing.T) {
	// DataOffset==0 but DataLength > HeaderLength → unconsumed bytes would desync stream.
	buf := make([]byte, commonHeaderSize)
	hdr := CommonHeader{
		Type:         pduCapsuleResp,
		HeaderLength: 24,
		DataOffset:   0,
		DataLength:   100, // invalid: no data expected but DataLength > HeaderLength
	}
	hdr.Marshal(buf)
	r := NewReader(bytes.NewReader(buf))
	_, err := r.Dequeue()
	if err == nil {
		t.Fatal("expected error for DataOffset=0 with DataLength > HeaderLength")
	}
}

// ============================================================
// IO Queue Host Identity Tests (Finding: HostNQN continuity)
// ============================================================

func TestIOQueue_HostNQNMismatch(t *testing.T) {
	nqn := "nqn.test:hostnqn"
	dev := newMockDevice(256, 512)
	srv := NewServer(Config{Enabled: true, ListenAddr: "127.0.0.1:0", MaxIOQueues: 4})
	srv.AddVolume(nqn, dev, dev.DeviceNGUID())

	// Admin connect with HostNQN "host-A"
	adminClient, adminServer := pipeConn()
	defer adminClient.Close()

	adminCtrl := newController(adminServer, srv)
	go adminCtrl.Serve()

	ar := NewReader(adminClient)
	aw := NewWriter(adminClient)

	sendICReq(aw)
	recvICResp(t, ar)

	sendConnect(aw, 0, 64, 0, nqn, "host-A", 0xFFFF)
	resp := recvCapsuleResp(t, ar)
	cntlID := uint16(resp.DW0)

	// IO connect with same CNTLID + SubNQN but different HostNQN "host-B"
	ioClient, ioServer := pipeConn()
	defer ioClient.Close()

	ioCtrl := newController(ioServer, srv)
	go ioCtrl.Serve()

	ir := NewReader(ioClient)
	iw := NewWriter(ioClient)

	sendICReq(iw)
	recvICResp(t, ir)

	sendConnect(iw, 1, 64, 0, nqn, "host-B", cntlID)
	resp = recvCapsuleResp(t, ir)
	if !StatusWord(resp.Status).IsError() {
		t.Fatal("expected error for HostNQN mismatch on IO queue connect")
	}

	adminClient.Close()
	ioClient.Close()
}

// ============================================================
// Write Payload Size Validation Tests (Finding #3)
// ============================================================

func TestIO_WritePayloadSizeMismatch(t *testing.T) {
	nqn := "nqn.test:io-paysize"
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

	// Write with NLB=1 (1 block = 512 bytes) but payload = 256 bytes
	writeData := make([]byte, 256) // wrong size
	writeCmd := CapsuleCommand{
		OpCode: ioWrite,
		CID:    400,
		D10:    0,
		D12:    0, // 1 block
	}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, writeData)

	resp := recvCapsuleResp(t, r)
	if !StatusWord(resp.Status).IsError() {
		t.Fatal("expected error for payload size mismatch")
	}
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("expected InvalidField, got 0x%04x", resp.Status)
	}

	clientConn.Close()
}

func TestIO_WritePayloadTooLarge(t *testing.T) {
	nqn := "nqn.test:io-paysize2"
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

	// Write with NLB=1 (1 block = 512 bytes) but payload = 1024 bytes
	writeData := make([]byte, 1024) // too large
	writeCmd := CapsuleCommand{
		OpCode: ioWrite,
		CID:    401,
		D10:    0,
		D12:    0, // 1 block
	}
	w.SendWithData(pduCapsuleCmd, 0, &writeCmd, capsuleCmdSize, writeData)

	resp := recvCapsuleResp(t, r)
	if !StatusWord(resp.Status).IsError() {
		t.Fatal("expected error for oversized payload")
	}
	if StatusWord(resp.Status) != StatusInvalidField {
		t.Fatalf("expected InvalidField, got 0x%04x", resp.Status)
	}

	clientConn.Close()
}

// ============================================================
// Disconnect Non-Error Tests (Finding #5)
// ============================================================

func TestDisconnect_NoError(t *testing.T) {
	nqn := "nqn.test:disconnect"
	client, r, w, _, cntlID := setupAdminSession(t, nqn)
	defer client.Close()

	_ = cntlID

	// Send Disconnect
	disconnectCmd := CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcDisconnect,
		CID:    50,
	}
	w.SendWithData(pduCapsuleCmd, 0, &disconnectCmd, capsuleCmdSize, nil)

	// Should get a success response
	resp := recvCapsuleResp(t, r)
	if StatusWord(resp.Status).IsError() {
		t.Fatalf("Disconnect response should be success, got 0x%04x", resp.Status)
	}

	// Connection should be closed after disconnect
	time.Sleep(50 * time.Millisecond)
	_, err := r.Dequeue()
	if err == nil {
		t.Fatal("expected read error after disconnect")
	}

	client.Close()
}
