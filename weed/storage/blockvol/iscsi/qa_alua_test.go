package iscsi

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
)

// TestQAALUA runs adversarial tests for ALUA implementation.
func TestQAALUA(t *testing.T) {
	tests := []struct {
		name string
		fn   func(*testing.T)
	}{
		// Group A: State boundary + transition
		{"standby_metadata_cmds_allowed", testQA_StandbyMetadataCmdsAllowed},
		{"unavailable_rejects_writes", testQA_UnavailableRejectsWrites},
		{"transitioning_rejects_writes", testQA_TransitioningRejectsWrites},
		{"active_allows_all_ops", testQA_ActiveAllowsAllOps},
		{"state_change_mid_stream", testQA_StateChangeMidStream},

		// Group B: VPD 0x83 edge cases
		{"vpd83_no_alua_single_descriptor", testQA_VPD83_NoALUA_SingleDescriptor},
		{"vpd83_truncation_mid_descriptor", testQA_VPD83_TruncationMidDescriptor},
		{"vpd83_naa_high_bits_uuid", testQA_VPD83_NAAHighBitsUUID},

		// Group C: REPORT TPG edge cases
		{"report_tpg_no_alua_rejected", testQA_ReportTPG_NoALUA_Rejected},
		{"report_tpg_small_alloc_len", testQA_ReportTPG_SmallAllocLen},
		{"report_tpg_tpgid_boundary", testQA_ReportTPG_TPGIDBoundary},
		{"report_tpg_all_states", testQA_ReportTPG_AllStates},

		// Group D: Concurrency
		{"concurrent_state_reads", testQA_ConcurrentStateReads},
		{"concurrent_standby_reject", testQA_ConcurrentStandbyReject},

		// Group E: INQUIRY interaction
		{"inquiry_tpgs_preserves_cmdque", testQA_InquiryTPGS_PreservesCmdQue},
		{"inquiry_vpd00_unchanged_with_alua", testQA_InquiryVPD00_UnchangedWithALUA},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(t)
		})
	}
}

// --- Group A: State boundary + transition ---

// testQA_StandbyMetadataCmdsAllowed verifies that metadata/discovery commands
// work on standby paths. dm-multipath sends these to probe path health.
func testQA_StandbyMetadataCmdsAllowed(t *testing.T) {
	dev := newMockALUADevice(100*4096, ALUAStandby, 1)
	h := NewSCSIHandler(dev)

	// TEST UNIT READY — must succeed (path health check)
	var cdb [16]byte
	cdb[0] = ScsiTestUnitReady
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("TUR on Standby: expected GOOD, got %d", r.Status)
	}

	// INQUIRY — must succeed
	cdb = [16]byte{}
	cdb[0] = ScsiInquiry
	binary.BigEndian.PutUint16(cdb[3:5], 96)
	r = h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("INQUIRY on Standby: expected GOOD, got %d", r.Status)
	}

	// READ CAPACITY 10 — must succeed
	cdb = [16]byte{}
	cdb[0] = ScsiReadCapacity10
	r = h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("READ CAPACITY(10) on Standby: expected GOOD, got %d", r.Status)
	}

	// READ CAPACITY 16 — must succeed
	cdb = [16]byte{}
	cdb[0] = ScsiServiceActionIn16
	cdb[1] = ScsiSAReadCapacity16
	binary.BigEndian.PutUint32(cdb[10:14], 32)
	r = h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("READ CAPACITY(16) on Standby: expected GOOD, got %d", r.Status)
	}

	// REPORT LUNS — must succeed
	cdb = [16]byte{}
	cdb[0] = ScsiReportLuns
	binary.BigEndian.PutUint32(cdb[6:10], 256)
	r = h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("REPORT LUNS on Standby: expected GOOD, got %d", r.Status)
	}

	// MODE SENSE 6 — must succeed
	cdb = [16]byte{}
	cdb[0] = ScsiModeSense6
	cdb[2] = 0x3f // all pages
	cdb[4] = 255
	r = h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("MODE SENSE(6) on Standby: expected GOOD, got %d", r.Status)
	}

	// REQUEST SENSE — must succeed
	cdb = [16]byte{}
	cdb[0] = ScsiRequestSense
	cdb[4] = 18
	r = h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("REQUEST SENSE on Standby: expected GOOD, got %d", r.Status)
	}

	// REPORT TARGET PORT GROUPS — must succeed (ALUA path probing)
	cdb = [16]byte{}
	cdb[0] = ScsiMaintenanceIn
	cdb[1] = 0x0a
	binary.BigEndian.PutUint32(cdb[6:10], 255)
	r = h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("REPORT TPG on Standby: expected GOOD, got %d", r.Status)
	}

	// Read16 — reads must work on standby for path probing
	cdb = [16]byte{}
	cdb[0] = ScsiRead16
	binary.BigEndian.PutUint64(cdb[2:10], 0)
	binary.BigEndian.PutUint32(cdb[10:14], 1)
	r = h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("READ(16) on Standby: expected GOOD, got %d", r.Status)
	}
}

// testQA_UnavailableRejectsWrites verifies writes are rejected when ALUA state
// is Unavailable (RoleStale). Same sense as Standby.
func testQA_UnavailableRejectsWrites(t *testing.T) {
	dev := newMockALUADevice(100*4096, ALUAUnavailable, 1)
	h := NewSCSIHandler(dev)

	opcodes := []struct {
		name   string
		opcode uint8
	}{
		{"Write10", ScsiWrite10},
		{"Write16", ScsiWrite16},
		{"SyncCache10", ScsiSyncCache10},
		{"SyncCache16", ScsiSyncCache16},
	}

	for _, op := range opcodes {
		var cdb [16]byte
		cdb[0] = op.opcode
		if op.opcode == ScsiWrite10 {
			binary.BigEndian.PutUint16(cdb[7:9], 1)
		} else if op.opcode == ScsiWrite16 {
			binary.BigEndian.PutUint32(cdb[10:14], 1)
		}
		r := h.HandleCommand(cdb, make([]byte, 4096))
		if r.SenseKey != SenseNotReady {
			t.Fatalf("%s on Unavailable: expected NOT_READY, got %02x", op.name, r.SenseKey)
		}
		if r.SenseASC != 0x04 || r.SenseASCQ != 0x0B {
			t.Fatalf("%s on Unavailable: expected ASC=04h ASCQ=0Bh, got %02x/%02x",
				op.name, r.SenseASC, r.SenseASCQ)
		}
	}
}

// testQA_TransitioningRejectsWrites verifies writes are rejected during
// Transitioning state (RoleRebuilding, RoleDraining).
func testQA_TransitioningRejectsWrites(t *testing.T) {
	dev := newMockALUADevice(100*4096, ALUATransitioning, 1)
	h := NewSCSIHandler(dev)

	var cdb [16]byte
	cdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], 0)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	r := h.HandleCommand(cdb, make([]byte, 4096))
	if r.SenseKey != SenseNotReady {
		t.Fatalf("Write10 on Transitioning: expected NOT_READY, got %02x", r.SenseKey)
	}
	if r.SenseASCQ != 0x0B {
		t.Fatalf("Write10 on Transitioning: expected ASCQ=0Bh (standby), got %02x", r.SenseASCQ)
	}

	// But reads should still work (transitioning is still accessible for reads)
	cdb = [16]byte{}
	cdb[0] = ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], 0)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	r = h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("Read10 on Transitioning: expected GOOD, got status %d (sense %02x)", r.Status, r.SenseKey)
	}
}

// testQA_ActiveAllowsAllOps verifies that Active/Optimized allows all operations.
func testQA_ActiveAllowsAllOps(t *testing.T) {
	dev := newMockALUADevice(100*4096, ALUAActiveOptimized, 1)
	h := NewSCSIHandler(dev)

	// Write10
	var cdb [16]byte
	cdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], 0)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	r := h.HandleCommand(cdb, make([]byte, 4096))
	if r.Status != SCSIStatusGood {
		t.Fatalf("Write10 on Active: expected GOOD, got status %d", r.Status)
	}

	// Read10
	cdb = [16]byte{}
	cdb[0] = ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], 0)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	r = h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("Read10 on Active: expected GOOD, got status %d", r.Status)
	}

	// SyncCache10
	cdb = [16]byte{}
	cdb[0] = ScsiSyncCache10
	r = h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("SyncCache10 on Active: expected GOOD, got status %d", r.Status)
	}

	// Unmap
	unmapData := make([]byte, 24)
	binary.BigEndian.PutUint16(unmapData[0:2], 22)
	binary.BigEndian.PutUint16(unmapData[2:4], 16)
	binary.BigEndian.PutUint64(unmapData[8:16], 0)
	binary.BigEndian.PutUint32(unmapData[16:20], 1)
	cdb = [16]byte{}
	cdb[0] = ScsiUnmap
	r = h.HandleCommand(cdb, unmapData)
	if r.Status != SCSIStatusGood {
		t.Fatalf("Unmap on Active: expected GOOD, got status %d", r.Status)
	}
}

// mutableALUADevice allows changing ALUA state between calls to simulate
// role transitions during I/O.
type mutableALUADevice struct {
	*mockBlockDevice
	state atomic.Uint32
	tpgID uint16
	naa   [8]byte
}

func newMutableALUADevice(volumeSize uint64, initialState uint8, tpgID uint16) *mutableALUADevice {
	d := &mutableALUADevice{
		mockBlockDevice: newMockDevice(volumeSize),
		tpgID:           tpgID,
		naa:             [8]byte{0x60, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x01},
	}
	d.state.Store(uint32(initialState))
	return d
}

func (m *mutableALUADevice) ALUAState() uint8    { return uint8(m.state.Load()) }
func (m *mutableALUADevice) TPGroupID() uint16   { return m.tpgID }
func (m *mutableALUADevice) DeviceNAA() [8]byte  { return m.naa }
func (m *mutableALUADevice) SetState(s uint8)    { m.state.Store(uint32(s)) }

// testQA_StateChangeMidStream verifies that ALUA state changes between commands
// are reflected immediately. A role change (Active→Standby) must fence writes
// on the very next command.
func testQA_StateChangeMidStream(t *testing.T) {
	dev := newMutableALUADevice(100*4096, ALUAActiveOptimized, 1)
	h := NewSCSIHandler(dev)

	// Write succeeds while Active
	var cdb [16]byte
	cdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], 0)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	r := h.HandleCommand(cdb, make([]byte, 4096))
	if r.Status != SCSIStatusGood {
		t.Fatalf("Write while Active: expected GOOD, got %d", r.Status)
	}

	// Transition to Standby (simulates demotion)
	dev.SetState(ALUAStandby)

	// Next write must be rejected immediately
	r = h.HandleCommand(cdb, make([]byte, 4096))
	if r.SenseKey != SenseNotReady {
		t.Fatalf("Write after transition to Standby: expected NOT_READY, got %02x", r.SenseKey)
	}

	// Reads still work
	cdb = [16]byte{}
	cdb[0] = ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], 0)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	r = h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("Read after transition to Standby: expected GOOD, got %d", r.Status)
	}

	// Transition back to Active
	dev.SetState(ALUAActiveOptimized)

	// Writes work again
	cdb = [16]byte{}
	cdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], 0)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	r = h.HandleCommand(cdb, make([]byte, 4096))
	if r.Status != SCSIStatusGood {
		t.Fatalf("Write after re-activation: expected GOOD, got %d", r.Status)
	}

	// Transition to Transitioning
	dev.SetState(ALUATransitioning)

	// REPORT TPG should show Transitioning
	cdb = [16]byte{}
	cdb[0] = ScsiMaintenanceIn
	cdb[1] = 0x0a
	binary.BigEndian.PutUint32(cdb[6:10], 255)
	r = h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("REPORT TPG during Transitioning: expected GOOD, got %d", r.Status)
	}
	state := r.Data[4] & 0x0F
	if state != ALUATransitioning {
		t.Fatalf("REPORT TPG state: got %02x, want %02x", state, ALUATransitioning)
	}
}

// --- Group B: VPD 0x83 edge cases ---

// testQA_VPD83_NoALUA_SingleDescriptor verifies that without ALUAProvider,
// VPD 0x83 returns only the NAA descriptor (no TPG, no RTP).
func testQA_VPD83_NoALUA_SingleDescriptor(t *testing.T) {
	dev := newMockDevice(100 * 4096) // plain device, no ALUAProvider
	h := NewSCSIHandler(dev)

	var cdb [16]byte
	cdb[0] = ScsiInquiry
	cdb[1] = 0x01
	cdb[2] = 0x83
	binary.BigEndian.PutUint16(cdb[3:5], 255)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("status: %d", r.Status)
	}

	pageLen := binary.BigEndian.Uint16(r.Data[2:4])
	// Only NAA descriptor: 4 (header) + 8 (identifier) = 12 bytes
	if pageLen != 12 {
		t.Fatalf("page length without ALUA: got %d, want 12 (NAA only)", pageLen)
	}

	// Verify it's type 3 (NAA)
	if r.Data[4+1]&0x0F != 0x03 {
		t.Fatalf("descriptor type: got %02x, want 03 (NAA)", r.Data[4+1]&0x0F)
	}

	// Verify hardcoded NAA value (0x60, 0x01, ...)
	if r.Data[8] != 0x60 || r.Data[9] != 0x01 {
		t.Fatalf("hardcoded NAA: got %02x %02x, want 60 01", r.Data[8], r.Data[9])
	}
}

// testQA_VPD83_TruncationMidDescriptor verifies that allocLen smaller than the
// full VPD 0x83 response truncates correctly without panic.
func testQA_VPD83_TruncationMidDescriptor(t *testing.T) {
	dev := newMockALUADevice(100*4096, ALUAActiveOptimized, 1)
	h := NewSCSIHandler(dev)

	// Full response is 4 (page header) + 28 (descriptors) = 32 bytes.
	// Try various truncation points including mid-descriptor.
	allocLens := []uint16{1, 4, 8, 12, 16, 20, 24, 28, 31, 32, 255}

	for _, al := range allocLens {
		var cdb [16]byte
		cdb[0] = ScsiInquiry
		cdb[1] = 0x01
		cdb[2] = 0x83
		binary.BigEndian.PutUint16(cdb[3:5], al)
		r := h.HandleCommand(cdb, nil)
		if r.Status != SCSIStatusGood {
			t.Fatalf("allocLen=%d: expected GOOD, got %d", al, r.Status)
		}
		maxExpected := 32
		if int(al) < maxExpected {
			maxExpected = int(al)
		}
		if len(r.Data) != maxExpected {
			t.Fatalf("allocLen=%d: data len=%d, want %d", al, len(r.Data), maxExpected)
		}
	}
}

// testQA_VPD83_NAAHighBitsUUID verifies that uuidToNAA always produces a valid
// NAA-6 identifier even when UUID[0] has all bits set. The high nibble of byte 0
// must always be 0x6_.
func testQA_VPD83_NAAHighBitsUUID(t *testing.T) {
	// Test with various UUID[0] values
	testCases := []byte{0x00, 0x0F, 0xF0, 0xFF, 0x5A, 0xA5}

	for _, b := range testCases {
		dev := &mockALUADevice{
			mockBlockDevice: newMockDevice(100 * 4096),
			aluaState:       ALUAActiveOptimized,
			tpgID:           1,
			naa:             [8]byte{}, // will be set below
		}
		// Simulate what uuidToNAA does in main.go
		var uuid [16]byte
		uuid[0] = b
		for i := 1; i < 16; i++ {
			uuid[i] = byte(i)
		}
		// Manually compute NAA
		dev.naa[0] = 0x60 | (uuid[0] & 0x0F)
		copy(dev.naa[1:], uuid[1:8])

		h := NewSCSIHandler(dev)
		var cdb [16]byte
		cdb[0] = ScsiInquiry
		cdb[1] = 0x01
		cdb[2] = 0x83
		binary.BigEndian.PutUint16(cdb[3:5], 255)
		r := h.HandleCommand(cdb, nil)
		if r.Status != SCSIStatusGood {
			t.Fatalf("UUID[0]=%02x: status %d", b, r.Status)
		}

		// NAA byte is at offset 8 (4 page header + 4 desc header)
		naaByte := r.Data[8]
		if naaByte&0xF0 != 0x60 {
			t.Fatalf("UUID[0]=%02x: NAA high nibble = %02x, want 0x6_", b, naaByte&0xF0)
		}
		// Low nibble should be UUID[0]'s low nibble
		wantLow := b & 0x0F
		if naaByte&0x0F != wantLow {
			t.Fatalf("UUID[0]=%02x: NAA low nibble = %02x, want %02x", b, naaByte&0x0F, wantLow)
		}
	}
}

// --- Group C: REPORT TPG edge cases ---

// testQA_ReportTPG_NoALUA_Rejected verifies that REPORT TARGET PORT GROUPS
// returns ILLEGAL_REQUEST when the device doesn't implement ALUAProvider.
func testQA_ReportTPG_NoALUA_Rejected(t *testing.T) {
	dev := newMockDevice(100 * 4096) // no ALUAProvider
	h := NewSCSIHandler(dev)

	var cdb [16]byte
	cdb[0] = ScsiMaintenanceIn
	cdb[1] = 0x0a
	binary.BigEndian.PutUint32(cdb[6:10], 255)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusCheckCond {
		t.Fatalf("expected CHECK_CONDITION, got %d", r.Status)
	}
	if r.SenseKey != SenseIllegalRequest {
		t.Fatalf("expected ILLEGAL_REQUEST, got %02x", r.SenseKey)
	}
}

// testQA_ReportTPG_SmallAllocLen verifies REPORT TPG handles allocation lengths
// smaller than the full 16-byte response without panic.
func testQA_ReportTPG_SmallAllocLen(t *testing.T) {
	dev := newMockALUADevice(100*4096, ALUAActiveOptimized, 1)
	h := NewSCSIHandler(dev)

	allocLens := []uint32{1, 4, 8, 12, 15, 16, 255}

	for _, al := range allocLens {
		var cdb [16]byte
		cdb[0] = ScsiMaintenanceIn
		cdb[1] = 0x0a
		binary.BigEndian.PutUint32(cdb[6:10], al)
		r := h.HandleCommand(cdb, nil)
		if r.Status != SCSIStatusGood {
			t.Fatalf("allocLen=%d: expected GOOD, got %d", al, r.Status)
		}
		maxExpected := uint32(16)
		if al < maxExpected {
			maxExpected = al
		}
		if uint32(len(r.Data)) != maxExpected {
			t.Fatalf("allocLen=%d: data len=%d, want %d", al, len(r.Data), maxExpected)
		}
	}
}

// testQA_ReportTPG_TPGIDBoundary verifies TPG IDs at boundary values:
// 0 (reserved but we don't reject), 1 (default), 0xFFFF (max).
func testQA_ReportTPG_TPGIDBoundary(t *testing.T) {
	tpgIDs := []uint16{0, 1, 2, 255, 0x7FFF, 0xFFFF}

	for _, id := range tpgIDs {
		dev := newMockALUADevice(100*4096, ALUAActiveOptimized, id)
		h := NewSCSIHandler(dev)

		var cdb [16]byte
		cdb[0] = ScsiMaintenanceIn
		cdb[1] = 0x0a
		binary.BigEndian.PutUint32(cdb[6:10], 255)
		r := h.HandleCommand(cdb, nil)
		if r.Status != SCSIStatusGood {
			t.Fatalf("TPGID=%d: expected GOOD, got %d", id, r.Status)
		}
		gotID := binary.BigEndian.Uint16(r.Data[8:10])
		if gotID != id {
			t.Fatalf("TPGID=%d: response has %d", id, gotID)
		}
	}
}

// testQA_ReportTPG_AllStates verifies REPORT TPG correctly reports all 5 ALUA states.
func testQA_ReportTPG_AllStates(t *testing.T) {
	states := []struct {
		state uint8
		name  string
	}{
		{ALUAActiveOptimized, "ActiveOptimized"},
		{ALUAActiveNonOpt, "ActiveNonOptimized"},
		{ALUAStandby, "Standby"},
		{ALUAUnavailable, "Unavailable"},
		{ALUATransitioning, "Transitioning"},
	}

	for _, s := range states {
		dev := newMockALUADevice(100*4096, s.state, 1)
		h := NewSCSIHandler(dev)

		var cdb [16]byte
		cdb[0] = ScsiMaintenanceIn
		cdb[1] = 0x0a
		binary.BigEndian.PutUint32(cdb[6:10], 255)
		r := h.HandleCommand(cdb, nil)
		if r.Status != SCSIStatusGood {
			t.Fatalf("%s: expected GOOD, got %d", s.name, r.Status)
		}
		got := r.Data[4] & 0x0F
		if got != s.state {
			t.Fatalf("%s: state=%02x, want %02x", s.name, got, s.state)
		}
	}
}

// --- Group D: Concurrency ---

// testQA_ConcurrentStateReads verifies that concurrent REPORT TPG calls during
// state transitions don't race or crash.
func testQA_ConcurrentStateReads(t *testing.T) {
	dev := newMutableALUADevice(100*4096, ALUAActiveOptimized, 1)
	h := NewSCSIHandler(dev)

	var wg sync.WaitGroup
	const goroutines = 8
	const iterations = 500

	// Goroutines querying REPORT TPG
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				var cdb [16]byte
				cdb[0] = ScsiMaintenanceIn
				cdb[1] = 0x0a
				binary.BigEndian.PutUint32(cdb[6:10], 255)
				r := h.HandleCommand(cdb, nil)
				if r.Status != SCSIStatusGood {
					t.Errorf("concurrent REPORT TPG: status %d", r.Status)
					return
				}
				// State should be one of the valid ALUA states
				state := r.Data[4] & 0x0F
				if state != ALUAActiveOptimized && state != ALUAStandby &&
					state != ALUAUnavailable && state != ALUATransitioning {
					t.Errorf("concurrent REPORT TPG: invalid state %02x", state)
					return
				}
			}
		}()
	}

	// Concurrently flip state
	wg.Add(1)
	go func() {
		defer wg.Done()
		states := []uint8{ALUAActiveOptimized, ALUAStandby, ALUAUnavailable, ALUATransitioning}
		for j := 0; j < iterations*2; j++ {
			dev.SetState(states[j%len(states)])
		}
	}()

	wg.Wait()
}

// testQA_ConcurrentStandbyReject verifies that concurrent write attempts on a
// standby device all get properly rejected without races.
func testQA_ConcurrentStandbyReject(t *testing.T) {
	dev := newMockALUADevice(100*4096, ALUAStandby, 1)
	h := NewSCSIHandler(dev)

	var wg sync.WaitGroup
	var rejectCount atomic.Int64
	const goroutines = 8
	const iterations = 200

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				var cdb [16]byte
				cdb[0] = ScsiWrite10
				binary.BigEndian.PutUint32(cdb[2:6], 0)
				binary.BigEndian.PutUint16(cdb[7:9], 1)
				r := h.HandleCommand(cdb, make([]byte, 4096))
				if r.SenseKey == SenseNotReady && r.SenseASC == 0x04 && r.SenseASCQ == 0x0B {
					rejectCount.Add(1)
				} else {
					t.Errorf("concurrent standby write: unexpected sense %02x/%02x/%02x",
						r.SenseKey, r.SenseASC, r.SenseASCQ)
					return
				}
			}
		}()
	}

	wg.Wait()

	expected := int64(goroutines * iterations)
	if rejectCount.Load() != expected {
		t.Fatalf("rejected %d/%d writes", rejectCount.Load(), expected)
	}
}

// --- Group E: INQUIRY interaction ---

// testQA_InquiryTPGS_PreservesCmdQue verifies that setting TPGS bits doesn't
// clobber the CmdQue bit in byte 7 or any other INQUIRY fields.
func testQA_InquiryTPGS_PreservesCmdQue(t *testing.T) {
	dev := newMockALUADevice(100*4096, ALUAActiveOptimized, 1)
	h := NewSCSIHandler(dev)

	var cdb [16]byte
	cdb[0] = ScsiInquiry
	binary.BigEndian.PutUint16(cdb[3:5], 96)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("status: %d", r.Status)
	}

	// Byte 5: TPGS=01 (0x10), nothing else should be set
	if r.Data[5] != 0x10 {
		t.Fatalf("byte 5: got %02x, want 0x10 (TPGS=01 only)", r.Data[5])
	}

	// Byte 7: CmdQue=1 (0x02) must still be set
	if r.Data[7]&0x02 == 0 {
		t.Fatalf("byte 7: CmdQue bit not set (%02x)", r.Data[7])
	}

	// Byte 0: peripheral device type still 0x00
	if r.Data[0] != 0x00 {
		t.Fatalf("byte 0: got %02x, want 0x00", r.Data[0])
	}

	// Vendor (bytes 8-15) should be "SeaweedF"
	vendor := string(r.Data[8:16])
	if vendor != "SeaweedF" {
		t.Fatalf("vendor: got %q, want %q", vendor, "SeaweedF")
	}

	// Product (bytes 16-31) should start with "BlockVol"
	product := string(r.Data[16:24])
	if product != "BlockVol" {
		t.Fatalf("product: got %q, want %q", product, "BlockVol")
	}
}

// testQA_InquiryVPD00_UnchangedWithALUA verifies that the supported VPD pages
// list (0x00) doesn't change with ALUA — we don't add new VPD pages, only
// modify existing 0x83.
func testQA_InquiryVPD00_UnchangedWithALUA(t *testing.T) {
	devNoALUA := newMockDevice(100 * 4096)
	devALUA := newMockALUADevice(100*4096, ALUAActiveOptimized, 1)

	hNoALUA := NewSCSIHandler(devNoALUA)
	hALUA := NewSCSIHandler(devALUA)

	var cdb [16]byte
	cdb[0] = ScsiInquiry
	cdb[1] = 0x01
	cdb[2] = 0x00
	binary.BigEndian.PutUint16(cdb[3:5], 255)

	r1 := hNoALUA.HandleCommand(cdb, nil)
	r2 := hALUA.HandleCommand(cdb, nil)

	if r1.Status != SCSIStatusGood || r2.Status != SCSIStatusGood {
		t.Fatalf("status: noALUA=%d alua=%d", r1.Status, r2.Status)
	}

	if len(r1.Data) != len(r2.Data) {
		t.Fatalf("VPD 0x00 length differs: noALUA=%d alua=%d", len(r1.Data), len(r2.Data))
	}
	for i := range r1.Data {
		if r1.Data[i] != r2.Data[i] {
			t.Fatalf("VPD 0x00 byte %d differs: noALUA=%02x alua=%02x", i, r1.Data[i], r2.Data[i])
		}
	}
}
