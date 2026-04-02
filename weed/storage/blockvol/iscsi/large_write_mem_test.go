package iscsi

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// slowDevice wraps a device and adds configurable delay to WriteAt.
// This simulates a real blockvol that blocks on WAL-full during large writes.
type slowDevice struct {
	inner      BlockDevice
	writeDelay time.Duration
	writeCalls atomic.Int64
}

func (d *slowDevice) ReadAt(lba uint64, length uint32) ([]byte, error) {
	return d.inner.ReadAt(lba, length)
}

func (d *slowDevice) WriteAt(lba uint64, data []byte) error {
	d.writeCalls.Add(1)
	if d.writeDelay > 0 {
		time.Sleep(d.writeDelay)
	}
	return d.inner.WriteAt(lba, data)
}

func (d *slowDevice) Trim(lba uint64, length uint32) error    { return d.inner.Trim(lba, length) }
func (d *slowDevice) SyncCache() error                         { return d.inner.SyncCache() }
func (d *slowDevice) BlockSize() uint32                        { return d.inner.BlockSize() }
func (d *slowDevice) VolumeSize() uint64                       { return d.inner.VolumeSize() }
func (d *slowDevice) IsHealthy() bool                          { return true }

// TestLargeWriteMemory_4MB sends 4MB WRITE(10) commands through a real
// iSCSI session and measures heap growth. This is the in-process version
// of the hardware test that showed 25GB RSS.
func TestLargeWriteMemory_4MB(t *testing.T) {
	// Use a mock device with 64MB (16K blocks).
	mockDev := newMockDevice(64 * 1024 * 1024)
	dev := &slowDevice{inner: mockDev, writeDelay: 0}

	client, server := net.Pipe()
	defer client.Close()

	config := DefaultTargetConfig()
	config.TargetName = testTargetName
	config.MaxRecvDataSegmentLength = 262144 // 256KB
	config.MaxBurstLength = 4 * 1024 * 1024  // 4MB — allow full 4MB burst
	config.FirstBurstLength = 65536           // 64KB immediate
	config.ImmediateData = true
	config.InitialR2T = true

	resolver := newTestResolverWithDevice(dev)
	logger := log.New(io.Discard, "", 0)

	sess := NewSession(server, config, resolver, resolver, logger)
	done := make(chan error, 1)
	go func() { done <- sess.HandleConnection() }()
	defer func() { client.Close(); <-done }()

	doLogin(t, client)

	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := int64(m.HeapAlloc)

	// Send 10 × 4MB WRITE(10) commands with immediate data + R2T flow.
	writeSize := 4 * 1024 * 1024
	data := make([]byte, writeSize)
	for i := range data {
		data[i] = byte(i)
	}

	cmdSN := uint32(0)
	for i := 0; i < 10; i++ {
		lba := uint32(i * 1024) // 4MB = 1024 blocks of 4KB
		blocks := uint16(1024)

		// Send SCSI WRITE(10) with immediate data (first 64KB).
		cmd := &PDU{}
		cmd.SetOpcode(OpSCSICmd)
		cmd.SetOpSpecific1(FlagF | FlagW)
		cmd.SetInitiatorTaskTag(uint32(i + 1))
		cmd.SetExpectedDataTransferLength(uint32(writeSize))
		cmd.SetCmdSN(cmdSN)
		cmdSN++

		var cdb [16]byte
		cdb[0] = ScsiWrite10
		binary.BigEndian.PutUint32(cdb[2:6], lba)
		binary.BigEndian.PutUint16(cdb[7:9], blocks)
		cmd.SetCDB(cdb)

		// Immediate data: first 64KB.
		immLen := 65536
		cmd.DataSegment = data[:immLen]

		if err := WritePDU(client, cmd); err != nil {
			t.Fatalf("write cmd %d: %v", i, err)
		}

		// Handle R2T + Data-Out for remaining data.
		sent := immLen
		for sent < writeSize {
			// Read R2T.
			r2t, err := ReadPDU(client)
			if err != nil {
				t.Fatalf("read R2T %d: %v", i, err)
			}
			if r2t.Opcode() == OpSCSIResp {
				status := r2t.SCSIStatus()
				t.Fatalf("write %d: early SCSI response status=0x%02x (sent %d/%d)", i, status, sent, writeSize)
			}
			if r2t.Opcode() != OpR2T {
				t.Fatalf("write %d: expected R2T, got %s", i, OpcodeName(r2t.Opcode()))
			}

			desiredLen := int(r2t.DesiredDataLength())
			ttt := r2t.TargetTransferTag()

			// Send Data-Out PDUs in MaxRecvDataSegmentLength chunks.
			seqSent := 0
			dataSN := uint32(0)
			for seqSent < desiredLen && sent < writeSize {
				chunk := config.MaxRecvDataSegmentLength
				if desiredLen-seqSent < chunk {
					chunk = desiredLen - seqSent
				}
				if writeSize-sent < chunk {
					chunk = writeSize - sent
				}

				doPDU := &PDU{}
				doPDU.SetOpcode(OpSCSIDataOut)
				doPDU.SetInitiatorTaskTag(uint32(i + 1))
				doPDU.SetTargetTransferTag(ttt)
				doPDU.SetBufferOffset(uint32(sent))
				doPDU.SetDataSN(dataSN)
				dataSN++
				doPDU.DataSegment = data[sent : sent+chunk]

				if seqSent+chunk >= desiredLen || sent+chunk >= writeSize {
					doPDU.SetOpSpecific1(FlagF) // Final
				}

				if err := WritePDU(client, doPDU); err != nil {
					t.Fatalf("write data-out %d: %v", i, err)
				}

				sent += chunk
				seqSent += chunk
			}
		}

		// Read SCSI Response.
		client.SetReadDeadline(time.Now().Add(10 * time.Second))
		resp, err := ReadPDU(client)
		client.SetReadDeadline(time.Time{})
		if err != nil {
			t.Fatalf("read response %d: %v", i, err)
		}
		if resp.Opcode() != OpSCSIResp {
			t.Fatalf("write %d: expected SCSIResp, got %s", i, OpcodeName(resp.Opcode()))
		}
		if resp.SCSIStatus() != SCSIStatusGood {
			t.Fatalf("write %d: status=0x%02x", i, resp.SCSIStatus())
		}

		runtime.GC()
		runtime.ReadMemStats(&m)
		heap := int64(m.HeapAlloc)
		t.Logf("write %d: heap=%d MB (delta=%d MB)", i, heap/(1024*1024), (heap-heapBefore)/(1024*1024))
	}

	runtime.GC()
	runtime.ReadMemStats(&m)
	heapAfter := int64(m.HeapAlloc)
	deltaMB := (heapAfter - heapBefore) / (1024 * 1024)
	t.Logf("final: heap=%d MB, delta=%d MB, writes=%d", heapAfter/(1024*1024), deltaMB, dev.writeCalls.Load())

	if deltaMB > 200 {
		t.Errorf("MEMORY LEAK: heap grew %d MB for 10 × 4MB iSCSI writes", deltaMB)
	}
}

// TestLargeWriteMemory_SlowDevice simulates WAL-full blocking: writes
// take 100ms each (as if WAL admission is throttling). This keeps buffers
// alive longer and tests if they accumulate.
func TestLargeWriteMemory_SlowDevice(t *testing.T) {
	mockDev := newMockDevice(64 * 1024 * 1024)
	dev := &slowDevice{inner: mockDev, writeDelay: 100 * time.Millisecond}

	client, server := net.Pipe()
	defer client.Close()

	config := DefaultTargetConfig()
	config.TargetName = testTargetName
	config.ImmediateData = true
	config.InitialR2T = false // allow full immediate data

	resolver := newTestResolverWithDevice(dev)
	logger := log.New(io.Discard, "", 0)

	sess := NewSession(server, config, resolver, resolver, logger)
	doneCh := make(chan error, 1)
	go func() { doneCh <- sess.HandleConnection() }()
	defer func() { client.Close(); <-doneCh }()

	doLogin(t, client)

	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := int64(m.HeapAlloc)

	// Send many 4KB writes rapidly (simulating inode table writes).
	cmdSN := uint32(0)
	for i := 0; i < 200; i++ {
		sendSCSIWriteImmediate(t, client, uint32(i), bytes.Repeat([]byte{0xBB}, 4096), uint32(i+1), cmdSN)
		cmdSN++

		resp, err := ReadPDU(client)
		if err != nil {
			t.Fatalf("write %d response: %v", i, err)
		}
		if resp.SCSIStatus() != SCSIStatusGood {
			t.Fatalf("write %d: status=0x%02x", i, resp.SCSIStatus())
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&m)
	heapAfter := int64(m.HeapAlloc)
	delta := (heapAfter - heapBefore) / (1024 * 1024)
	t.Logf("200 × 4KB writes with 100ms delay: heap delta=%d MB, writes=%d", delta, dev.writeCalls.Load())

	if delta > 100 {
		t.Errorf("MEMORY LEAK with slow device: heap grew %d MB", delta)
	}
}
