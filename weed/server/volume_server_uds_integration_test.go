package weed_server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// newTestStoreForUds creates a minimal storage.Store for UDS integration tests.
// Caller must close(done) when finished to stop the channel drain goroutine.
func newTestStoreForUds(t *testing.T) (s *storage.Store, done chan struct{}) {
	t.Helper()
	tmpDir := t.TempDir()
	volDir := filepath.Join(tmpDir, "vol")
	if err := os.MkdirAll(volDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	s = storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "",
		[]string{volDir}, []int32{10}, []util.MinFreeSpace{{}}, "",
		storage.NeedleMapInMemory, []types.DiskType{types.HardDriveType}, 3)

	// Drain NewVolumesChan to prevent AddVolume from blocking
	done = make(chan struct{})
	go func() {
		for {
			select {
			case <-s.NewVolumesChan:
			case <-done:
				return
			}
		}
	}()

	return s, done
}

// TestUdsIntegration_LocateWrittenNeedle is an end-to-end test:
//  1. Create a volume store and write a needle
//  2. Start a UDS server backed by a VolumeServer
//  3. Send a Locate request over UDS
//  4. Verify the response contains correct offset, length, and dat_path
func TestUdsIntegration_LocateWrittenNeedle(t *testing.T) {
	store, done := newTestStoreForUds(t)
	defer close(done)

	vid := needle.VolumeId(1)
	err := store.AddVolume(vid, "", storage.NeedleMapInMemory, "000", "",
		0, needle.GetCurrentVersion(), 0, types.HardDriveType, 3)
	if err != nil {
		t.Fatalf("AddVolume: %v", err)
	}

	// Write a needle
	n := new(needle.Needle)
	n.Id = types.Uint64ToNeedleId(0x01)
	n.Cookie = types.Uint32ToCookie(0x12345678)
	n.Data = []byte("hello world integration test data")
	n.Checksum = needle.NewCRC(n.Data)

	_, err = store.WriteVolumeNeedle(vid, n, false, false)
	if err != nil {
		t.Fatalf("WriteVolumeNeedle: %v", err)
	}

	// Construct the fid string the same way SeaweedFS does
	fileId := needle.NewFileIdFromNeedle(vid, n)
	fid := fileId.String() // e.g. "1,0112345678"

	// Create a minimal VolumeServer with just the store field
	vs := &VolumeServer{
		store: store,
	}

	socketPath := filepath.Join(t.TempDir(), "test.sock")
	uds, err := NewUdsServer(vs, socketPath)
	if err != nil {
		t.Fatalf("NewUdsServer: %v", err)
	}
	defer uds.Stop()
	uds.Start()
	time.Sleep(10 * time.Millisecond)

	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	// Send Locate request
	reqBuf := make([]byte, UdsRequestSize)
	reqBuf[0] = 1 // opcode = Locate
	binary.LittleEndian.PutUint32(reqBuf[4:8], 42)

	// Copy fid into the 16-byte field (strip "vol," prefix — keep just keyCookie part? No — the UDS handler parses the full "vol,keyCookie" fid)
	if len(fid) > 16 {
		t.Fatalf("fid %q too long for 16-byte field", fid)
	}
	copy(reqBuf[8:24], fid)

	if _, err := conn.Write(reqBuf); err != nil {
		t.Fatalf("Write request: %v", err)
	}

	// Read 32-byte response header
	respBuf := make([]byte, UdsResponseSize)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		t.Fatalf("Read response: %v", err)
	}

	status := respBuf[0]
	if status != UdsStatusOk {
		t.Fatalf("Expected UdsStatusOk (0), got %d", status)
	}

	volumeId := binary.LittleEndian.Uint32(respBuf[4:8])
	if volumeId != uint32(vid) {
		t.Errorf("volumeId: expected %d, got %d", vid, volumeId)
	}

	offset := binary.LittleEndian.Uint64(respBuf[8:16])
	if offset == 0 {
		t.Error("Expected non-zero offset (super block occupies the first bytes)")
	}

	length := binary.LittleEndian.Uint64(respBuf[16:24])
	if length == 0 {
		t.Error("Expected non-zero length")
	}

	datPathLen := binary.LittleEndian.Uint16(respBuf[24:26])
	if datPathLen == 0 {
		t.Fatal("Expected non-zero dat_path_len")
	}

	// Read variable-length dat_path
	datPathBuf := make([]byte, datPathLen)
	if _, err := io.ReadFull(conn, datPathBuf); err != nil {
		t.Fatalf("Read dat_path: %v", err)
	}
	datPath := string(datPathBuf)

	if !strings.HasSuffix(datPath, ".dat") {
		t.Errorf("dat_path should end with .dat, got %q", datPath)
	}

	t.Logf("Locate %s -> offset=%d length=%d dat_path=%s", fid, offset, length, datPath)
}

// TestUdsIntegration_LocateNotFound verifies that looking up a non-existent needle returns NotFound.
func TestUdsIntegration_LocateNotFound(t *testing.T) {
	store, done := newTestStoreForUds(t)
	defer close(done)

	vid := needle.VolumeId(1)
	err := store.AddVolume(vid, "", storage.NeedleMapInMemory, "000", "",
		0, needle.GetCurrentVersion(), 0, types.HardDriveType, 3)
	if err != nil {
		t.Fatalf("AddVolume: %v", err)
	}

	vs := &VolumeServer{
		store: store,
	}

	socketPath := filepath.Join(t.TempDir(), "test.sock")
	uds, err := NewUdsServer(vs, socketPath)
	if err != nil {
		t.Fatalf("NewUdsServer: %v", err)
	}
	defer uds.Stop()
	uds.Start()
	time.Sleep(10 * time.Millisecond)

	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	// Locate a needle that was never written
	reqBuf := make([]byte, UdsRequestSize)
	reqBuf[0] = 1
	binary.LittleEndian.PutUint32(reqBuf[4:8], 1)
	copy(reqBuf[8:24], "1,ffaabbccddee")

	if _, err := conn.Write(reqBuf); err != nil {
		t.Fatalf("Write: %v", err)
	}

	respBuf := make([]byte, UdsResponseSize)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		t.Fatalf("Read: %v", err)
	}

	status := respBuf[0]
	if status != UdsStatusNotFound {
		t.Errorf("Expected UdsStatusNotFound (%d), got %d", UdsStatusNotFound, status)
	}
}

// TestUdsIntegration_MultipleRequests verifies that multiple sequential requests on the same connection work.
func TestUdsIntegration_MultipleRequests(t *testing.T) {
	store, done := newTestStoreForUds(t)
	defer close(done)

	vid := needle.VolumeId(1)
	err := store.AddVolume(vid, "", storage.NeedleMapInMemory, "000", "",
		0, needle.GetCurrentVersion(), 0, types.HardDriveType, 3)
	if err != nil {
		t.Fatalf("AddVolume: %v", err)
	}

	// Write 3 needles
	type needleInfo struct {
		n   *needle.Needle
		fid string
	}
	needles := make([]needleInfo, 3)
	for i := 0; i < 3; i++ {
		n := new(needle.Needle)
		n.Id = types.Uint64ToNeedleId(uint64(i + 1))
		n.Cookie = types.Uint32ToCookie(uint32(0xAABBCC00 + i))
		n.Data = []byte(fmt.Sprintf("needle data %d", i))
		n.Checksum = needle.NewCRC(n.Data)

		if _, err := store.WriteVolumeNeedle(vid, n, false, false); err != nil {
			t.Fatalf("WriteVolumeNeedle[%d]: %v", i, err)
		}
		fileId := needle.NewFileIdFromNeedle(vid, n)
		needles[i] = needleInfo{n: n, fid: fileId.String()}
	}

	vs := &VolumeServer{store: store}
	socketPath := filepath.Join(t.TempDir(), "test.sock")
	uds, err := NewUdsServer(vs, socketPath)
	if err != nil {
		t.Fatalf("NewUdsServer: %v", err)
	}
	defer uds.Stop()
	uds.Start()
	time.Sleep(10 * time.Millisecond)

	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	// Send 3 Locate requests sequentially on the same connection
	for i, ni := range needles {
		reqBuf := make([]byte, UdsRequestSize)
		reqBuf[0] = 1
		binary.LittleEndian.PutUint32(reqBuf[4:8], uint32(i+1))
		copy(reqBuf[8:24], ni.fid)

		if _, err := conn.Write(reqBuf); err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}

		respBuf := make([]byte, UdsResponseSize)
		if _, err := io.ReadFull(conn, respBuf); err != nil {
			t.Fatalf("Read[%d]: %v", i, err)
		}

		status := respBuf[0]
		if status != UdsStatusOk {
			t.Errorf("needle[%d] fid=%s: expected UdsStatusOk, got %d", i, ni.fid, status)
			continue
		}

		length := binary.LittleEndian.Uint64(respBuf[16:24])
		if length == 0 {
			t.Errorf("needle[%d]: expected non-zero length", i)
		}

		// Consume the dat_path bytes
		datPathLen := binary.LittleEndian.Uint16(respBuf[24:26])
		if datPathLen > 0 {
			datPathBuf := make([]byte, datPathLen)
			if _, err := io.ReadFull(conn, datPathBuf); err != nil {
				t.Fatalf("Read dat_path[%d]: %v", i, err)
			}
		}
	}
}
