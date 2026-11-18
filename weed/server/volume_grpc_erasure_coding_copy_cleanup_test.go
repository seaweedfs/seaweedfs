package weed_server

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// startEcCopySource serves srcDir's EC files over CopyFile and returns the
// address a VolumeEcShardsCopy names as its source.
func startEcCopySource(t *testing.T, srcDir string) string {
	t.Helper()
	srcStore := newRecoverTestStore(t, srcDir)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(grpcServer, &VolumeServer{store: srcStore})
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)
	port := lis.Addr().(*net.TCPAddr).Port
	return string(pb.NewServerAddress("127.0.0.1", port-10000, port))
}

func newEcCopyReceiver(t *testing.T, dstDir string) *VolumeServer {
	t.Helper()
	return &VolumeServer{
		store:                         newRecoverTestStore(t, dstDir),
		grpcDialOption:                grpc.WithTransportCredentials(insecure.NewCredentials()),
		AllowUntrustedRemoteEndpoints: true,
	}
}

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// A source whose .ecx is 0 bytes must fail the copy without stranding a shard,
// index or sidecar on the destination, and without touching what an existing
// holder of the same volume already has there.
func TestVolumeEcShardsCopy_EmptySourceEcxLeavesNoFiles(t *testing.T) {
	const collection = "col"
	vid := needle.VolumeId(57)

	srcDir := filepath.Join(t.TempDir(), "src")
	source := startEcCopySource(t, srcDir)
	srcBase := erasure_coding.EcShardFileName(collection, srcDir, int(vid))
	writeFile(t, srcBase+".ecx", nil)
	writeFile(t, srcBase+".ec00", bytes.Repeat([]byte{1}, 4096))
	writeFile(t, srcBase+".ecj", nil)
	writeFile(t, srcBase+".vif", []byte("{}"))

	req := &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       uint32(vid),
		Collection:     collection,
		ShardIds:       []uint32{0},
		CopyEcxFile:    true,
		CopyEcjFile:    true,
		CopyVifFile:    true,
		CopyEcsumFile:  true,
		SourceDataNode: source,
	}

	t.Run("fresh destination", func(t *testing.T) {
		dstDir := filepath.Join(t.TempDir(), "dst")
		receiver := newEcCopyReceiver(t, dstDir)
		before := listDir(t, dstDir)

		_, err := receiver.VolumeEcShardsCopy(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), "source .ecx is 0 bytes") {
			t.Fatalf("VolumeEcShardsCopy err = %v, want source .ecx is 0 bytes", err)
		}
		if after := listDir(t, dstDir); strings.Join(after, ",") != strings.Join(before, ",") {
			t.Fatalf("destination files after failed copy = %v, want %v", after, before)
		}
	})

	t.Run("existing holder", func(t *testing.T) {
		dstDir := filepath.Join(t.TempDir(), "dst")
		receiver := newEcCopyReceiver(t, dstDir)
		dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
		ecx := make([]byte, types.NeedleMapEntrySize)
		ecx[0] = 7
		ec05 := bytes.Repeat([]byte{5}, 4096)
		writeFile(t, dstBase+".ecx", ecx)
		writeFile(t, dstBase+".ec05", ec05)
		before := listDir(t, dstDir)

		if _, err := receiver.VolumeEcShardsCopy(context.Background(), req); err == nil {
			t.Fatalf("VolumeEcShardsCopy succeeded against a 0-byte source .ecx")
		}
		if after := listDir(t, dstDir); strings.Join(after, ",") != strings.Join(before, ",") {
			t.Fatalf("destination files after failed copy = %v, want %v", after, before)
		}
		if got, _ := os.ReadFile(dstBase + ".ecx"); !bytes.Equal(got, ecx) {
			t.Errorf("pre-existing .ecx changed: %x, want %x", got, ecx)
		}
		if got, _ := os.ReadFile(dstBase + ".ec05"); !bytes.Equal(got, ec05) {
			t.Errorf("pre-existing .ec05 changed")
		}
	})
}

// A copy failing after some shards landed removes exactly those shards and
// keeps the ones the destination held before the call, its index included.
func TestVolumeEcShardsCopy_FailedShardRemovesCopiedShards(t *testing.T) {
	const collection = "col"
	vid := needle.VolumeId(58)

	srcDir := filepath.Join(t.TempDir(), "src")
	source := startEcCopySource(t, srcDir)
	srcBase := erasure_coding.EcShardFileName(collection, srcDir, int(vid))
	srcEcx := make([]byte, types.NeedleMapEntrySize)
	srcEcx[0] = 9
	writeFile(t, srcBase+".ecx", srcEcx)
	writeFile(t, srcBase+".ec00", bytes.Repeat([]byte{1}, 4096))
	writeFile(t, srcBase+".ec05", bytes.Repeat([]byte{2}, 4096))

	dstDir := filepath.Join(t.TempDir(), "dst")
	receiver := newEcCopyReceiver(t, dstDir)
	dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
	ecx := make([]byte, types.NeedleMapEntrySize)
	ecx[0] = 7
	writeFile(t, dstBase+".ecx", ecx)
	ec05 := bytes.Repeat([]byte{5}, 4096)
	writeFile(t, dstBase+".ec05", ec05)
	ec01 := bytes.Repeat([]byte{6}, 4096)
	writeFile(t, dstBase+".ec01", ec01)
	before := listDir(t, dstDir)

	// Shard 0 copies, shard 5 replaces the existing holder's copy, shard 1 is
	// held here but missing on the source and fails the request.
	_, err := receiver.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       uint32(vid),
		Collection:     collection,
		ShardIds:       []uint32{0, 5, 1},
		CopyEcxFile:    true,
		SourceDataNode: source,
	})
	if err == nil {
		t.Fatalf("VolumeEcShardsCopy succeeded although shard 1 is missing on the source")
	}
	if after := listDir(t, dstDir); strings.Join(after, ",") != strings.Join(before, ",") {
		t.Fatalf("destination files after failed copy = %v, want %v", after, before)
	}
	if got, _ := os.ReadFile(dstBase + ".ecx"); !bytes.Equal(got, ecx) {
		t.Errorf("pre-existing .ecx changed: %x, want %x", got, ecx)
	}
	if got, _ := os.ReadFile(dstBase + ".ec05"); !bytes.Equal(got, ec05) {
		t.Errorf("pre-existing .ec05 changed")
	}
	if got, _ := os.ReadFile(dstBase + ".ec01"); !bytes.Equal(got, ec01) {
		t.Errorf("pre-existing .ec01 changed by its failed transfer")
	}
}

// A copied index replaces an existing holder's in place, so a mounted
// EcVolume's open handle reads the source's index, tombstones included.
func TestVolumeEcShardsCopy_ExistingEcxUpdatedInPlace(t *testing.T) {
	const collection = "col"
	vid := needle.VolumeId(59)

	srcDir := filepath.Join(t.TempDir(), "src")
	source := startEcCopySource(t, srcDir)
	srcBase := erasure_coding.EcShardFileName(collection, srcDir, int(vid))
	srcEcx := bytes.Repeat([]byte{9}, 2*types.NeedleMapEntrySize)
	writeFile(t, srcBase+".ecx", srcEcx)
	srcEc00 := bytes.Repeat([]byte{1}, 4096)
	writeFile(t, srcBase+".ec00", srcEc00)
	srcVif := []byte(`{"version":3}`)
	writeFile(t, srcBase+".vif", srcVif)

	dstDir := filepath.Join(t.TempDir(), "dst")
	receiver := newEcCopyReceiver(t, dstDir)
	dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
	writeFile(t, dstBase+".ecx", make([]byte, 2*types.NeedleMapEntrySize))
	writeFile(t, dstBase+".ec00", bytes.Repeat([]byte{3}, 1024))
	held, err := os.Open(dstBase + ".ecx")
	if err != nil {
		t.Fatalf("open .ecx: %v", err)
	}
	defer held.Close()

	if _, err := receiver.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       uint32(vid),
		Collection:     collection,
		ShardIds:       []uint32{0},
		CopyEcxFile:    true,
		CopyVifFile:    true,
		CopyEcsumFile:  true,
		SourceDataNode: source,
	}); err != nil {
		t.Fatalf("VolumeEcShardsCopy: %v", err)
	}
	got := make([]byte, len(srcEcx))
	if _, err := held.ReadAt(got, 0); err != nil {
		t.Fatalf("read open .ecx: %v", err)
	}
	if !bytes.Equal(got, srcEcx) {
		t.Fatalf("open .ecx handle reads %x after the copy, want the source index %x", got, srcEcx)
	}
	if got, _ := os.ReadFile(dstBase + ".ec00"); !bytes.Equal(got, srcEc00) {
		t.Errorf("existing .ec00 not replaced by the source's")
	}
	if got, _ := os.ReadFile(dstBase + ".vif"); !bytes.Equal(got, srcVif) {
		t.Errorf("copied .vif = %s, want %s", got, srcVif)
	}
	if _, err := os.Stat(dstBase + erasure_coding.BitrotSidecarExt); !os.IsNotExist(err) {
		t.Errorf("a source without .ecsum left one here: %v", err)
	}
	for _, name := range listDir(t, dstDir) {
		if strings.HasSuffix(name, ".copying") {
			t.Errorf("staged file %s left behind", name)
		}
	}
}

// A copy failing after the .vif transferred keeps the existing holder's .vif,
// which must stay paired with the index the failed copy left in place.
func TestVolumeEcShardsCopy_FailedSidecarKeepsExistingVif(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode-000 file, so the sidecar transfer cannot be made to fail")
	}
	const collection = "col"
	vid := needle.VolumeId(60)

	srcDir := filepath.Join(t.TempDir(), "src")
	source := startEcCopySource(t, srcDir)
	srcBase := erasure_coding.EcShardFileName(collection, srcDir, int(vid))
	writeFile(t, srcBase+".ecx", bytes.Repeat([]byte{9}, types.NeedleMapEntrySize))
	writeFile(t, srcBase+".ec00", bytes.Repeat([]byte{1}, 4096))
	writeFile(t, srcBase+".vif", []byte(`{"version":3}`))
	writeFile(t, srcBase+erasure_coding.BitrotSidecarExt, []byte("sum"))
	if err := os.Chmod(srcBase+erasure_coding.BitrotSidecarExt, 0); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	dstDir := filepath.Join(t.TempDir(), "dst")
	receiver := newEcCopyReceiver(t, dstDir)
	dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
	ecx := make([]byte, types.NeedleMapEntrySize)
	writeFile(t, dstBase+".ecx", ecx)
	vif := []byte(`{"version":2}`)
	writeFile(t, dstBase+".vif", vif)
	before := listDir(t, dstDir)

	if _, err := receiver.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       uint32(vid),
		Collection:     collection,
		ShardIds:       []uint32{0},
		CopyEcxFile:    true,
		CopyVifFile:    true,
		CopyEcsumFile:  true,
		SourceDataNode: source,
	}); err == nil {
		t.Fatalf("VolumeEcShardsCopy succeeded although the source .ecsum is unreadable")
	}
	if after := listDir(t, dstDir); strings.Join(after, ",") != strings.Join(before, ",") {
		t.Fatalf("destination files after failed copy = %v, want %v", after, before)
	}
	if got, _ := os.ReadFile(dstBase + ".vif"); !bytes.Equal(got, vif) {
		t.Errorf("pre-existing .vif changed to %s, want %s", got, vif)
	}
	if got, _ := os.ReadFile(dstBase + ".ecx"); !bytes.Equal(got, ecx) {
		t.Errorf("pre-existing .ecx changed")
	}
}
