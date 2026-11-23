package weed_server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/operation/volume_move"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A copy naming a disk type must land on that medium even when the volume's
// index already sits on another disk of the server, which auto-select prefers.
func TestVolumeEcShardsCopy_DiskTypePicksThatMedium(t *testing.T) {
	const collection = "col"
	srcDir := filepath.Join(t.TempDir(), "src")
	source := startEcCopySource(t, srcDir)

	root := t.TempDir()
	ssdDir, hddHtDir := filepath.Join(root, "ssd"), filepath.Join(root, "hdd_ht")
	for _, d := range []string{ssdDir, hddHtDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{ssdDir, hddHtDir}, []int32{100, 100}, []util.MinFreeSpace{{}, {}}, "",
		storage.NeedleMapInMemory, []types.DiskType{types.SsdType, types.ToDiskType("hdd_ht")}, nil, 3, stats.DefaultDiskIOProbeConfig())
	t.Cleanup(store.Close)
	receiver := &VolumeServer{
		store:                         store,
		grpcDialOption:                grpc.WithTransportCredentials(insecure.NewCredentials()),
		AllowUntrustedRemoteEndpoints: true,
	}

	seed := func(vid needle.VolumeId) {
		ecx := make([]byte, types.NeedleMapEntrySize)
		ecx[0] = 1
		srcBase := erasure_coding.EcShardFileName(collection, srcDir, int(vid))
		writeFile(t, srcBase+".ecx", ecx)
		writeFile(t, srcBase+".ec02", bytes.Repeat([]byte{2}, 4096))
		writeFile(t, erasure_coding.EcShardFileName(collection, ssdDir, int(vid))+".ecx", ecx)
	}
	copyShard := func(vid needle.VolumeId, diskId uint32, diskType string) error {
		_, err := receiver.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
			VolumeId:       uint32(vid),
			Collection:     collection,
			ShardIds:       []uint32{2},
			CopyEcxFile:    true,
			SourceDataNode: source,
			DiskId:         diskId,
			DiskType:       diskType,
		})
		return err
	}
	landed := func(dir string, vid needle.VolumeId) bool {
		_, err := os.Stat(erasure_coding.EcShardFileName(collection, dir, int(vid)) + ".ec02")
		return err == nil
	}

	seed(1)
	if err := copyShard(1, 0, ""); err != nil {
		t.Fatalf("copy without a disk type: %v", err)
	}
	if !landed(ssdDir, 1) {
		t.Fatalf("without a disk type the shard should follow the index onto ssd")
	}

	seed(2)
	if err := copyShard(2, 0, "hdd_ht"); err != nil {
		t.Fatalf("copy to hdd_ht: %v", err)
	}
	if !landed(hddHtDir, 2) || landed(ssdDir, 2) {
		t.Fatalf("hdd_ht copy: on hdd_ht=%v, on ssd=%v; want only hdd_ht", landed(hddHtDir, 2), landed(ssdDir, 2))
	}

	seed(3)
	if err := copyShard(3, 1, "ssd"); status.Code(err) != codes.InvalidArgument || !volume_move.IsDiskTypeMismatchError(err) {
		t.Fatalf("disk 1 is hdd_ht, a copy naming ssd: err = %v, want an InvalidArgument disk type mismatch", err)
	}
	if err := copyShard(3, 0, "hdd"); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("no hdd disk: err = %v, want ResourceExhausted", err)
	}
}
