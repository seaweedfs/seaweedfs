package weed_server

import (
	"bytes"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// A delete on a tiered volume appends an offset-0 tombstone to .idx with no
// .dat record. The incremental copy has to carry that row: regenerating the
// backup's index from .dat alone resurrects the needle, and the row-count
// skew makes every later backup diverge and rebuild.
func TestIncrementalCopyCarriesRemoteTierTombstone(t *testing.T) {
	store := tierUploadForDeletes(t, "000")
	const vid = needle.VolumeId(1)
	if _, err := store.DeleteVolumeNeedle(vid, &needle.Needle{Id: 2}); err != nil {
		t.Fatalf("delete on tiered volume: %v", err)
	}
	source := store.GetVolume(vid)
	wantIdxSize := source.IndexFileSize()

	port := serveGrpc(t, func(server *grpc.Server) {
		volume_server_pb.RegisterVolumeServerServer(server, &VolumeServer{store: store})
	})
	serverAddress := pb.NewServerAddressWithGrpcPort("127.0.0.1:0", port)
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())

	backupDir := t.TempDir()
	backup, err := storage.NewVolume(backupDir, backupDir, "", vid, storage.NeedleMapInMemory,
		&super_block.ReplicaPlacement{}, &needle.TTL{}, 0, source.Version(), 0, 0)
	if err != nil {
		t.Fatalf("backup volume: %v", err)
	}

	if err := backup.IncrementalBackup(serverAddress, dialOption); err != nil {
		t.Fatalf("incremental backup: %v", err)
	}
	if got := backup.IndexFileSize(); got != wantIdxSize {
		t.Fatalf("backup idx size %d, want %d", got, wantIdxSize)
	}
	sourceIdx, err := os.ReadFile(source.FileName(".idx"))
	if err != nil {
		t.Fatalf("read source idx: %v", err)
	}
	backupIdx, err := os.ReadFile(backup.FileName(".idx"))
	if err != nil {
		t.Fatalf("read backup idx: %v", err)
	}
	if !bytes.Equal(backupIdx, sourceIdx) {
		t.Fatalf("backup idx differs from the replica's")
	}

	idxFile, err := os.Open(backup.FileName(".idx"))
	if err != nil {
		t.Fatalf("open backup idx: %v", err)
	}
	seenDeleted := false
	err = idx.WalkIndexFile(idxFile, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		if key == 2 && (offset.IsZero() || size.IsDeleted()) {
			seenDeleted = true
		}
		return nil
	})
	idxFile.Close()
	if err != nil {
		t.Fatalf("walk backup idx: %v", err)
	}
	if !seenDeleted {
		t.Fatalf("backup idx has no tombstone for the deleted needle")
	}
	backup.Close()

	checkStore := newVolumeCopyTestStore(t, backupDir)
	checkVolume := checkStore.GetVolume(vid)
	if checkVolume == nil {
		t.Fatalf("reloaded backup volume not found")
	}
	if _, err := checkStore.ReadVolumeNeedle(vid, &needle.Needle{Id: 2}, nil, nil); err == nil {
		t.Fatalf("the deleted needle reads on the backup")
	}
	if _, err := checkStore.ReadVolumeNeedle(vid, &needle.Needle{Id: 3}, nil, nil); err != nil {
		t.Fatalf("a live needle does not read on the backup: %v", err)
	}

	// the copy now ends on the tombstone row; the next run stays a no-op
	// instead of diverging into a rebuild
	if err := checkVolume.IncrementalBackup(serverAddress, dialOption); err != nil {
		t.Fatalf("second incremental backup: %v", err)
	}

	// a delete landing after the copy rides the index stream alone: the run
	// appends just the tombstone row and the needle stays deleted
	if _, err := store.DeleteVolumeNeedle(vid, &needle.Needle{Id: 3}); err != nil {
		t.Fatalf("second delete on tiered volume: %v", err)
	}
	if err := checkVolume.IncrementalBackup(serverAddress, dialOption); err != nil {
		t.Fatalf("incremental backup over a new tombstone: %v", err)
	}
	sourceIdx, err = os.ReadFile(source.FileName(".idx"))
	if err != nil {
		t.Fatalf("re-read source idx: %v", err)
	}
	backupIdx, err = os.ReadFile(checkVolume.FileName(".idx"))
	if err != nil {
		t.Fatalf("re-read backup idx: %v", err)
	}
	if !bytes.Equal(backupIdx, sourceIdx) {
		t.Fatalf("backup idx differs from the replica's after a new tombstone")
	}
	if _, err := checkStore.ReadVolumeNeedle(vid, &needle.Needle{Id: 3}, nil, nil); err == nil {
		t.Fatalf("the newly deleted needle still reads on the backup")
	}
}
