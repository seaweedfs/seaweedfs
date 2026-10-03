package weed_server

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// A volume that mounts on a journal ReceiveFile is still writing must not
// compact it: the rest of the stream would land in the replaced inode and be
// gone at the next mount.
func TestReceiveFile_EcjStreamBlocksMountCompaction(t *testing.T) {
	storeDir := t.TempDir()
	// The store first, so its startup scan finds no volume to mount and
	// ReceiveFile's mounted check lets the .ecj through.
	vs := &VolumeServer{store: newTraversalTestStore(storeDir)}

	base := filepath.Join(storeDir, "4")
	ecx := make([]byte, types.NeedleMapEntrySize)
	types.NeedleIdToBytes(ecx[0:types.NeedleIdSize], 7)
	types.OffsetToBytes(ecx[types.NeedleIdSize:types.NeedleIdSize+types.OffsetSize], types.ToOffset(8))
	types.SizeToBytes(ecx[types.NeedleIdSize+types.OffsetSize:], types.Size(10))
	if err := os.WriteFile(base+".ecx", ecx, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(base+".vif", nil, 0o644); err != nil {
		t.Fatal(err)
	}

	// A bloated prefix (100 ids, 4096 times over) the mount would compact,
	// then an id only the second chunk carries.
	one := make([]byte, 100*types.NeedleIdSize)
	for i := 0; i < 100; i++ {
		types.NeedleIdToBytes(one[i*types.NeedleIdSize:], types.NeedleId(1000+i))
	}
	bloated := bytes.Repeat(one, 4096)
	tail := make([]byte, types.NeedleIdSize)
	types.NeedleIdToBytes(tail, 5000)

	var mounted *erasure_coding.EcVolume
	stream := &fakeReceiveFileStream{
		reqs: []*volume_server_pb.ReceiveFileRequest{
			infoReq(&volume_server_pb.ReceiveFileInfo{
				VolumeId:   4,
				Ext:        ".ecj",
				IsEcVolume: true,
				FileSize:   uint64(len(bloated) + len(tail)),
			}),
			contentReq(bloated),
			contentReq(tail),
		},
		onRecv: func(i int) {
			if i != 2 {
				return
			}
			// The first chunk is on disk and the second is not yet sent.
			ev, err := erasure_coding.NewEcVolume(types.HardDriveType, storeDir, storeDir, "", 4)
			if err != nil {
				t.Fatalf("mount during the stream: %v", err)
			}
			mounted = ev
		},
	}

	if err := vs.ReceiveFile(stream); err != nil {
		t.Fatalf("ReceiveFile: %v", err)
	}
	if stream.resp == nil || stream.resp.Error != "" {
		t.Fatalf("ReceiveFile rejected the journal: %+v", stream.resp)
	}
	if mounted == nil {
		t.Fatal("the mount hook never ran")
	}
	mounted.Close()

	got, err := os.ReadFile(base + ".ecj")
	if err != nil {
		t.Fatal(err)
	}
	if want := append(bloated, tail...); !bytes.Equal(got, want) {
		t.Fatalf("journal on disk is %d bytes, want the %d the stream sent", len(got), len(want))
	}
}
