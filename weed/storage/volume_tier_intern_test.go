package storage

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// Every tiered volume names the same backend and replication, so a server
// holding millions of them must end up with one copy of each. The sharing has
// to hold across a collection: a table that lets its entries go hands the next
// volume a second copy. The remote key names a single volume and is left alone.
func TestVolumeInfoSharesTheStringsEveryVolumeRepeats(t *testing.T) {
	dir := t.TempDir()
	load := func(name, key string) *volume_server_pb.VolumeInfo {
		t.Helper()
		path := dir + "/" + name
		if err := volume_info.SaveVolumeInfo(path, &volume_server_pb.VolumeInfo{
			Version:     3,
			Replication: "001",
			Files: []*volume_server_pb.RemoteFile{{
				BackendType: "s3", BackendId: "cold", Extension: ".dat", Key: key,
			}},
		}); err != nil {
			t.Fatal(err)
		}
		loaded, _, found, err := volume_info.MaybeLoadVolumeInfo(path)
		if err != nil || !found {
			t.Fatalf("load %s: found=%v err=%v", name, found, err)
		}
		internVolumeInfoStrings(loaded)
		return loaded
	}

	first := load("1.vif", "one")
	runtime.GC()
	second := load("2.vif", "two")

	shared := func(what, a, b string) {
		t.Helper()
		if a != b {
			t.Fatalf("%s read back as %q then %q", what, a, b)
		}
		if unsafe.StringData(a) != unsafe.StringData(b) {
			t.Errorf("%s was kept twice instead of shared", what)
		}
	}
	shared("replication", first.Replication, second.Replication)
	shared("backend type", first.Files[0].BackendType, second.Files[0].BackendType)
	shared("backend id", first.Files[0].BackendId, second.Files[0].BackendId)
	shared("extension", first.Files[0].Extension, second.Files[0].Extension)

	if first.Files[0].Key != "one" || second.Files[0].Key != "two" {
		t.Errorf("remote keys came back as %q and %q", first.Files[0].Key, second.Files[0].Key)
	}
}
