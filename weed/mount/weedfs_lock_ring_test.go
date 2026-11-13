package mount

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestEntryWriteOwnerMatchesFilerRouting(t *testing.T) {
	wfs := &WFS{}
	path := util.FullPath("/dir/file")
	if got := wfs.entryWriteOwner(path); got != "" {
		t.Fatalf("no lock client: owner %q, want none", got)
	}

	wfs.lockClient = cluster.NewLockClient(nil, "seed:8888")
	if got := wfs.entryWriteOwner(path); got != "" {
		t.Fatalf("no ring yet: owner %q, want none", got)
	}

	servers := []pb.ServerAddress{"filer1:8888", "filer2:8888", "filer3:8888"}
	wfs.lockClient.SetRing(servers, 1)
	filerRing := lock_manager.NewLockRing(time.Hour)
	filerRing.SetSnapshot(servers, 1)

	owners := map[pb.ServerAddress]bool{}
	for i := 0; i < 64; i++ {
		p := util.FullPath(fmt.Sprintf("/dir/file%d", i))
		want := filerRing.WriteOwner(s3_constants.ObjectWriteRouteKeyPrefix + string(p))
		if got := wfs.entryWriteOwner(p); got != want {
			t.Fatalf("%s: mount routes to %q, filer to %q", p, got, want)
		}
		owners[want] = true
	}
	if len(owners) < 2 {
		t.Fatalf("all paths hashed to %v; the test does not tell routing keys apart", owners)
	}
}
