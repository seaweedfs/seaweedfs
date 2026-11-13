package mount

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// savedEntriesFiler records the entries a mount saves.
type savedEntriesFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.Mutex
	updates []*filer_pb.UpdateEntryRequest
}

func (s *savedEntriesFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates = append(s.updates, proto.Clone(req).(*filer_pb.UpdateEntryRequest))
	return &filer_pb.UpdateEntryResponse{}, nil
}

// Creating or removing an entry directly in the mount root must not save the
// root: its entry is synthesized from the mount options, so saving it would
// replace the real directory on the filer, wiping a bucket's quota and the
// settings S3 keeps on it. Directories below the root are still saved.
func TestTouchDirMtimeCtimeLeavesTheMountRootOnTheFiler(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	root := util.FullPath("/buckets/b1")
	wfs.option.FilerMountRootPath = string(root)
	fake := &savedEntriesFiler{}
	startFakeFiler(t, wfs, fake)

	wfs.inodeToPath.Lookup(root, 1, true, false, 0, false)
	wfs.inodeToPath.MarkChildrenCached(root)
	sub := &filer_pb.Entry{
		Name:        "sub",
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755), Mtime: 1},
	}
	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(string(root), sub), 1); err != nil {
		t.Fatal(err)
	}

	wfs.touchDirMtimeCtime(root)
	wfs.touchDirMtimeCtime(root.Child("sub"))

	fake.mu.Lock()
	defer fake.mu.Unlock()
	var saved []string
	for _, req := range fake.updates {
		saved = append(saved, string(util.NewFullPath(req.Directory, req.Entry.Name)))
	}
	if len(saved) != 1 || saved[0] != "/buckets/b1/sub" {
		t.Fatalf("saved %v, want only /buckets/b1/sub", saved)
	}
	if fake.updates[0].Entry.Attributes.Mtime <= 1 {
		t.Errorf("sub mtime = %d, want it touched", fake.updates[0].Entry.Attributes.Mtime)
	}
}
