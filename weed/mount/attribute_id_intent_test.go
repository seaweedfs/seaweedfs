//go:build !freebsd

package mount

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

func TestAttributeExplicitCollidingIds(t *testing.T) {
	for _, valid := range []uint32{fuse.FATTR_UID, fuse.FATTR_GID, fuse.FATTR_UID | fuse.FATTR_GID, fuse.FATTR_MODE} {
		t.Run(map[uint32]string{fuse.FATTR_UID: "uid", fuse.FATTR_GID: "gid", fuse.FATTR_UID | fuse.FATTR_GID: "both", fuse.FATTR_MODE: "untouched"}[valid], func(t *testing.T) {
			wfs := newInvalidateTestWFS(t)
			mapper, err := meta_cache.NewUidGidMapper("1000:2000", "20:30")
			if err != nil {
				t.Fatal(err)
			}
			wfs.option.UidGidMapper = mapper
			wfs.metaCache = meta_cache.NewMetaCache(filepath.Join(t.TempDir(), "mapped"), mapper, util.FullPath("/"), false, func(p util.FullPath) { wfs.inodeToPath.MarkChildrenCached(p) }, func(p util.FullPath) bool { return wfs.inodeToPath.IsChildrenCached(p) }, wfs.onEntryInvalidation, nil)
			t.Cleanup(wfs.metaCache.Shutdown)
			wfs.atimeMap = make(map[uint64]time.Time)
			current := attributeAppendEntry(3)
			current.Attributes.Uid = 1000
			current.Attributes.Gid = 20
			fake := &attributeCASFiler{entry: proto.Clone(current).(*filer_pb.Entry)}
			startFakeFiler(t, wfs, fake)
			path := util.FullPath("/dir/file")
			wfs.inodeToPath.Lookup(util.FullPath("/dir"), 1, true, false, 0, false)
			wfs.inodeToPath.Lookup(path, 1, false, false, 42, true)
			wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))
			if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", current), 1); err != nil {
				t.Fatal(err)
			}
			in := &fuse.SetAttrIn{}
			in.NodeId = 42
			in.Valid = valid
			in.Uid = 1000
			in.Gid = 20
			in.Mode = 0600
			if code := wfs.SetAttr(nil, in, &fuse.AttrOut{}); code != fuse.OK {
				t.Fatal(code)
			}
			uid, gid := uint32(1000), uint32(20)
			if valid&fuse.FATTR_UID != 0 {
				uid = 2000
			}
			if valid&fuse.FATTR_GID != 0 {
				gid = 30
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.entry.Attributes.Uid != uid || fake.entry.Attributes.Gid != gid {
				t.Fatalf("acknowledged ids=(%d,%d), want (%d,%d) for explicitly supplied fields %x", fake.entry.Attributes.Uid, fake.entry.Attributes.Gid, uid, gid, valid)
			}
			if !proto.Equal(fake.entry.Chunks[0], current.Chunks[0]) {
				t.Fatal("attribute operation changed original chunk")
			}
		})
	}
}
