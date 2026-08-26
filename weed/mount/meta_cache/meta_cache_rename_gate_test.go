package meta_cache

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A rename stamps its source and takes the name away at the same log position,
// so the removal arrives at exactly the version the source already records.
// Read as a write that is already reflected, it was dropped and the source
// stayed visible next to the destination.
func TestRenameRemovesSourceAtItsOwnVersion(t *testing.T) {
	const version = 1787761708173717200

	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true, "/dir": true})
	defer mc.Shutdown()
	ctx := context.Background()

	write := func(name string, tsNs int64) *filer_pb.SubscribeMetadataResponse {
		return &filer_pb.SubscribeMetadataResponse{
			Directory: "/dir",
			EventNotification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{
					Name:       name,
					Attributes: &filer_pb.FuseAttributes{Crtime: 1, Mtime: 1, FileMode: 0100644, FileSize: 3},
				},
			},
			TsNs: tsNs,
		}
	}
	for _, event := range []*filer_pb.SubscribeMetadataResponse{write("src", version), write("dst", version-1000)} {
		if err := mc.ApplyMetadataResponseOwned(ctx, event, LocalMetadataResponseApplyOptions); err != nil {
			t.Fatalf("seed %s: %v", event.EventNotification.NewEntry.Name, err)
		}
	}

	rename := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "src"},
			NewEntry: &filer_pb.Entry{
				Name:       "dst",
				Attributes: &filer_pb.FuseAttributes{Crtime: 1, Mtime: 1, FileMode: 0100644, FileSize: 3},
			},
			NewParentPath: "/dir",
		},
		TsNs: version,
	}
	if err := mc.ApplyMetadataResponseOwned(ctx, rename, LocalMetadataResponseApplyOptions); err != nil {
		t.Fatalf("rename: %v", err)
	}

	if entry, _, err := mc.FindEntry(ctx, "/dir/src"); err == nil && entry != nil {
		t.Error("the renamed-away source is still cached")
	}
	if entry, _, err := mc.FindEntry(ctx, "/dir/dst"); err != nil || entry == nil {
		t.Errorf("the rename destination is missing: %v", err)
	}
}

// A removal older than the entry's record is still stale and must not apply:
// the record describes a write the removal never saw.
func TestRemovalOlderThanTheRecordStillFences(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true, "/dir": true})
	defer mc.Shutdown()
	ctx := context.Background()

	if err := mc.ApplyMetadataResponseOwned(ctx, &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{Crtime: 1, Mtime: 1, FileMode: 0100644, FileSize: 3},
			},
		},
		TsNs: 2000,
	}, LocalMetadataResponseApplyOptions); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := mc.ApplyMetadataResponseOwned(ctx, &filer_pb.SubscribeMetadataResponse{
		Directory:         "/dir",
		EventNotification: &filer_pb.EventNotification{OldEntry: &filer_pb.Entry{Name: "file"}},
		TsNs:              1000,
	}, LocalMetadataResponseApplyOptions); err != nil {
		t.Fatalf("stale removal: %v", err)
	}

	if entry, _, err := mc.FindEntry(ctx, "/dir/file"); err != nil || entry == nil {
		t.Errorf("a removal older than the entry's record deleted it: %v", err)
	}
}
