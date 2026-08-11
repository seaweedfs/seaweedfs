package meta_cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestSectionOf(t *testing.T) {
	ds := newDirSections([]string{"g", "p"})
	for name, want := range map[string]int{
		"a": 0, "f": 0,
		"g": 1, "h": 1, "o": 1,
		"p": 2, "z": 2,
	} {
		if got := ds.sectionOf(name); got != want {
			t.Errorf("sectionOf(%q) = %d, want %d", name, got, want)
		}
	}
	if lo, hi := ds.sectionRange(0); lo != "" || hi != "g" {
		t.Errorf("sectionRange(0) = %q..%q, want ..g", lo, hi)
	}
	if lo, hi := ds.sectionRange(1); lo != "g" || hi != "p" {
		t.Errorf("sectionRange(1) = %q..%q, want g..p", lo, hi)
	}
	if lo, hi := ds.sectionRange(2); lo != "p" || hi != "" {
		t.Errorf("sectionRange(2) = %q..%q, want p..", lo, hi)
	}
}

func buildSectionedDir(t *testing.T, mc *MetaCache, dir util.FullPath, snapshotTsNs int64, bounds []string) {
	t.Helper()
	if err := mc.BeginDirectoryBuild(context.Background(), dir); err != nil {
		t.Fatalf("begin build: %v", err)
	}
	if err := mc.CompleteDirectoryBuild(context.Background(), dir, snapshotTsNs, bounds); err != nil {
		t.Fatalf("complete build: %v", err)
	}
}

func applyChurn(t *testing.T, mc *MetaCache, dir, prefix string, n int, baseTsNs int64, options MetadataResponseApplyOptions) {
	t.Helper()
	for i := 0; i < n; i++ {
		resp := &filer_pb.SubscribeMetadataResponse{
			Directory: dir,
			EventNotification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{
					Name: fmt.Sprintf("%s%03d", prefix, i),
					Attributes: &filer_pb.FuseAttributes{
						Crtime:   1,
						Mtime:    1,
						FileMode: 0100644,
						FileSize: 1,
					},
				},
			},
			TsNs: baseTsNs + int64(i),
		}
		if err := mc.ApplyMetadataResponse(context.Background(), resp, options); err != nil {
			t.Fatalf("apply churn %d: %v", i, err)
		}
	}
}

func TestSectionBurstInvalidatesOnlyItsSection(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, []string{"m"})

	applyChurn(t, mc, "/dir", "a-", sectionHotThreshold, 2000, SubscriberMetadataResponseApplyOptions)

	if mc.IsNameFresh(util.FullPath("/dir/a-000")) {
		t.Fatal("burst section should be invalidated")
	}
	if !mc.IsNameFresh(util.FullPath("/dir/zzz")) {
		t.Fatal("the other section must stay fresh")
	}
}

func TestLocalChangesDoNotInvalidateSections(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, nil)

	applyChurn(t, mc, "/dir", "a-", 2*sectionHotThreshold, 2000, LocalMetadataResponseApplyOptions)

	if !mc.IsNameFresh(util.FullPath("/dir/a-000")) {
		t.Fatal("this mount's own writes must not invalidate its cache")
	}
}

func TestSectionRefreshReconciles(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()
	mc.SetPinnedChildFn(func(entry *filer.Entry) bool {
		return entry.Name() == "b-pinned"
	})

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, []string{"m"})

	for _, name := range []string{"b-keep", "b-vanish", "b-pinned"} {
		if err := mc.InsertEntry(context.Background(), &filer.Entry{
			FullPath: util.FullPath("/dir/" + name),
			Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1},
		}, 0); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	// applied ahead of the refresh snapshot; the listing must not roll it back
	if err := mc.ApplyMetadataResponse(context.Background(), &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name:       "b-newer",
				Attributes: &filer_pb.FuseAttributes{Crtime: 1, Mtime: 9, FileMode: 0100644, FileSize: 9},
			},
		},
		TsNs: 9000,
	}, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply newer event: %v", err)
	}

	applyChurn(t, mc, "/dir", "a-", sectionHotThreshold, 2000, SubscriberMetadataResponseApplyOptions)
	if mc.IsNameFresh(util.FullPath("/dir/b-keep")) {
		t.Fatal("section should be stale before the refresh")
	}

	if err := mc.enqueueAndWait(context.Background(), metadataApplyRequest{
		kind:      metadataSectionRefresh,
		buildPath: util.FullPath("/dir"),
		sectionLo: "",
		sectionHi: "m",
		sectionEntries: []*filer.Entry{
			{
				FullPath: util.FullPath("/dir/b-keep"),
				Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(2, 0), Mode: 0100644, FileSize: 7},
			},
			{
				FullPath: util.FullPath("/dir/b-new"),
				Attr:     filer.Attr{Crtime: time.Unix(2, 0), Mtime: time.Unix(2, 0), Mode: 0100644, FileSize: 3},
			},
		},
		snapshotTsNs: 5000,
	}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	if !mc.IsNameFresh(util.FullPath("/dir/b-keep")) {
		t.Fatal("section should be fresh after the refresh")
	}
	entry, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/b-keep"))
	if err != nil || entry.FileSize != 7 {
		t.Fatalf("b-keep = %+v, %v; want size 7", entry, err)
	}
	if entry, _, err = mc.FindEntry(context.Background(), util.FullPath("/dir/b-new")); err != nil || entry.FileSize != 3 {
		t.Fatalf("b-new = %+v, %v; want size 3", entry, err)
	}
	if _, _, err = mc.FindEntry(context.Background(), util.FullPath("/dir/b-vanish")); err != filer_pb.ErrNotFound {
		t.Fatalf("b-vanish error = %v, want not found", err)
	}
	if _, _, err = mc.FindEntry(context.Background(), util.FullPath("/dir/a-000")); err != filer_pb.ErrNotFound {
		t.Fatalf("churned a-000 error = %v, want not found", err)
	}
	if entry, _, err = mc.FindEntry(context.Background(), util.FullPath("/dir/b-newer")); err != nil || entry.FileSize != 9 {
		t.Fatalf("b-newer = %+v, %v; want size 9 kept", entry, err)
	}
	if entry, _, err = mc.FindEntry(context.Background(), util.FullPath("/dir/b-pinned")); err != nil || entry == nil {
		t.Fatalf("pinned local-only entry must survive a refresh: %v", err)
	}

	// a redelivered create older than the snapshot must not resurrect b-vanish
	if err := mc.ApplyMetadataResponse(context.Background(), &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name:       "b-vanish",
				Attributes: &filer_pb.FuseAttributes{Crtime: 1, Mtime: 1, FileMode: 0100644, FileSize: 1},
			},
		},
		TsNs: 4000,
	}, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply stale event: %v", err)
	}
	if _, _, err = mc.FindEntry(context.Background(), util.FullPath("/dir/b-vanish")); err != filer_pb.ErrNotFound {
		t.Fatalf("b-vanish resurrected by a pre-snapshot event: %v", err)
	}
}

func TestSectionRefreshSplitsOvergrownSection(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, nil)

	count := 2*dirSectionSize + 1
	entries := make([]*filer.Entry, 0, count)
	for i := 0; i < count; i++ {
		entries = append(entries, &filer.Entry{
			FullPath: util.FullPath(fmt.Sprintf("/dir/f-%05d", i)),
			Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1},
		})
	}
	if err := mc.enqueueAndWait(context.Background(), metadataApplyRequest{
		kind:           metadataSectionRefresh,
		buildPath:      util.FullPath("/dir"),
		sectionEntries: entries,
		snapshotTsNs:   5000,
	}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	mc.RLock()
	ds := mc.dirSections[util.FullPath("/dir")]
	bounds := append([]string(nil), ds.bounds...)
	sections := len(ds.sections)
	mc.RUnlock()
	want := []string{fmt.Sprintf("f-%05d", dirSectionSize), fmt.Sprintf("f-%05d", 2*dirSectionSize)}
	if len(bounds) != len(want) || bounds[0] != want[0] || bounds[1] != want[1] {
		t.Fatalf("bounds = %v, want %v", bounds, want)
	}
	if sections != len(bounds)+1 {
		t.Fatalf("sections = %d, want %d", sections, len(bounds)+1)
	}
}
