package meta_cache

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A cached directory's listing is split into contiguous name-range sections so
// a burst of remote changes invalidates one section, not the whole listing.
// Events keep applying to a stale section; staleness only means the next
// listing re-validates that range against the filer before serving it, and
// lookups in it read through until then.
const (
	// dirSectionSize is the target entries per section, fixed when a listing
	// is built and re-derived when a re-listed section has outgrown it.
	dirSectionSize = 1024
	// sectionHotThreshold remote changes within sectionHotWindow invalidate
	// the section they land in.
	sectionHotThreshold = 64
	sectionHotWindow    = 2 * time.Second
	// sectionRefreshTimeout bounds how long a readdir waits on re-validating
	// a section before serving the maintained-but-unverified cache instead.
	sectionRefreshTimeout = 5 * time.Second
	// sectionRefreshMaxEntries is the most one refresh will carry; a section
	// grown past it is cheaper to re-tile with a full directory rebuild.
	sectionRefreshMaxEntries = 4 * dirSectionSize
)

// ErrRefreshRangeTooLarge reports a section that outgrew one refresh; the
// caller should drop the directory cache so a full rebuild re-tiles it.
var ErrRefreshRangeTooLarge = errors.New("section outgrew one refresh")

// dirSections: bounds[i] is the first name of section i+1; section 0 starts at
// the beginning of the namespace, the last section runs to the end.
type dirSections struct {
	bounds   []string
	sections []sectionState
}

type sectionState struct {
	stale       bool
	updateCount int
	windowStart time.Time
}

func newDirSections(bounds []string) *dirSections {
	return &dirSections{bounds: bounds, sections: make([]sectionState, len(bounds)+1)}
}

func (ds *dirSections) sectionOf(name string) int {
	idx := sort.SearchStrings(ds.bounds, name)
	if idx < len(ds.bounds) && ds.bounds[idx] == name {
		idx++
	}
	return idx
}

// sectionRange returns the [lo, hi) name range of section idx; "" is unbounded.
func (ds *dirSections) sectionRange(idx int) (lo, hi string) {
	if idx > 0 {
		lo = ds.bounds[idx-1]
	}
	if idx < len(ds.bounds) {
		hi = ds.bounds[idx]
	}
	return
}

// noteSectionChangeLocked counts one remote change against the section it
// lands in, marking the section stale when a burst crosses the threshold.
func (mc *MetaCache) noteSectionChangeLocked(fp util.FullPath, now time.Time) {
	dir, name := fp.DirAndName()
	ds := mc.dirSections[util.FullPath(dir)]
	if ds == nil {
		return
	}
	s := &ds.sections[ds.sectionOf(name)]
	if s.stale {
		return
	}
	if s.windowStart.IsZero() || now.Sub(s.windowStart) > sectionHotWindow {
		s.windowStart = now
		s.updateCount = 0
	}
	s.updateCount++
	if s.updateCount >= sectionHotThreshold {
		s.stale = true
	}
}

// IsNameFresh reports whether the cached listing still vouches for this name.
// A directory without section state vouches for all of it.
func (mc *MetaCache) IsNameFresh(fp util.FullPath) bool {
	dir, name := fp.DirAndName()
	mc.RLock()
	defer mc.RUnlock()
	ds := mc.dirSections[util.FullPath(dir)]
	if ds == nil {
		return true
	}
	return !ds.sections[ds.sectionOf(name)].stale
}

type nameRange struct {
	lo, hi string
}

// staleRangesAhead returns the invalidated ranges among the count sections
// starting at the one holding startName.
func (mc *MetaCache) staleRangesAhead(dirPath util.FullPath, startName string, count int) (ranges []nameRange) {
	mc.RLock()
	defer mc.RUnlock()
	ds := mc.dirSections[dirPath]
	if ds == nil {
		return nil
	}
	idx := ds.sectionOf(startName)
	for i := idx; i < idx+count && i < len(ds.sections); i++ {
		if ds.sections[i].stale {
			lo, hi := ds.sectionRange(i)
			ranges = append(ranges, nameRange{lo: lo, hi: hi})
		}
	}
	return
}

func (mc *MetaCache) rangeStale(dirPath util.FullPath, lo string) bool {
	mc.RLock()
	defer mc.RUnlock()
	ds := mc.dirSections[dirPath]
	if ds == nil {
		return false
	}
	return ds.sections[ds.sectionOf(lo)].stale
}

// EnsureListingFresh re-validates invalidated sections before a listing pages
// through them: the section holding startName and the one after it, which is
// as far as one readdir batch can reach. Sections further on are handled by
// the batches that reach them.
func EnsureListingFresh(ctx context.Context, mc *MetaCache, client filer_pb.FilerClient, dirPath util.FullPath, startName string) error {
	ranges := mc.staleRangesAhead(dirPath, startName, 2)
	if len(ranges) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, sectionRefreshTimeout)
	defer cancel()
	for _, r := range ranges {
		if err := mc.refreshSection(ctx, client, dirPath, r.lo, r.hi); err != nil {
			return err
		}
	}
	return nil
}

func (mc *MetaCache) refreshSection(ctx context.Context, client filer_pb.FilerClient, dirPath util.FullPath, lo, hi string) error {
	_, err, _ := mc.visitGroup.Do(string(dirPath)+"\x00section\x00"+lo, func() (interface{}, error) {
		if !mc.rangeStale(dirPath, lo) {
			return nil, nil
		}
		entries, snapshotTsNs, err := mc.listFilerRange(ctx, client, dirPath, lo, hi)
		if err != nil {
			return nil, err
		}
		return nil, mc.enqueueAndWait(ctx, metadataApplyRequest{
			kind:           metadataSectionRefresh,
			buildPath:      dirPath,
			sectionLo:      lo,
			sectionHi:      hi,
			sectionEntries: entries,
			snapshotTsNs:   snapshotTsNs,
		})
	})
	return err
}

// listFilerRange reads [lo, hi) from the filer at one snapshot, paging by
// section-sized batches.
func (mc *MetaCache) listFilerRange(ctx context.Context, client filer_pb.FilerClient, dirPath util.FullPath, lo, hi string) (entries []*filer.Entry, snapshotTsNs int64, err error) {
	startFrom, includeStart := lo, lo != ""
	for {
		var page []*filer.Entry
		var pageCount int
		var last string
		done := false
		err = client.WithFilerClient(false, func(sc filer_pb.SeaweedFilerClient) error {
			// reset in case a failover retry re-runs a partly streamed page
			page, pageCount, last, done = nil, 0, "", false
			ts, listErr := filer_pb.DoSeaweedListWithSnapshot(ctx, sc, dirPath, "", func(pbEntry *filer_pb.Entry, isLast bool) error {
				pageCount++
				last = pbEntry.Name
				if hi != "" && pbEntry.Name >= hi {
					done = true
				}
				if done {
					return nil
				}
				if !mc.includeSystemEntries && IsHiddenSystemEntry(string(dirPath), pbEntry.Name) {
					return nil
				}
				page = append(page, filer.FromPbEntry(string(dirPath), pbEntry))
				return nil
			}, startFrom, includeStart, dirSectionSize, snapshotTsNs)
			if listErr != nil {
				return listErr
			}
			if snapshotTsNs == 0 {
				snapshotTsNs = ts
			}
			return nil
		})
		if err != nil {
			return nil, 0, err
		}
		entries = append(entries, page...)
		if len(entries) > sectionRefreshMaxEntries {
			return nil, 0, ErrRefreshRangeTooLarge
		}
		if done || pageCount < dirSectionSize {
			return entries, snapshotTsNs, nil
		}
		startFrom, includeStart = last, false
	}
}

// applySectionRefreshNow reconciles one section against a filer listing of its
// range, then marks it fresh. Runs on the apply loop; mutations go through the
// version gate so the listing cannot roll back a newer applied event, and
// pinned local-only entries (deferred creates not yet on the filer) survive.
func (mc *MetaCache) applySectionRefreshNow(ctx context.Context, req metadataApplyRequest) error {
	dirPath, lo, hi := req.buildPath, req.sectionLo, req.sectionHi
	snapshotTsNs := req.snapshotTsNs

	mc.Lock()
	defer mc.Unlock()

	fetched := make(map[string]struct{}, len(req.sectionEntries))
	for _, entry := range req.sectionEntries {
		fetched[entry.Name()] = struct{}{}
		if snapshotTsNs == 0 {
			// A pre-upgrade filer stamps no snapshot, leaving nothing to
			// order against: only fill gaps, so a concurrently applied event
			// can never be rolled back.
			if mc.entryExistsLocked(ctx, entry.FullPath) {
				continue
			}
			if _, tombstone := mc.getEntryVersionRecordLocked(ctx, entry.FullPath); tombstone {
				continue
			}
		} else if mc.entryVersionBlocksLocked(ctx, entry.FullPath, snapshotTsNs) {
			continue
		}
		if err := mc.localStore.InsertEntry(ctx, entry); err != nil {
			return err
		}
		mc.setEntryVersionLocked(ctx, entry.FullPath, snapshotTsNs)
	}

	// Deletions need the snapshot as an ordering reference; without one a
	// name created after the listing would be swept away.
	if snapshotTsNs != 0 {
		var vanished []*filer.Entry
		if _, err := mc.localStore.ListDirectoryEntries(ctx, dirPath, lo, true, math.MaxInt64, func(entry *filer.Entry) (bool, error) {
			if hi != "" && entry.Name() >= hi {
				return false, nil
			}
			if _, found := fetched[entry.Name()]; !found {
				vanished = append(vanished, entry)
			}
			return true, nil
		}); err != nil {
			return err
		}
		for _, entry := range vanished {
			if mc.pinnedChildFn != nil && mc.pinnedChildFn(entry) {
				continue
			}
			if mc.entryVersionBlocksLocked(ctx, entry.FullPath, snapshotTsNs) {
				continue
			}
			if err := mc.localStore.DeleteEntry(ctx, entry.FullPath); err != nil {
				return err
			}
			mc.setEntryTombstoneLocked(ctx, entry.FullPath, snapshotTsNs)
		}
	}

	ds := mc.dirSections[dirPath]
	if ds == nil {
		return nil
	}
	idx := ds.sectionOf(lo)
	// The table can be rebuilt or re-split between the listing and this
	// apply; only touch it if the section still covers the range just read.
	if curLo, curHi := ds.sectionRange(idx); curLo != lo || curHi != hi {
		return nil
	}
	if len(req.sectionEntries) > 2*dirSectionSize {
		// the section outgrew its target size; re-derive bounds inside it so
		// the next burst invalidates a slice of it, not all of it
		var newBounds []string
		for i := dirSectionSize; i < len(req.sectionEntries); i += dirSectionSize {
			newBounds = append(newBounds, req.sectionEntries[i].Name())
		}
		bounds := make([]string, 0, len(ds.bounds)+len(newBounds))
		bounds = append(bounds, ds.bounds[:idx]...)
		bounds = append(bounds, newBounds...)
		bounds = append(bounds, ds.bounds[idx:]...)
		sections := make([]sectionState, 0, len(bounds)+1)
		sections = append(sections, ds.sections[:idx]...)
		sections = append(sections, make([]sectionState, len(newBounds)+1)...)
		sections = append(sections, ds.sections[idx+1:]...)
		ds.bounds, ds.sections = bounds, sections
	} else {
		ds.sections[idx] = sectionState{}
	}
	return nil
}
