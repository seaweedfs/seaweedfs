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
// the beginning of the namespace, the last section runs to the end. It is a
// plain state machine — no locking, no store; MetaCache drives it under its
// own mutex.
type dirSections struct {
	bounds   []string
	sections []sectionState
}

type sectionState struct {
	stale       bool
	updateCount int
	windowStart time.Time
	// floorTsNs is the section's own listing snapshot: a refresh at it covered
	// every name in the range, present or absent, so it fences like the
	// directory floor but for this range alone.
	floorTsNs int64
}

func newDirSections(bounds []string) *dirSections {
	return &dirSections{bounds: bounds, sections: make([]sectionState, len(bounds)+1)}
}

// sectionBoundsCollector derives section boundaries from an ordered listing:
// every dirSectionSize-th name starts a new section.
type sectionBoundsCollector struct {
	count  int
	bounds []string
}

func (c *sectionBoundsCollector) note(name string) {
	if c.count > 0 && c.count%dirSectionSize == 0 {
		c.bounds = append(c.bounds, name)
	}
	c.count++
}

// sectionRefresh carries one section's re-listing to the apply loop.
type sectionRefresh struct {
	lo, hi       string
	entries      []*filer.Entry
	snapshotTsNs int64
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

// noteChange counts one change against the section it lands in, marking the
// section stale when a burst crosses the threshold.
func (ds *dirSections) noteChange(name string, now time.Time) {
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

func (ds *dirSections) isFresh(name string) bool {
	return !ds.sections[ds.sectionOf(name)].stale
}

// floorOf returns the refresh snapshot covering this name, or zero when its
// section has never been re-listed. A stale section keeps fencing: what its
// last listing established stays established.
func (ds *dirSections) floorOf(name string) int64 {
	return ds.sections[ds.sectionOf(name)].floorTsNs
}

type nameRange struct {
	lo, hi string
}

// staleRangesAhead returns the invalidated ranges from the section holding
// startName to the end of the directory.
func (ds *dirSections) staleRangesAhead(startName string) (ranges []nameRange) {
	for i := ds.sectionOf(startName); i < len(ds.sections); i++ {
		if ds.sections[i].stale {
			lo, hi := ds.sectionRange(i)
			ranges = append(ranges, nameRange{lo: lo, hi: hi})
		}
	}
	return
}

// completeRefresh marks the section covering exactly [lo, hi) fresh after a
// re-listing that fetched names at snapshotTsNs, re-splitting a section that
// outgrew twice its target size. The snapshot becomes the section's floor. It
// reports false without touching anything when there is no snapshot — an
// unversioned listing vouches for nothing — or when the table no longer has
// that section, rebuilt or re-split since the listing was taken, as splicing
// bounds from a stale range could leave the table unsorted.
func (ds *dirSections) completeRefresh(lo, hi string, names []string, snapshotTsNs int64) bool {
	if snapshotTsNs == 0 {
		return false
	}
	idx := ds.sectionOf(lo)
	if curLo, curHi := ds.sectionRange(idx); curLo != lo || curHi != hi {
		return false
	}
	if len(names) > 2*dirSectionSize {
		var newBounds []string
		for i := dirSectionSize; i < len(names); i += dirSectionSize {
			newBounds = append(newBounds, names[i])
		}
		bounds := make([]string, 0, len(ds.bounds)+len(newBounds))
		bounds = append(bounds, ds.bounds[:idx]...)
		bounds = append(bounds, newBounds...)
		bounds = append(bounds, ds.bounds[idx:]...)
		sections := make([]sectionState, 0, len(bounds)+1)
		sections = append(sections, ds.sections[:idx]...)
		for i := 0; i <= len(newBounds); i++ {
			sections = append(sections, sectionState{floorTsNs: snapshotTsNs})
		}
		sections = append(sections, ds.sections[idx+1:]...)
		ds.bounds, ds.sections = bounds, sections
	} else {
		ds.sections[idx] = sectionState{floorTsNs: snapshotTsNs}
	}
	return true
}

// noteSectionChangeLocked counts one remote change against the section of the
// directory it lands in.
func (mc *MetaCache) noteSectionChangeLocked(fp util.FullPath, now time.Time) {
	dir, name := fp.DirAndName()
	if ds := mc.dirSections[util.FullPath(dir)]; ds != nil {
		ds.noteChange(name, now)
	}
}

// IsNameFresh reports whether the cached listing still vouches for this name.
// A directory without section state vouches for all of it.
func (mc *MetaCache) IsNameFresh(fp util.FullPath) bool {
	dir, name := fp.DirAndName()
	mc.RLock()
	defer mc.RUnlock()
	ds := mc.dirSections[util.FullPath(dir)]
	return ds == nil || ds.isFresh(name)
}

func (mc *MetaCache) staleRangesAhead(dirPath util.FullPath, startName string) []nameRange {
	mc.RLock()
	defer mc.RUnlock()
	ds := mc.dirSections[dirPath]
	if ds == nil {
		return nil
	}
	return ds.staleRangesAhead(startName)
}

func (mc *MetaCache) rangeStale(dirPath util.FullPath, lo string) bool {
	mc.RLock()
	defer mc.RUnlock()
	ds := mc.dirSections[dirPath]
	return ds != nil && !ds.isFresh(lo)
}

// EnsureListingFresh re-validates every invalidated section from startName to
// the end of the directory before a listing pages through them. A listing's
// reach is unknowable up front — a resumed handle can skip far ahead, and
// shrunken sections let one batch span many — so all of them are covered.
func EnsureListingFresh(ctx context.Context, mc *MetaCache, client filer_pb.FilerClient, dirPath util.FullPath, startName string) error {
	ranges := mc.staleRangesAhead(dirPath, startName)
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
			kind:      metadataSectionRefresh,
			buildPath: dirPath,
			refresh:   &sectionRefresh{lo: lo, hi: hi, entries: entries, snapshotTsNs: snapshotTsNs},
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
func (mc *MetaCache) applySectionRefreshNow(ctx context.Context, dirPath util.FullPath, r *sectionRefresh) error {
	lo, hi, snapshotTsNs := r.lo, r.hi, r.snapshotTsNs

	mc.Lock()
	defer mc.Unlock()

	fetchedNames := make([]string, 0, len(r.entries))
	fetched := make(map[string]struct{}, len(r.entries))
	for _, entry := range r.entries {
		fetchedNames = append(fetchedNames, entry.Name())
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
			if err := mc.localStore.InsertEntry(ctx, entry); err != nil {
				return err
			}
			mc.setEntryVersionLocked(ctx, entry.FullPath, 0)
			continue
		}
		if mc.entryVersionBlocksLocked(ctx, entry.FullPath, snapshotTsNs) {
			continue
		}
		// no per-entry version: the section floor set below covers the range
		if err := mc.localStore.InsertEntry(ctx, entry); err != nil {
			return err
		}
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
			mc.clearEntryVersionLocked(ctx, entry.FullPath)
		}
	}

	// The floor fences the whole range, absent names included; an unversioned
	// listing sets none and the section stays stale, read through until a
	// filer that stamps snapshots re-validates it.
	if ds := mc.dirSections[dirPath]; ds != nil {
		ds.completeRefresh(lo, hi, fetchedNames, snapshotTsNs)
	}
	return nil
}
