package pb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// SystemLogDir mirrors weed/filer/topics.go to avoid pulling weed/filer
// (which would create a dep cycle for the worker).
const SystemLogDir = "/topics/.system/log"

// LogDirLister is the directory-listing capability the listing helpers
// need. The production implementation wraps filer_pb.SeaweedFilerClient
// via NewClientLogDirLister; tests inject in-memory fakes.
type LogDirLister interface {
	// ListLogDirEntries returns up to limit entries under dir, starting
	// after startFrom (exclusive). The caller paginates by passing the
	// last returned entry name as startFrom on the next call. Returns
	// entries in lexicographic name order.
	ListLogDirEntries(ctx context.Context, dir, startFrom string, limit int) ([]*filer_pb.Entry, error)
}

// FilerShardRange is the (earliest, latest) chunk-timestamp pair for one
// filer's persisted log files. Both are MessagePosition with Offset=0
// (chunk filenames have minute resolution; the actual entry offsets within
// the chunk are not exposed by the listing helpers).
type FilerShardRange struct {
	Earliest MessagePosition
	Latest   MessagePosition
}

// EarliestRetainedPositionPerShard walks SystemLogDir oldest-first and
// returns the earliest retained chunk per filer_id. Used by lazy-seeding
// to safely floor a newly-discovered shard's cursor: a brand-new shard
// can't claim positions earlier than its earliest retained chunk.
//
// The returned MessagePosition has Offset=0 and TsNs set to the chunk
// filename's parsed time (minute resolution). Callers comparing this
// against an entry-level cursor should use the lex (ts, offset) order so
// entries at exactly the floor's TsNs with Offset>0 are not skipped.
func EarliestRetainedPositionPerShard(ctx context.Context, lister LogDirLister) (map[string]MessagePosition, error) {
	out := make(map[string]MessagePosition)
	err := walkLogChunks(ctx, lister, false, func(filerId string, ts time.Time) bool {
		if _, seen := out[filerId]; !seen {
			out[filerId] = MessagePosition{TsNs: ts.UnixNano(), Offset: 0}
		}
		return true // keep walking; we want every filer's earliest
	})
	return out, err
}

// RetainedLogRangePerShard walks SystemLogDir twice (oldest- and newest-
// first) and returns {earliest, latest} per filer_id. Used by tail-drain
// GC: cursor[F] >= range.latest means F has been tail-drained and the
// cursor entry can be GC'd; absence of a range for F whose cursor was
// never observed at range.latest is the lost-log signal.
//
// Latest carries Offset=int64-max so a `cursor >= range.latest` lex
// comparison (under the strict `<=` skip predicate) is satisfied only
// when the cursor is at or past the chunk's last entry — matches the
// design's tail-drain criterion.
func RetainedLogRangePerShard(ctx context.Context, lister LogDirLister) (map[string]FilerShardRange, error) {
	earliest := make(map[string]time.Time)
	latest := make(map[string]time.Time)

	if err := walkLogChunks(ctx, lister, false, func(filerId string, ts time.Time) bool {
		if e, ok := earliest[filerId]; !ok || ts.Before(e) {
			earliest[filerId] = ts
		}
		return true
	}); err != nil {
		return nil, err
	}
	if err := walkLogChunks(ctx, lister, true, func(filerId string, ts time.Time) bool {
		if l, ok := latest[filerId]; !ok || ts.After(l) {
			latest[filerId] = ts
		}
		return true
	}); err != nil {
		return nil, err
	}

	out := make(map[string]FilerShardRange, len(earliest))
	for filerId, e := range earliest {
		out[filerId] = FilerShardRange{
			Earliest: MessagePosition{TsNs: e.UnixNano(), Offset: 0},
			// Latest's Offset = max so any cursor at or past the chunk's
			// last entry compares >= range.latest in lex (ts, offset).
			Latest: MessagePosition{TsNs: latest[filerId].UnixNano(), Offset: 1<<63 - 1},
		}
	}
	return out, nil
}

// walkLogChunks iterates every (filerId, chunk-time) tuple under
// SystemLogDir. cb returns false to stop the walk early.
func walkLogChunks(ctx context.Context, lister LogDirLister, newestFirst bool, cb func(filerId string, ts time.Time) bool) error {
	dayEntries, err := listAll(ctx, lister, SystemLogDir, newestFirst)
	if err != nil {
		return err
	}
	for _, day := range dayEntries {
		if !day.IsDirectory {
			continue
		}
		hourMinuteEntries, err := listAll(ctx, lister, SystemLogDir+"/"+day.Name, newestFirst)
		if err != nil {
			return fmt.Errorf("list %s/%s: %w", SystemLogDir, day.Name, err)
		}
		for _, hm := range hourMinuteEntries {
			filerId := getFilerIdFromName(hm.Name)
			if filerId == "" {
				continue
			}
			ts, ok := parseChunkTime(day.Name, hm.Name)
			if !ok {
				continue
			}
			if !cb(filerId, ts) {
				return nil
			}
		}
	}
	return nil
}

// listAll paginates ListLogDirEntries until the directory is fully read.
// listingLimit per call is intentionally large; SystemLogDir's
// per-day-dir typically holds at most ~1440 entries.
const listingLimit = 4096

func listAll(ctx context.Context, lister LogDirLister, dir string, newestFirst bool) ([]*filer_pb.Entry, error) {
	var out []*filer_pb.Entry
	startFrom := ""
	for {
		entries, err := lister.ListLogDirEntries(ctx, dir, startFrom, listingLimit)
		if err != nil {
			return nil, err
		}
		if len(entries) == 0 {
			break
		}
		out = append(out, entries...)
		if len(entries) < listingLimit {
			break
		}
		startFrom = entries[len(entries)-1].Name
	}
	if newestFirst {
		// reverse in place
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	return out, nil
}

// getFilerIdFromName extracts the filerId (8 hex chars) suffix from a chunk
// filename "HH-MM.<filerId>". Returns "" if the format doesn't match.
func getFilerIdFromName(name string) string {
	idx := strings.LastIndex(name, ".")
	if idx < 0 || idx >= len(name)-1 {
		return ""
	}
	return name[idx+1:]
}

// parseChunkTime parses the day-dir + chunk-file names back into a UTC
// time. Day = "YYYY-MM-DD"; chunk file = "HH-MM.<filerId>". Returns false
// when the format doesn't parse (caller skips the entry).
func parseChunkTime(day, hmName string) (time.Time, bool) {
	dot := strings.LastIndex(hmName, ".")
	if dot < 0 {
		return time.Time{}, false
	}
	hm := hmName[:dot]
	t, err := time.Parse("2006-01-02-15-04", day+"-"+hm)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
