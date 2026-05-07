package pb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// SystemLogDir mirrors weed/filer/topics.go to avoid pulling weed/filer.
const SystemLogDir = "/topics/.system/log"

// LogDirLister lets tests fake the listing without a filer gRPC server.
type LogDirLister interface {
	ListLogDirEntries(ctx context.Context, dir, startFrom string, limit int) ([]*filer_pb.Entry, error)
}

type FilerShardRange struct {
	Earliest MessagePosition
	Latest   MessagePosition
}

// EarliestRetainedPositionPerShard floors a newly-discovered shard's cursor.
// Offset is 0; chunk filenames have minute resolution.
func EarliestRetainedPositionPerShard(ctx context.Context, lister LogDirLister) (map[string]MessagePosition, error) {
	out := make(map[string]MessagePosition)
	err := walkLogChunks(ctx, lister, func(filerId string, ts time.Time) bool {
		if _, seen := out[filerId]; !seen {
			out[filerId] = MessagePosition{TsNs: ts.UnixNano(), Offset: 0}
		}
		return true
	})
	return out, err
}

// RetainedLogRangePerShard returns {earliest, latest} per filer in one pass
// (a TOCTOU window between two passes could leave `latest` zero). Latest's
// Offset is int64-max so cursor >= range.latest under the strict <= skip
// predicate matches the tail-drain criterion.
func RetainedLogRangePerShard(ctx context.Context, lister LogDirLister) (map[string]FilerShardRange, error) {
	earliest := make(map[string]time.Time)
	latest := make(map[string]time.Time)
	if err := walkLogChunks(ctx, lister, func(filerId string, ts time.Time) bool {
		if e, ok := earliest[filerId]; !ok || ts.Before(e) {
			earliest[filerId] = ts
		}
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
			Latest:   MessagePosition{TsNs: latest[filerId].UnixNano(), Offset: 1<<63 - 1},
		}
	}
	return out, nil
}

// walkLogChunks iterates every (filerId, chunk-time) tuple. cb returns
// false to stop early. Per-day chunks are streamed page-by-page (a busy
// day can hold hundreds of thousands of entries).
func walkLogChunks(ctx context.Context, lister LogDirLister, cb func(filerId string, ts time.Time) bool) error {
	stop := errStopWalk
	dayErr := streamEntries(ctx, lister, SystemLogDir, func(day *filer_pb.Entry) error {
		if !day.IsDirectory {
			return nil
		}
		err := streamEntries(ctx, lister, SystemLogDir+"/"+day.Name, func(hm *filer_pb.Entry) error {
			filerId := getFilerIdFromName(hm.Name)
			if filerId == "" {
				return nil
			}
			ts, ok := parseChunkTime(day.Name, hm.Name)
			if !ok {
				return nil
			}
			if !cb(filerId, ts) {
				return stop
			}
			return nil
		})
		if err != nil && err != stop {
			return fmt.Errorf("list %s/%s: %w", SystemLogDir, day.Name, err)
		}
		return err
	})
	if dayErr == stop {
		return nil
	}
	return dayErr
}

// errStopWalk short-circuits nested streamEntries calls when cb stops.
var errStopWalk = fmt.Errorf("stop walk")

// listingLimit: per-day SystemLogDir typically holds ~1440 entries.
const listingLimit = 4096

// streamEntries paginates without buffering. fn returning a non-nil error
// halts the walk; callers distinguish real errors from errStopWalk.
func streamEntries(ctx context.Context, lister LogDirLister, dir string, fn func(*filer_pb.Entry) error) error {
	startFrom := ""
	for {
		entries, err := lister.ListLogDirEntries(ctx, dir, startFrom, listingLimit)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		for _, e := range entries {
			if err := fn(e); err != nil {
				return err
			}
		}
		if len(entries) < listingLimit {
			return nil
		}
		startFrom = entries[len(entries)-1].Name
	}
}

// getFilerIdFromName parses "HH-MM.<filerId>"; returns "" on mismatch.
func getFilerIdFromName(name string) string {
	idx := strings.LastIndex(name, ".")
	if idx < 0 || idx >= len(name)-1 {
		return ""
	}
	return name[idx+1:]
}

// parseChunkTime joins day "YYYY-MM-DD" + "HH-MM.<filerId>" back into UTC time.
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
