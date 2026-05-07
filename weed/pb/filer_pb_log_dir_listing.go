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

// LogDirLister abstracts ListEntries so tests can fake it without spinning
// up a filer gRPC server.
type LogDirLister interface {
	ListLogDirEntries(ctx context.Context, dir, startFrom string, limit int) ([]*filer_pb.Entry, error)
}

type FilerShardRange struct {
	Earliest MessagePosition
	Latest   MessagePosition
}

// EarliestRetainedPositionPerShard returns the earliest retained chunk
// per filer_id, used by lazy-seeding to floor a newly-discovered shard's
// cursor. Offset is 0; chunk filenames have minute resolution.
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

// RetainedLogRangePerShard returns {earliest, latest} per filer in a single
// pass. Two passes would double RPCs and open a TOCTOU window where a filer
// disappearing between walks would leave a zero-time `latest`.
//
// Latest's Offset = int64-max so a `cursor >= range.latest` lex comparison
// (under the strict <= skip predicate) is satisfied only when the cursor is
// at or past the chunk's last entry — the tail-drain criterion.
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

// walkLogChunks iterates every (filerId, chunk-time) tuple under SystemLogDir.
// cb returns false to stop early.
func walkLogChunks(ctx context.Context, lister LogDirLister, cb func(filerId string, ts time.Time) bool) error {
	dayEntries, err := listAll(ctx, lister, SystemLogDir)
	if err != nil {
		return err
	}
	for _, day := range dayEntries {
		if !day.IsDirectory {
			continue
		}
		hourMinuteEntries, err := listAll(ctx, lister, SystemLogDir+"/"+day.Name)
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

// listingLimit is intentionally large; per-day SystemLogDir holds ~1440 entries.
const listingLimit = 4096

func listAll(ctx context.Context, lister LogDirLister, dir string) ([]*filer_pb.Entry, error) {
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
	return out, nil
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
