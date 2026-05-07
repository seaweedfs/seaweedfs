package pb

import (
	"context"
	"sort"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// fakeLogDirLister is a minimal in-memory LogDirLister for tests. The
// caller seeds entries via Add(dir, name, isDirectory); each ListLogDirEntries
// returns a stable lex-sorted slice with startFrom-based pagination.
type fakeLogDirLister struct {
	entries map[string][]*filer_pb.Entry // dir -> sorted entries
}

func newFakeLister() *fakeLogDirLister {
	return &fakeLogDirLister{entries: map[string][]*filer_pb.Entry{}}
}

func (f *fakeLogDirLister) Add(dir, name string, isDir bool) {
	f.entries[dir] = append(f.entries[dir], &filer_pb.Entry{
		Name:        name,
		IsDirectory: isDir,
	})
}

func (f *fakeLogDirLister) finalize() {
	for d, e := range f.entries {
		sort.Slice(e, func(i, j int) bool { return e[i].Name < e[j].Name })
		f.entries[d] = e
	}
}

func (f *fakeLogDirLister) ListLogDirEntries(ctx context.Context, dir, startFrom string, limit int) ([]*filer_pb.Entry, error) {
	all, ok := f.entries[dir]
	if !ok {
		return nil, nil
	}
	startIdx := 0
	for i, e := range all {
		if e.Name > startFrom {
			startIdx = i
			break
		}
		if i == len(all)-1 {
			startIdx = len(all)
		}
	}
	end := startIdx + limit
	if end > len(all) {
		end = len(all)
	}
	return all[startIdx:end], nil
}

// scenario builds a fake SystemLogDir with the given (day, hm, filerId) tuples.
type chunkSpec struct {
	day, hm, filerId string
}

func buildScenario(specs ...chunkSpec) *fakeLogDirLister {
	f := newFakeLister()
	dayDirs := map[string]bool{}
	for _, s := range specs {
		if !dayDirs[s.day] {
			f.Add(SystemLogDir, s.day, true)
			dayDirs[s.day] = true
		}
		f.Add(SystemLogDir+"/"+s.day, s.hm+"."+s.filerId, false)
	}
	f.finalize()
	return f
}

func TestEarliestRetainedPositionPerShard_BasicTwoFilers(t *testing.T) {
	f := buildScenario(
		chunkSpec{"2026-05-07", "10-30", "filer01"},
		chunkSpec{"2026-05-07", "10-31", "filer01"},
		chunkSpec{"2026-05-07", "10-32", "filer02"},
		chunkSpec{"2026-05-08", "00-00", "filer01"},
		chunkSpec{"2026-05-08", "00-00", "filer02"},
	)
	got, err := EarliestRetainedPositionPerShard(context.Background(), f)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// filer01's earliest is 2026-05-07 10:30; filer02's is 2026-05-07 10:32.
	if len(got) != 2 {
		t.Fatalf("want 2 filers, got %d (%v)", len(got), got)
	}
	if got["filer01"].Offset != 0 {
		t.Fatalf("Earliest should have Offset=0, got %v", got["filer01"])
	}
}

func TestEarliestRetainedPositionPerShard_EmptyDir(t *testing.T) {
	f := newFakeLister() // no entries
	got, err := EarliestRetainedPositionPerShard(context.Background(), f)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty dir should yield empty map, got %v", got)
	}
}

func TestRetainedLogRangePerShard_EarliestAndLatest(t *testing.T) {
	f := buildScenario(
		chunkSpec{"2026-05-07", "10-30", "filer01"},
		chunkSpec{"2026-05-07", "10-31", "filer01"},
		chunkSpec{"2026-05-07", "10-32", "filer02"},
		chunkSpec{"2026-05-08", "00-00", "filer01"},
		chunkSpec{"2026-05-08", "00-00", "filer02"},
	)
	got, err := RetainedLogRangePerShard(context.Background(), f)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 filers, got %v", got)
	}
	// Latest's Offset is int64-max so cursor >= range.latest in lex (ts,
	// offset) order is the design's tail-drain criterion.
	if got["filer01"].Latest.Offset != 1<<63-1 {
		t.Fatalf("Latest.Offset want max, got %v", got["filer01"].Latest)
	}
	// filer01: earliest 10-30 < latest 00-00 (next day).
	if !got["filer01"].Earliest.LessOrEqual(got["filer01"].Latest) {
		t.Fatalf("earliest should be <= latest, got %+v", got["filer01"])
	}
	if got["filer02"].Earliest.TsNs == got["filer02"].Latest.TsNs {
		// filer02 has chunks at 10-32 and 00-00 (next day) so distinct.
		t.Fatalf("filer02 earliest/latest should differ, got %+v", got["filer02"])
	}
}

func TestRetainedLogRangePerShard_SingleChunk(t *testing.T) {
	// One filer with a single chunk: earliest == latest's TsNs.
	f := buildScenario(chunkSpec{"2026-05-07", "10-30", "filer01"})
	got, err := RetainedLogRangePerShard(context.Background(), f)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got["filer01"].Earliest.TsNs != got["filer01"].Latest.TsNs {
		t.Fatalf("single chunk: TsNs should match, got %+v", got["filer01"])
	}
}

func TestWalkLogChunks_SkipsMalformedNames(t *testing.T) {
	f := newFakeLister()
	f.Add(SystemLogDir, "2026-05-07", true)
	// Valid + a malformed entry without a "." separator and one without filerId.
	f.Add(SystemLogDir+"/2026-05-07", "10-30.filer01", false)
	f.Add(SystemLogDir+"/2026-05-07", "junk", false)
	f.Add(SystemLogDir+"/2026-05-07", "11-00.", false)
	f.finalize()

	got, err := EarliestRetainedPositionPerShard(context.Background(), f)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(got) != 1 || got["filer01"].TsNs == 0 {
		t.Fatalf("only the valid chunk should yield a filer, got %v", got)
	}
}

func TestGetFilerIdFromName(t *testing.T) {
	cases := map[string]string{
		"10-30.filer01":     "filer01",
		"10-30.deadbeef":    "deadbeef",
		"junk":              "",
		"11-00.":            "",
		"":                  "",
		"a.b.c":             "c",
	}
	for name, want := range cases {
		if got := getFilerIdFromName(name); got != want {
			t.Fatalf("%q: want %q, got %q", name, want, got)
		}
	}
}

func TestParseChunkTime(t *testing.T) {
	if _, ok := parseChunkTime("2026-05-07", "10-30.filer01"); !ok {
		t.Fatalf("valid format should parse")
	}
	if _, ok := parseChunkTime("not-a-date", "10-30.filer01"); ok {
		t.Fatalf("bad day should not parse")
	}
	if _, ok := parseChunkTime("2026-05-07", "no-dot"); ok {
		t.Fatalf("missing dot should not parse")
	}
}
