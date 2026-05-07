package pb

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// buildPositionedLogFile constructs the on-disk log file format with entries
// that carry both TsNs and Offset, so per-shard cursor tests can probe the
// strict (ts, offset) skip predicate.
func buildPositionedLogFile(entries []*filer_pb.LogEntry) []byte {
	var buf bytes.Buffer
	for _, le := range entries {
		// Wrap LogEntry's Data with a SubscribeMetadataResponse so the
		// path-filter step accepts it.
		event := &filer_pb.SubscribeMetadataResponse{
			Directory:         "/data/x",
			TsNs:              le.TsNs,
			EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "obj"}},
		}
		eventBytes, _ := proto.Marshal(event)
		le.Data = eventBytes
		entryBytes, _ := proto.Marshal(le)
		sizeBuf := make([]byte, 4)
		util.Uint32toBytes(sizeBuf, uint32(len(entryBytes)))
		buf.Write(sizeBuf)
		buf.Write(entryBytes)
	}
	return buf.Bytes()
}

type positionedTest struct {
	refs     []*filer_pb.LogFileChunkRef
	fileData map[string][]byte
}

func (p *positionedTest) reader() LogFileReaderFn {
	return func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error) {
		if len(chunks) == 0 {
			return nil, fmt.Errorf("no chunks")
		}
		key := chunks[0].FileId
		data, ok := p.fileData[key]
		if !ok {
			return nil, fmt.Errorf("file not found: %s", key)
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}
}

// newPositionedTest creates 2 filers, 1 file each, 4 entries per file with
// distinct (ts, offset) tuples. Useful for testing the merge order and skip
// predicate.
func newPositionedTest() *positionedTest {
	pt := &positionedTest{fileData: map[string][]byte{}}
	base := time.Now().Add(-time.Hour).UnixNano()
	build := func(filerId string, offsets []int64, tsBase int64) {
		entries := make([]*filer_pb.LogEntry, 0, len(offsets))
		for _, off := range offsets {
			entries = append(entries, &filer_pb.LogEntry{TsNs: tsBase + off, Offset: off})
		}
		key := filerId + ":" + filerId
		pt.fileData[key] = buildPositionedLogFile(entries)
		pt.refs = append(pt.refs, &filer_pb.LogFileChunkRef{
			Chunks:   []*filer_pb.FileChunk{{FileId: key}},
			FileTsNs: tsBase,
			FilerId:  filerId,
		})
	}
	build("alpha", []int64{0, 1, 2, 3}, base)
	build("beta", []int64{0, 1, 2, 3}, base) // identical timestamps -> tied; merge tiebreak by filerId
	return pt
}

func TestReadLogFileRefsWithPosition_OrderAndCursor(t *testing.T) {
	pt := newPositionedTest()
	startPositions := map[string]MessagePosition{
		"alpha": {TsNs: 0, Offset: 0},
		"beta":  {TsNs: 0, Offset: 0},
	}
	type emit struct {
		filer string
		pos   MessagePosition
	}
	var emits []emit
	last, err := ReadLogFileRefsWithPosition(pt.refs, pt.reader(), startPositions, 0, PathFilter{PathPrefix: "/"},
		func(event *filer_pb.SubscribeMetadataResponse, filerId string, pos MessagePosition) error {
			emits = append(emits, emit{filerId, pos})
			return nil
		})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// 8 events total (4 per filer); merge order is (ts, filerId, offset).
	if len(emits) != 8 {
		t.Fatalf("want 8 emits, got %d", len(emits))
	}
	// Verify monotonic non-decreasing TsNs. Tied ts -> alpha before beta
	// (deterministic filerId sort).
	for i := 1; i < len(emits); i++ {
		if emits[i].pos.TsNs < emits[i-1].pos.TsNs {
			t.Fatalf("emit %d: ts went backwards: %d -> %d", i, emits[i-1].pos.TsNs, emits[i].pos.TsNs)
		}
	}
	if last["alpha"].Offset != 3 || last["beta"].Offset != 3 {
		t.Fatalf("lastByFiler want offset 3 each, got %v", last)
	}
}

func TestReadLogFileRefsWithPosition_PerShardSkip(t *testing.T) {
	pt := newPositionedTest()
	// Tell alpha to skip its first two entries (offsets 0 and 1); beta starts fresh.
	startPositions := map[string]MessagePosition{
		"alpha": {TsNs: 0, Offset: 1}, // strict <=, so offset 0 and 1 are skipped
		"beta":  {TsNs: 0, Offset: 0},
	}
	// The startPositions reference TsNs=0 won't actually drop alpha's entries
	// because their TsNs is base+offset (much larger than 0). To exercise the
	// (ts, offset) compound predicate, we set startPositions to alpha's
	// second entry's exact (ts, offset).
	base := pt.refs[0].FileTsNs
	startPositions["alpha"] = MessagePosition{TsNs: base + 1, Offset: 1}

	var alphaEmits, betaEmits int
	last, err := ReadLogFileRefsWithPosition(pt.refs, pt.reader(), startPositions, 0, PathFilter{PathPrefix: "/"},
		func(event *filer_pb.SubscribeMetadataResponse, filerId string, pos MessagePosition) error {
			if filerId == "alpha" {
				alphaEmits++
			} else {
				betaEmits++
			}
			return nil
		})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// alpha startPos is (base+1, 1); strict <= skips offsets 0 and 1, leaving 2 and 3.
	if alphaEmits != 2 {
		t.Fatalf("alpha want 2 emits, got %d", alphaEmits)
	}
	// beta unchanged: 4 emits.
	if betaEmits != 4 {
		t.Fatalf("beta want 4 emits, got %d", betaEmits)
	}
	if last["alpha"].Offset != 3 {
		t.Fatalf("alpha lastByFiler want offset 3, got %v", last["alpha"])
	}
}

func TestReadLogFileRefsWithPosition_PausedShardEmitsNothing(t *testing.T) {
	pt := newPositionedTest()
	startPositions := map[string]MessagePosition{
		"alpha": MaxMessagePosition, // paused (active blocker on shard alpha)
		"beta":  {TsNs: 0, Offset: 0},
	}
	var alphaEmits, betaEmits int
	last, err := ReadLogFileRefsWithPosition(pt.refs, pt.reader(), startPositions, 0, PathFilter{PathPrefix: "/"},
		func(event *filer_pb.SubscribeMetadataResponse, filerId string, pos MessagePosition) error {
			if filerId == "alpha" {
				alphaEmits++
			} else {
				betaEmits++
			}
			return nil
		})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if alphaEmits != 0 {
		t.Fatalf("paused alpha should emit 0, got %d", alphaEmits)
	}
	if betaEmits != 4 {
		t.Fatalf("beta unaffected, want 4 emits, got %d", betaEmits)
	}
	if _, has := last["alpha"]; has {
		t.Fatalf("paused alpha should not produce a lastByFiler entry, got %v", last["alpha"])
	}
}

func TestReadLogFileRefsWithPosition_StopTsNsCaps(t *testing.T) {
	pt := newPositionedTest()
	startPositions := map[string]MessagePosition{
		"alpha": {TsNs: 0, Offset: 0},
		"beta":  {TsNs: 0, Offset: 0},
	}
	base := pt.refs[0].FileTsNs
	stopTsNs := base + 1 // only entries with TsNs <= base+1 should pass

	var emits int
	_, err := ReadLogFileRefsWithPosition(pt.refs, pt.reader(), startPositions, stopTsNs, PathFilter{PathPrefix: "/"},
		func(event *filer_pb.SubscribeMetadataResponse, filerId string, pos MessagePosition) error {
			emits++
			return nil
		})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Each filer has entries at TsNs in {base, base+1, base+2, base+3}; with
	// stopTsNs = base+1, two entries pass per filer (ts <= base+1). 2*2 = 4.
	if emits != 4 {
		t.Fatalf("with stop=%d want 4 emits, got %d", stopTsNs, emits)
	}
}

func TestReadLogFileRefsWithPosition_CallbackErrorHalts(t *testing.T) {
	pt := newPositionedTest()
	startPositions := map[string]MessagePosition{
		"alpha": {TsNs: 0, Offset: 0},
		"beta":  {TsNs: 0, Offset: 0},
	}
	wantErr := fmt.Errorf("halt")
	var emits int
	_, err := ReadLogFileRefsWithPosition(pt.refs, pt.reader(), startPositions, 0, PathFilter{PathPrefix: "/"},
		func(event *filer_pb.SubscribeMetadataResponse, filerId string, pos MessagePosition) error {
			emits++
			if emits == 2 {
				return wantErr
			}
			return nil
		})
	if err == nil || err.Error() != wantErr.Error() {
		t.Fatalf("want halt err, got %v", err)
	}
}

func TestMessagePosition_LessOrEqual(t *testing.T) {
	cases := []struct {
		a, b MessagePosition
		le   bool
	}{
		{MessagePosition{1, 0}, MessagePosition{2, 0}, true},
		{MessagePosition{2, 0}, MessagePosition{1, 0}, false},
		{MessagePosition{1, 5}, MessagePosition{1, 5}, true},  // equal
		{MessagePosition{1, 4}, MessagePosition{1, 5}, true},  // tied ts, smaller offset
		{MessagePosition{1, 6}, MessagePosition{1, 5}, false}, // tied ts, larger offset
	}
	for _, c := range cases {
		if got := c.a.LessOrEqual(c.b); got != c.le {
			t.Fatalf("(%v).LessOrEqual(%v) want %v, got %v", c.a, c.b, c.le, got)
		}
	}
}
