package hls

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

type walkedChunk struct {
	segmentIndex int
	offset       int64
	data         string
}

type zeroBeforeEOFReader struct {
	reader   io.Reader
	returned bool
}

func (r *zeroBeforeEOFReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n == 0 && err == io.EOF && !r.returned {
		r.returned = true
		return 0, nil
	}
	return n, err
}

func TestWalkSegmentChunks(t *testing.T) {
	metadata := &Metadata{
		Version:        MetadataVersion,
		TargetDuration: 6,
		Segments: []Segment{
			{Offset: 0, Size: 3, Duration: 6},
			{Offset: 3, Size: 2, Duration: 6},
		},
	}
	var got []walkedChunk
	err := WalkSegmentChunks(bytes.NewBufferString("abcde"), metadata, 16, func(i int, offset int64, data []byte) error {
		got = append(got, walkedChunk{i, offset, string(data)})
		return nil
	})
	if err != nil {
		t.Fatalf("WalkSegmentChunks() error = %v", err)
	}
	want := []walkedChunk{{0, 0, "abc"}, {1, 3, "de"}}
	if len(got) != len(want) {
		t.Fatalf("chunks = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chunk %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestWalkSegmentChunksAcceptsTransientZeroReadBeforeEOF(t *testing.T) {
	metadata := &Metadata{
		Version:        MetadataVersion,
		TargetDuration: 6,
		Segments:       []Segment{{Offset: 0, Size: 3, Duration: 6}},
	}
	reader := &zeroBeforeEOFReader{reader: bytes.NewBufferString("abc")}
	if err := WalkSegmentChunks(reader, metadata, 16, func(int, int64, []byte) error { return nil }); err != nil {
		t.Fatalf("WalkSegmentChunks() rejected valid streaming EOF: %v", err)
	}
}

// A segment larger than the chunk limit is split into limit-sized chunks whose
// offsets stay inside the segment; the trailing chunk holds only the remainder
// and never crosses into the next segment.
func TestWalkSegmentChunksSplitsOversizedSegments(t *testing.T) {
	metadata := &Metadata{
		Version:        MetadataVersion,
		TargetDuration: 6,
		Segments: []Segment{
			{Offset: 0, Size: 5, Duration: 6},
			{Offset: 5, Size: 4, Duration: 6},
		},
	}
	var got []walkedChunk
	err := WalkSegmentChunks(bytes.NewBufferString("ABCDEFGHI"), metadata, 2, func(i int, offset int64, data []byte) error {
		got = append(got, walkedChunk{i, offset, string(data)})
		return nil
	})
	if err != nil {
		t.Fatalf("WalkSegmentChunks() error = %v", err)
	}
	want := []walkedChunk{
		{0, 0, "AB"}, {0, 2, "CD"}, {0, 4, "E"},
		{1, 5, "FG"}, {1, 7, "HI"},
	}
	if len(got) != len(want) {
		t.Fatalf("chunks = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chunk %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestWalkSegmentChunksSingleChunkPerSegmentWhenUnlimited(t *testing.T) {
	metadata := &Metadata{
		Version:        MetadataVersion,
		TargetDuration: 6,
		Segments: []Segment{
			{Offset: 0, Size: 5, Duration: 6},
			{Offset: 5, Size: 4, Duration: 6},
		},
	}
	var got []walkedChunk
	err := WalkSegmentChunks(bytes.NewBufferString("ABCDEFGHI"), metadata, 0, func(i int, offset int64, data []byte) error {
		got = append(got, walkedChunk{i, offset, string(data)})
		return nil
	})
	if err != nil {
		t.Fatalf("WalkSegmentChunks() error = %v", err)
	}
	want := []walkedChunk{{0, 0, "ABCDE"}, {1, 5, "FGHI"}}
	if len(got) != len(want) {
		t.Fatalf("chunks = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chunk %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestWalkSegmentChunksRejectsTruncatedInput(t *testing.T) {
	metadata := &Metadata{
		Version:        MetadataVersion,
		TargetDuration: 6,
		Segments:       []Segment{{Offset: 0, Size: 4, Duration: 6}},
	}
	err := WalkSegmentChunks(bytes.NewBufferString("abc"), metadata, 16, func(int, int64, []byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "read segment 0") {
		t.Fatalf("error = %v, want truncated input error", err)
	}
}

func TestWalkSegmentChunksRejectsTrailingInput(t *testing.T) {
	metadata := &Metadata{
		Version:        MetadataVersion,
		TargetDuration: 6,
		Segments:       []Segment{{Offset: 0, Size: 3, Duration: 6}},
	}
	err := WalkSegmentChunks(bytes.NewBufferString("abcd"), metadata, 16, func(int, int64, []byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "trailing bytes") {
		t.Fatalf("error = %v, want trailing input error", err)
	}
}

func TestWalkSegmentChunksPropagatesCallbackError(t *testing.T) {
	metadata := &Metadata{
		Version:        MetadataVersion,
		TargetDuration: 6,
		Segments:       []Segment{{Offset: 0, Size: 3, Duration: 6}},
	}
	sentinel := errors.New("upload failed")
	err := WalkSegmentChunks(bytes.NewBufferString("abc"), metadata, 16, func(int, int64, []byte) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want callback error", err)
	}
}

func TestTotalSize(t *testing.T) {
	metadata := &Metadata{Segments: []Segment{{Size: 10}, {Size: 20}, {Size: 30}}}
	if got := TotalSize(metadata); got != 60 {
		t.Fatalf("TotalSize() = %d, want 60", got)
	}
}
