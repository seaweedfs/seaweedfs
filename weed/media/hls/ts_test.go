package hls

import (
	"strings"
	"testing"
)

func TestParseSingleFilePlaylistExplicitOffsets(t *testing.T) {
	playlist := `#EXTM3U
#EXT-X-VERSION:4
#EXT-X-TARGETDURATION:7
#EXT-X-MEDIA-SEQUENCE:3
#EXTINF:6.006,
#EXT-X-BYTERANGE:1000@0
video.ts
#EXTINF:5.994,
#EXT-X-BYTERANGE:900@1000
video.ts
#EXT-X-ENDLIST
`
	metadata, err := ParseSingleFilePlaylist([]byte(playlist))
	if err != nil {
		t.Fatalf("ParseSingleFilePlaylist() error = %v", err)
	}
	if metadata.TargetDuration != 7 || metadata.MediaSequence != 3 {
		t.Fatalf("unexpected playlist metadata: %+v", metadata)
	}
	if len(metadata.Segments) != 2 {
		t.Fatalf("segment count = %d, want 2", len(metadata.Segments))
	}
	if got := metadata.Segments[1]; got.Offset != 1000 || got.Size != 900 || got.Duration != 5.994 {
		t.Fatalf("segment[1] = %+v", got)
	}
}

func TestParseSingleFilePlaylistImplicitOffsets(t *testing.T) {
	playlist := `#EXTM3U
#EXTINF:4.0,
#EXT-X-BYTERANGE:1880@0
video.ts
#EXTINF:4.0,
#EXT-X-BYTERANGE:3760
video.ts
#EXT-X-ENDLIST
`
	metadata, err := ParseSingleFilePlaylist([]byte(playlist))
	if err != nil {
		t.Fatalf("ParseSingleFilePlaylist() error = %v", err)
	}
	if metadata.TargetDuration != 4 {
		t.Fatalf("target duration = %d, want 4", metadata.TargetDuration)
	}
	if got := metadata.Segments[1].Offset; got != 1880 {
		t.Fatalf("implicit offset = %d, want 1880", got)
	}
}

func TestParseSingleFilePlaylistRejectsMultipleMediaURIs(t *testing.T) {
	playlist := `#EXTM3U
#EXTINF:4,
#EXT-X-BYTERANGE:100@0
one.ts
#EXTINF:4,
#EXT-X-BYTERANGE:100@100
two.ts
#EXT-X-ENDLIST
`
	_, err := ParseSingleFilePlaylist([]byte(playlist))
	if err == nil || !strings.Contains(err.Error(), "not single-file HLS") {
		t.Fatalf("error = %v, want single-file rejection", err)
	}
}

func TestParseSingleFilePlaylistRejectsGap(t *testing.T) {
	playlist := `#EXTM3U
#EXTINF:4,
#EXT-X-BYTERANGE:100@0
video.ts
#EXTINF:4,
#EXT-X-BYTERANGE:100@101
video.ts
#EXT-X-ENDLIST
`
	_, err := ParseSingleFilePlaylist([]byte(playlist))
	if err == nil || !strings.Contains(err.Error(), "non-contiguous") {
		t.Fatalf("error = %v, want non-contiguous rejection", err)
	}
}

func TestParseSingleFilePlaylistRequiresEndList(t *testing.T) {
	playlist := `#EXTM3U
#EXTINF:4,
#EXT-X-BYTERANGE:100@0
video.ts
`
	_, err := ParseSingleFilePlaylist([]byte(playlist))
	if err == nil || !strings.Contains(err.Error(), "EXT-X-ENDLIST") {
		t.Fatalf("error = %v, want VOD rejection", err)
	}
}

func TestParseSingleFilePlaylistRequiresByteRange(t *testing.T) {
	playlist := `#EXTM3U
#EXTINF:4,
segment.ts
#EXT-X-ENDLIST
`
	_, err := ParseSingleFilePlaylist([]byte(playlist))
	if err == nil || !strings.Contains(err.Error(), "EXT-X-BYTERANGE") {
		t.Fatalf("error = %v, want byte-range rejection", err)
	}
}

func TestParseSingleFilePlaylistRejectsUnsupportedStatefulTags(t *testing.T) {
	tests := []string{
		"#EXT-X-KEY:METHOD=AES-128,URI=key.bin",
		"#EXT-X-DISCONTINUITY",
		"#EXT-X-MAP:URI=init.mp4",
		"#EXT-X-GAP",
		"#EXT-X-I-FRAMES-ONLY",
	}
	for _, tag := range tests {
		playlist := "#EXTM3U\n" + tag + "\n#EXTINF:4,\n#EXT-X-BYTERANGE:188@0\nvideo.ts\n#EXT-X-ENDLIST\n"
		if _, err := ParseSingleFilePlaylist([]byte(playlist)); err == nil {
			t.Fatalf("ParseSingleFilePlaylist accepted unsupported tag %q", tag)
		}
	}
}

func TestParseSingleFilePlaylistRejectsByteRangeOverflow(t *testing.T) {
	playlist := `#EXTM3U
#EXTINF:4,
#EXT-X-BYTERANGE:9223372036854775807@0
video.ts
#EXTINF:4,
#EXT-X-BYTERANGE:1
video.ts
#EXT-X-ENDLIST
`
	_, err := ParseSingleFilePlaylist([]byte(playlist))
	if err == nil || !strings.Contains(err.Error(), "overflows int64") {
		t.Fatalf("error = %v, want overflow rejection", err)
	}
}

func TestParseSingleFilePlaylistRejectsMediaSequenceOverflow(t *testing.T) {
	playlist := `#EXTM3U
#EXT-X-MEDIA-SEQUENCE:9223372036854775807
#EXTINF:4,
#EXT-X-BYTERANGE:188@0
video.ts
#EXTINF:4,
#EXT-X-BYTERANGE:188
video.ts
#EXT-X-ENDLIST
`
	_, err := ParseSingleFilePlaylist([]byte(playlist))
	if err == nil || !strings.Contains(err.Error(), "overflows segment numbering") {
		t.Fatalf("error = %v, want media-sequence overflow rejection", err)
	}
}

func TestTargetDurationUsesRFC8216Rounding(t *testing.T) {
	playlist := `#EXTM3U
#EXT-X-TARGETDURATION:6
#EXTINF:6.006,
#EXT-X-BYTERANGE:100@0
video.ts
#EXT-X-ENDLIST
`
	if _, err := ParseSingleFilePlaylist([]byte(playlist)); err != nil {
		t.Fatalf("6.006 second segment is valid with target duration 6 after nearest-integer rounding: %v", err)
	}
}

func TestValidateTSPackets(t *testing.T) {
	packet := func(sync byte) []byte {
		p := make([]byte, TSPacketSize)
		p[0] = sync
		return p
	}

	valid := append(packet(0x47), packet(0x47)...)
	if err := ValidateTSPackets(valid); err != nil {
		t.Fatalf("ValidateTSPackets(valid) error = %v", err)
	}

	if err := ValidateTSPackets(packet(0x47)[:187]); err == nil {
		t.Fatal("ValidateTSPackets accepted a non-packet-aligned length")
	}

	badSync := append(packet(0x47), packet(0x00)...)
	if err := ValidateTSPackets(badSync); err == nil {
		t.Fatal("ValidateTSPackets accepted a packet without the 0x47 sync byte")
	}

	// An MP4 begins with a box size and the "ftyp" fourcc, never 0x47.
	mp4 := make([]byte, TSPacketSize)
	copy(mp4, []byte{0x00, 0x00, 0x00, 0x20, 'f', 't', 'y', 'p'})
	if err := ValidateTSPackets(mp4); err == nil {
		t.Fatal("ValidateTSPackets accepted MP4 content as MPEG-TS")
	}
}

func TestValidate(t *testing.T) {
	metadata := &Metadata{
		Version:        MetadataVersion,
		TargetDuration: 6,
		Segments: []Segment{
			{Offset: 0, Size: 100, Duration: 6},
			{Offset: 100, Size: 200, Duration: 6},
		},
	}
	if err := Validate(metadata, 300, 256); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if err := Validate(metadata, 301, 256); err == nil {
		t.Fatal("Validate() accepted mismatched file size")
	}
	if err := Validate(metadata, 300, 150); err == nil {
		t.Fatal("Validate() accepted oversized segment")
	}

	overflow := &Metadata{
		Version:        MetadataVersion,
		TargetDuration: 6,
		Segments: []Segment{
			{Offset: 0, Size: maxInt64Value, Duration: 6},
			{Offset: maxInt64Value, Size: 1, Duration: 6},
		},
	}
	if err := Validate(overflow, -1, 0); err == nil {
		t.Fatal("Validate() accepted overflowing segment ranges")
	}
}

func TestRenderMediaPlaylistUsesPlainSegmentURIs(t *testing.T) {
	metadata := &Metadata{
		Version:        MetadataVersion,
		TargetDuration: 7,
		MediaSequence:  3,
		Segments: []Segment{
			{Offset: 0, Size: 100, Duration: 6.006},
			{Offset: 100, Size: 200, Duration: 5.994},
		},
	}
	got := string(RenderMediaPlaylist(metadata))
	want := `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:7
#EXT-X-MEDIA-SEQUENCE:3
#EXT-X-PLAYLIST-TYPE:VOD
#EXTINF:6.006,
3.ts
#EXTINF:5.994,
4.ts
#EXT-X-ENDLIST
`
	if got != want {
		t.Fatalf("playlist mismatch\n--- got ---\n%s--- want ---\n%s", got, want)
	}
}
