package weed_server

import (
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	media_hls "github.com/seaweedfs/seaweedfs/weed/media/hls"
)

func TestHlsTsSegmentForSequence(t *testing.T) {
	metadata := &media_hls.Metadata{
		MediaSequence: 7,
		Segments: []media_hls.Segment{
			{Offset: 0, Size: 188, Duration: 4},
			{Offset: 188, Size: 376, Duration: 4},
		},
	}

	if _, ok := hlsTsSegmentForSequence(metadata, 6); ok {
		t.Fatal("sequence before media sequence unexpectedly resolved")
	}
	if got, ok := hlsTsSegmentForSequence(metadata, 7); !ok || got.Offset != 0 || got.Size != 188 {
		t.Fatalf("first segment = %+v, %v", got, ok)
	}
	if got, ok := hlsTsSegmentForSequence(metadata, 8); !ok || got.Offset != 188 || got.Size != 376 {
		t.Fatalf("second segment = %+v, %v", got, ok)
	}
	if _, ok := hlsTsSegmentForSequence(metadata, 9); ok {
		t.Fatal("sequence after last segment unexpectedly resolved")
	}
	if _, ok := hlsTsSegmentForSequence(nil, 0); ok {
		t.Fatal("nil metadata unexpectedly resolved")
	}
}

func TestApplyHlsTsPassthroughHeaders(t *testing.T) {
	entry := &filer.Entry{Extended: map[string][]byte{
		"Cache-Control":       []byte("public, max-age=60"),
		"Expires":             []byte("Wed, 21 Oct 2030 07:28:00 GMT"),
		hlsTsMetadataKey:      []byte("must-not-be-copied-by-this-helper"),
		"Content-Disposition": []byte("attachment"),
	}}
	recorder := httptest.NewRecorder()
	applyHlsTsPassthroughHeaders(recorder, entry)

	if got := recorder.Header().Get("Cache-Control"); got != "public, max-age=60" {
		t.Fatalf("Cache-Control = %q", got)
	}
	if got := recorder.Header().Get("Expires"); got == "" {
		t.Fatal("Expires header was not copied")
	}
	if got := recorder.Header().Get(hlsTsMetadataKey); got != "" {
		t.Fatalf("internal HLS metadata leaked as response header: %q", got)
	}
	if got := recorder.Header().Get("Content-Disposition"); got != "" {
		t.Fatalf("unexpected Content-Disposition passthrough: %q", got)
	}
}
