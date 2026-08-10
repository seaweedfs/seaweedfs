package hlsts

import (
	"net/url"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/format"
	"github.com/seaweedfs/seaweedfs/weed/format/formattest"
)

const ffmpegPlaylist = `#EXTM3U
#EXT-X-VERSION:4
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:VOD
#EXTINF:6.000000,
#EXT-X-BYTERANGE:1128@0
video.ts
#EXTINF:6.000000,
#EXT-X-BYTERANGE:940
video.ts
#EXTINF:2.500000,
#EXT-X-BYTERANGE:376@2068
video.ts
#EXT-X-ENDLIST
`

func TestIndexSidecar(t *testing.T) {
	layout, err := Adapter{}.IndexSidecar([]byte(ffmpegPlaylist))
	if err != nil {
		t.Fatalf("IndexSidecar() error = %v", err)
	}
	wantSizes := []int64{1128, 940, 376}
	if len(layout.ExtentSizes) != len(wantSizes) {
		t.Fatalf("extents = %v, want %v", layout.ExtentSizes, wantSizes)
	}
	for i := range wantSizes {
		if layout.ExtentSizes[i] != wantSizes[i] {
			t.Fatalf("extent %d = %d, want %d", i, layout.ExtentSizes[i], wantSizes[i])
		}
	}
	if layout.Align != TSPacketSize || layout.Format != FormatName {
		t.Fatalf("layout = %+v", layout)
	}
	if err := layout.Validate(1128 + 940 + 376); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	formattest.EncodeRoundTrip(t, layout)
}

func TestIndexSidecarDefaultsTargetDuration(t *testing.T) {
	playlist := "#EXTM3U\n#EXTINF:5.6,\n#EXT-X-BYTERANGE:188@0\nv.ts\n#EXT-X-ENDLIST\n"
	layout, err := Adapter{}.IndexSidecar([]byte(playlist))
	if err != nil {
		t.Fatalf("IndexSidecar() error = %v", err)
	}
	info, err := decodePlaylistInfo(layout.Payload, len(layout.ExtentSizes))
	if err != nil {
		t.Fatalf("decodePlaylistInfo() error = %v", err)
	}
	if info.TargetDuration != 6 {
		t.Fatalf("TargetDuration = %d, want 6", info.TargetDuration)
	}
}

func TestIndexSidecarRejections(t *testing.T) {
	tests := []struct {
		name     string
		playlist string
		wantErr  string
	}{
		{"missing header", "#EXTINF:6,\n#EXT-X-BYTERANGE:188@0\nv.ts\n#EXT-X-ENDLIST\n", "EXTM3U"},
		{"missing endlist", "#EXTM3U\n#EXTINF:6,\n#EXT-X-BYTERANGE:188@0\nv.ts\n", "EXT-X-ENDLIST"},
		{"no segments", "#EXTM3U\n#EXT-X-ENDLIST\n", "no media segments"},
		{"encryption", "#EXTM3U\n#EXT-X-KEY:METHOD=AES-128\n#EXTINF:6,\n#EXT-X-BYTERANGE:188@0\nv.ts\n#EXT-X-ENDLIST\n", "EXT-X-KEY"},
		{"discontinuity", "#EXTM3U\n#EXTINF:6,\n#EXT-X-BYTERANGE:188@0\nv.ts\n#EXT-X-DISCONTINUITY\n#EXT-X-ENDLIST\n", "EXT-X-DISCONTINUITY"},
		{"map", "#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:6,\n#EXT-X-BYTERANGE:188@0\nv.ts\n#EXT-X-ENDLIST\n", "EXT-X-MAP"},
		{"gap", "#EXTM3U\n#EXT-X-GAP\n#EXTINF:6,\n#EXT-X-BYTERANGE:188@0\nv.ts\n#EXT-X-ENDLIST\n", "EXT-X-GAP"},
		{"iframes only", "#EXTM3U\n#EXT-X-I-FRAMES-ONLY\n#EXTINF:6,\n#EXT-X-BYTERANGE:188@0\nv.ts\n#EXT-X-ENDLIST\n", "EXT-X-I-FRAMES-ONLY"},
		{"no byterange", "#EXTM3U\n#EXTINF:6,\nv.ts\n#EXT-X-ENDLIST\n", "EXT-X-BYTERANGE"},
		{"gap in ranges", "#EXTM3U\n#EXTINF:6,\n#EXT-X-BYTERANGE:188@0\nv.ts\n#EXTINF:6,\n#EXT-X-BYTERANGE:188@376\nv.ts\n#EXT-X-ENDLIST\n", "non-contiguous"},
		{"unaligned size", "#EXTM3U\n#EXTINF:6,\n#EXT-X-BYTERANGE:100@0\nv.ts\n#EXT-X-ENDLIST\n", "TS packet"},
		{"two media files", "#EXTM3U\n#EXTINF:6,\n#EXT-X-BYTERANGE:188@0\na.ts\n#EXTINF:6,\n#EXT-X-BYTERANGE:188@188\nb.ts\n#EXT-X-ENDLIST\n", "one shared media URI"},
		{"target too small", "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:6,\n#EXT-X-BYTERANGE:188@0\nv.ts\n#EXT-X-ENDLIST\n", "smaller than"},
		{"zero duration", "#EXTM3U\n#EXTINF:0,\n#EXT-X-BYTERANGE:188@0\nv.ts\n#EXT-X-ENDLIST\n", "EXTINF"},
		{"dangling extinf", "#EXTM3U\n#EXTINF:6,\n#EXT-X-BYTERANGE:188@0\nv.ts\n#EXTINF:6,\n#EXT-X-ENDLIST\n", "incomplete"},
	}
	for _, test := range tests {
		_, err := Adapter{}.IndexSidecar([]byte(test.playlist))
		if err == nil || !strings.Contains(err.Error(), test.wantErr) {
			t.Fatalf("%s: error = %v, want %q", test.name, err, test.wantErr)
		}
	}
}

func TestSidecarTruncations(t *testing.T) {
	formattest.SidecarTruncations(t, Adapter{}, []byte(ffmpegPlaylist))
}

func viewObject(t *testing.T) format.Object {
	t.Helper()
	layout, err := Adapter{}.IndexSidecar([]byte(ffmpegPlaylist))
	if err != nil {
		t.Fatalf("IndexSidecar() error = %v", err)
	}
	return format.Object{Name: "movie.ts", Size: layout.TotalSize(), Layout: layout}
}

func TestViewPlaylist(t *testing.T) {
	plan, err := Adapter{}.View(format.ViewRequest{Query: url.Values{}}, viewObject(t))
	if err != nil {
		t.Fatalf("View() error = %v", err)
	}
	if plan.ContentType != PlaylistContentType {
		t.Fatalf("ContentType = %q", plan.ContentType)
	}
	want := `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:VOD
#EXTINF:6.000,
movie.ts?format.view=hls-ts&seq=0
#EXTINF:6.000,
movie.ts?format.view=hls-ts&seq=1
#EXTINF:2.500,
movie.ts?format.view=hls-ts&seq=2
#EXT-X-ENDLIST
`
	if string(plan.Body) != want {
		t.Fatalf("playlist = %q, want %q", plan.Body, want)
	}
}

func TestViewSegment(t *testing.T) {
	obj := viewObject(t)
	plan, err := Adapter{}.View(format.ViewRequest{Query: url.Values{"seq": {"1"}}}, obj)
	if err != nil {
		t.Fatalf("View() error = %v", err)
	}
	if plan.Body != nil || plan.Extent != 1 || plan.ContentType != MediaContentType {
		t.Fatalf("plan = %+v", plan)
	}
	for _, bad := range []string{"3", "-1", "x", "9999999999999999999"} {
		if _, err := (Adapter{}).View(format.ViewRequest{Query: url.Values{"seq": {bad}}}, obj); err != format.ErrNoSuchView {
			t.Fatalf("seq %q: error = %v, want ErrNoSuchView", bad, err)
		}
	}
}

func TestViewSegmentHonorsMediaSequence(t *testing.T) {
	playlist := "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:10\n#EXTINF:6,\n#EXT-X-BYTERANGE:188@0\nv.ts\n#EXTINF:6,\n#EXT-X-BYTERANGE:376\nv.ts\n#EXT-X-ENDLIST\n"
	layout, err := Adapter{}.IndexSidecar([]byte(playlist))
	if err != nil {
		t.Fatalf("IndexSidecar() error = %v", err)
	}
	obj := format.Object{Name: "v.ts", Size: layout.TotalSize(), Layout: layout}
	plan, err := Adapter{}.View(format.ViewRequest{Query: url.Values{"seq": {"11"}}}, obj)
	if err != nil {
		t.Fatalf("View() error = %v", err)
	}
	if plan.Extent != 1 {
		t.Fatalf("Extent = %d, want 1", plan.Extent)
	}
	if _, err := (Adapter{}).View(format.ViewRequest{Query: url.Values{"seq": {"9"}}}, obj); err != format.ErrNoSuchView {
		t.Fatalf("seq below media sequence: error = %v, want ErrNoSuchView", err)
	}
}

func TestSniff(t *testing.T) {
	head := make([]byte, 400)
	head[0], head[TSPacketSize] = tsSyncByte, tsSyncByte
	if !(Adapter{}).Sniff(format.Hint{Head: head}) {
		t.Fatalf("Sniff() rejected TS head")
	}
	head[TSPacketSize] = 0
	if (Adapter{}).Sniff(format.Hint{Head: head}) {
		t.Fatalf("Sniff() accepted non-TS head")
	}
}
