package weed_server

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	media_hls "github.com/seaweedfs/seaweedfs/weed/media/hls"
)

func TestHlsTsMaxChunkBytesSafelyHandlesOutOfRangeValues(t *testing.T) {
	alignDown := func(n int64) int64 { return n - n%media_hls.TSPacketSize }
	fs := &FilerServer{option: &FilerOption{MaxMB: 4}}

	for _, value := range []string{"9223372036854775808", "8796093022208"} {
		req := httptest.NewRequest(http.MethodPost, "/hls/video/movie?maxMB="+value, nil)
		if got, want := fs.hlsTsMaxChunkBytes(req), alignDown(4*hlsTsMiB); got != want {
			t.Fatalf("maxMB=%q produced %d, want inherited %d", value, got, want)
		}
	}

	if strconv.IntSize == 64 {
		tooLarge := hlsTsMaxSafeMB + 1
		fs.option.MaxMB = int(tooLarge)
		req := httptest.NewRequest(http.MethodPost, "/hls/video/movie", nil)
		if got, want := fs.hlsTsMaxChunkBytes(req), alignDown(hlsTsMaxSafeMB*hlsTsMiB); got != want {
			t.Fatalf("oversized filer MaxMB produced %d, want clamped %d", got, want)
		}
	}
}
