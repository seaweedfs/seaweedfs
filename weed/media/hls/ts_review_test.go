package hls

import (
	"strings"
	"testing"
)

func TestTargetDurationRejectsRoundedSegmentAboveBound(t *testing.T) {
	playlist := `#EXTM3U
#EXT-X-TARGETDURATION:6
#EXTINF:6.6,
#EXT-X-BYTERANGE:188@0
video.ts
#EXT-X-ENDLIST
`
	_, err := ParseSingleFilePlaylist([]byte(playlist))
	if err == nil || !strings.Contains(err.Error(), "EXT-X-TARGETDURATION") {
		t.Fatalf("error = %v, want target-duration rejection", err)
	}
}
