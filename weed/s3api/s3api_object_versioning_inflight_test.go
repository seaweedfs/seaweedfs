package s3api

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestVersionPointerInFlight(t *testing.T) {
	inverted := func(at time.Time) string {
		return fmt.Sprintf("%016x", uint64(math.MaxInt64-at.UnixNano())) + "0000000000000000"
	}
	pointer := func(id string) *filer_pb.Entry {
		return &filer_pb.Entry{Extended: map[string][]byte{s3_constants.ExtLatestVersionIdKey: []byte(id)}}
	}
	now := time.Now()
	for _, tc := range []struct {
		name  string
		entry *filer_pb.Entry
		want  bool
	}{
		{"just minted", pointer(generateVersionId(true)), true},
		{"just minted, old format", pointer(generateVersionId(false)), true},
		{"minted within the grace", pointer(inverted(now.Add(-staleVersionPointerGrace / 2))), true},
		{"minted before the grace", pointer(inverted(now.Add(-2 * staleVersionPointerGrace))), false},
		{"null version", pointer("null"), false},
		{"no pointer", &filer_pb.Entry{}, false},
	} {
		if got := versionPointerInFlight(tc.entry); got != tc.want {
			t.Errorf("%s: versionPointerInFlight = %v, want %v", tc.name, got, tc.want)
		}
	}
}
