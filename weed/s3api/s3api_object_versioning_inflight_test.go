package s3api

import (
	"errors"
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

func TestHealVersionsPointerInFlight(t *testing.T) {
	for _, tc := range []struct {
		name         string
		withFile     bool
		wantInFlight bool
	}{
		{"recent pointer to a present file is consistent", true, false},
		{"recent pointer to a missing file waits", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &anonymousReadFiler{entries: make(map[string]*filer_pb.Entry)}
			versionId := generateVersionId(true)
			fileName := "v_" + versionId
			f.entries["/buckets/b/k"+s3_constants.VersionsFolder] = &filer_pb.Entry{
				Name:        "k" + s3_constants.VersionsFolder,
				IsDirectory: true,
				Extended: map[string][]byte{
					s3_constants.ExtLatestVersionIdKey:       []byte(versionId),
					s3_constants.ExtLatestVersionFileNameKey: []byte(fileName),
				},
			}
			if tc.withFile {
				f.entries["/buckets/b/k"+s3_constants.VersionsFolder+"/"+fileName] = &filer_pb.Entry{Name: fileName}
			}
			s3a := newPutTestServer(t, startFakeFiler(t, f))
			err := s3a.healVersionsPointer("b", "k")
			if tc.wantInFlight {
				if !errors.Is(err, errVersionPointerInFlight) {
					t.Fatalf("healVersionsPointer = %v, want in flight", err)
				}
			} else if err != nil {
				t.Fatalf("healVersionsPointer = %v, want nil", err)
			}
		})
	}
}
