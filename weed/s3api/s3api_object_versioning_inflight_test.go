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

func invertedVersionId(at time.Time) string {
	return fmt.Sprintf("%016x", uint64(math.MaxInt64-at.UnixNano())) + "0000000000000000"
}

func versionPointerEntry(versionId string) *filer_pb.Entry {
	return &filer_pb.Entry{Extended: map[string][]byte{s3_constants.ExtLatestVersionIdKey: []byte(versionId)}}
}

func TestVersionPointerGraceLeft(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name     string
		entry    *filer_pb.Entry
		min, max time.Duration
	}{
		{"just minted", versionPointerEntry(generateVersionId(true)), staleVersionPointerGrace - time.Minute, staleVersionPointerGrace},
		{"just minted, old format", versionPointerEntry(generateVersionId(false)), staleVersionPointerGrace - time.Minute, staleVersionPointerGrace},
		{"minted within the grace", versionPointerEntry(invertedVersionId(now.Add(-staleVersionPointerGrace + 2*time.Minute))), time.Minute, 2 * time.Minute},
		{"minted in the future", versionPointerEntry(invertedVersionId(now.Add(time.Hour))), staleVersionPointerGrace, staleVersionPointerGrace},
		{"minted before the grace", versionPointerEntry(invertedVersionId(now.Add(-2 * staleVersionPointerGrace))), 0, 0},
		{"null version", versionPointerEntry("null"), 0, 0},
		{"no pointer", &filer_pb.Entry{}, 0, 0},
	} {
		if got := versionPointerGraceLeft(tc.entry); got < tc.min || got > tc.max {
			t.Errorf("%s: versionPointerGraceLeft = %v, want within [%v, %v]", tc.name, got, tc.min, tc.max)
		}
	}
}

func newInFlightPointerServer(t *testing.T, versionId string, withFile bool) *S3ApiServer {
	t.Helper()
	f := &anonymousReadFiler{entries: make(map[string]*filer_pb.Entry)}
	fileName := "v_" + versionId
	f.entries["/buckets/b/k"+s3_constants.VersionsFolder] = &filer_pb.Entry{
		Name:        "k" + s3_constants.VersionsFolder,
		IsDirectory: true,
		Extended: map[string][]byte{
			s3_constants.ExtLatestVersionIdKey:       []byte(versionId),
			s3_constants.ExtLatestVersionFileNameKey: []byte(fileName),
		},
	}
	if withFile {
		f.entries["/buckets/b/k"+s3_constants.VersionsFolder+"/"+fileName] = &filer_pb.Entry{Name: fileName}
	}
	return newPutTestServer(t, startFakeFiler(t, f))
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
			s3a := newInFlightPointerServer(t, generateVersionId(true), tc.withFile)
			err := s3a.healVersionsPointer("b", "k")
			var inFlight versionPointerInFlightError
			if tc.wantInFlight {
				if !errors.As(err, &inFlight) || inFlight.graceLeft <= 0 {
					t.Fatalf("healVersionsPointer = %v, want in flight", err)
				}
			} else if err != nil {
				t.Fatalf("healVersionsPointer = %v, want nil", err)
			}
		})
	}
}

func TestDrainVersionsHealQueueRequeuesInFlightForGraceLeft(t *testing.T) {
	s3a := newInFlightPointerServer(t, invertedVersionId(time.Now().Add(-staleVersionPointerGrace+time.Minute)), false)
	s3a.versionsHealQueue = newVersionsHealQueue()
	s3a.versionsHealQueue.Enqueue("b", "k")
	s3a.drainVersionsHealQueue()

	c := s3a.versionsHealQueue.pending[versionsHealKey("b", "k")]
	if c == nil {
		t.Fatal("in-flight candidate was dropped from the queue")
	}
	if c.attempts != 0 {
		t.Errorf("attempts = %d, want 0", c.attempts)
	}
	if wait := time.Until(c.nextRetry); wait <= 0 || wait > time.Minute {
		t.Errorf("next retry in %v, want within the minute of grace left", wait)
	}
}
