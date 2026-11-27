package s3api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/protobuf/proto"
)

func invertedVersionId(at time.Time) string {
	return fmt.Sprintf("%016x", uint64(math.MaxInt64-at.UnixNano())) + "0000000000000000"
}

func versionPointerEntry(versionId string) *filer_pb.Entry {
	return &filer_pb.Entry{Extended: map[string][]byte{s3_constants.ExtLatestVersionIdKey: []byte(versionId)}}
}

func committedAt(entry *filer_pb.Entry, at time.Time) *filer_pb.Entry {
	entry.CommitTsNs = at.UnixNano()
	return entry
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
		{"minted before the grace, stored just now", committedAt(versionPointerEntry(invertedVersionId(now.Add(-2*staleVersionPointerGrace))), now), staleVersionPointerGrace - time.Minute, staleVersionPointerGrace},
		{"minted and stored before the grace", committedAt(versionPointerEntry(invertedVersionId(now.Add(-2*staleVersionPointerGrace))), now.Add(-staleVersionPointerGrace)), 0, 0},
		{"null version stored just now", committedAt(versionPointerEntry("null"), now), staleVersionPointerGrace - time.Minute, staleVersionPointerGrace},
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

type healPointerFiler struct {
	*anonymousReadFiler
	updates []*filer_pb.UpdateEntryRequest
}

func (f *healPointerFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	f.mu.Lock()
	var names []string
	for p := range f.entries {
		if dir, name := path.Split(p); strings.TrimSuffix(dir, "/") == req.Directory && name > req.StartFromFileName {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var entries []*filer_pb.Entry
	for _, name := range names {
		entries = append(entries, proto.Clone(f.entries[path.Join(req.Directory, name)]).(*filer_pb.Entry))
	}
	f.mu.Unlock()
	for _, entry := range entries {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: entry}); err != nil {
			return err
		}
	}
	return nil
}

func (f *healPointerFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, req)
	f.entries[path.Join(req.Directory, req.Entry.Name)] = proto.Clone(req.Entry).(*filer_pb.Entry)
	return &filer_pb.UpdateEntryResponse{}, nil
}

func newHealPointerServer(t *testing.T, stored *filer_pb.Entry, presentVersionId string) (*S3ApiServer, *healPointerFiler) {
	t.Helper()
	f := &healPointerFiler{anonymousReadFiler: &anonymousReadFiler{entries: make(map[string]*filer_pb.Entry)}}
	stored.Name = "k" + s3_constants.VersionsFolder
	stored.IsDirectory = true
	f.entries["/buckets/b/"+stored.Name] = stored
	fileName := "v_" + presentVersionId
	f.entries["/buckets/b/"+stored.Name+"/"+fileName] = &filer_pb.Entry{
		Name:     fileName,
		Extended: map[string][]byte{s3_constants.ExtVersionIdKey: []byte(presentVersionId)},
	}
	return newPutTestServer(t, startFakeFiler(t, f)), f
}

func TestHealPointerlessVersionsEntryIgnoresRecentCommit(t *testing.T) {
	presentId := invertedVersionId(time.Now().Add(-time.Hour))
	stored := committedAt(&filer_pb.Entry{Extended: map[string][]byte{}}, time.Now())
	s3a, f := newHealPointerServer(t, stored, presentId)

	if _, err := s3a.healStaleLatestVersionPointer("b", "k", proto.Clone(stored).(*filer_pb.Entry), ""); err != nil {
		t.Fatalf("healStaleLatestVersionPointer: %v", err)
	}
	if len(f.updates) != 1 {
		t.Fatalf("updates = %d, want the pointer repaired", len(f.updates))
	}
	if got := string(f.updates[0].Entry.Extended[s3_constants.ExtLatestVersionIdKey]); got != presentId {
		t.Errorf("repaired pointer = %q, want %q", got, presentId)
	}
}

func TestHealStalePointerDefersWhenLiveEntryWasJustStored(t *testing.T) {
	old := time.Now().Add(-2 * staleVersionPointerGrace)
	pointerId := invertedVersionId(old)
	stored := committedAt(&filer_pb.Entry{Extended: map[string][]byte{
		s3_constants.ExtLatestVersionIdKey:       []byte(pointerId),
		s3_constants.ExtLatestVersionFileNameKey: []byte("v_" + pointerId),
	}}, old)
	s3a, f := newHealPointerServer(t, stored, invertedVersionId(old.Add(-time.Hour)))
	snapshot := proto.Clone(stored).(*filer_pb.Entry)
	stored.CommitTsNs = time.Now().UnixNano()

	if _, err := s3a.healStaleLatestVersionPointer("b", "k", snapshot, "v_"+pointerId); err != nil {
		t.Fatalf("healStaleLatestVersionPointer: %v", err)
	}
	if len(f.updates) != 0 {
		t.Fatalf("updates = %d, want the repair deferred for the pointer stored again", len(f.updates))
	}
}
