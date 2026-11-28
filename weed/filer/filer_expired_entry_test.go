package filer

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func putSweepTestEntry(t *testing.T, store *stubFilerStore, path string, isDir bool, ttlSec int32, crtime time.Time) {
	t.Helper()
	mode := os.FileMode(0o644)
	if isDir {
		mode = os.ModeDir | 0o755
	}
	entry := &Entry{FullPath: util.FullPath(path), Attr: Attr{Mode: mode, Crtime: crtime, Mtime: crtime, TtlSec: ttlSec}}
	if err := store.InsertEntry(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteExpiredEntryKeepsAReplacement(t *testing.T) {
	f, store := newTestFilerWithStubStore(t)
	ctx := context.Background()
	p := util.FullPath("/logs/current")

	putSweepTestEntry(t, store, string(p), false, 120, time.Now())
	if err := f.deleteExpiredEntry(ctx, p); err != nil {
		t.Fatal(err)
	}
	if store.getEntry(string(p)) == nil {
		t.Fatal("live replacement removed")
	}

	putSweepTestEntry(t, store, string(p), false, 120, time.Now().Add(-time.Hour))
	lock := f.EntryLockTable.AcquireLock("test", p, util.ExclusiveLock)
	if err := f.deleteExpiredEntry(ctx, p); err != nil {
		t.Fatal(err)
	}
	f.EntryLockTable.ReleaseLock(p, lock)
	if store.getEntry(string(p)) == nil {
		t.Fatal("removed while a writer held the path")
	}

	s3Expiring := &Entry{FullPath: p, Attr: Attr{Mode: 0o644, Crtime: time.Now().Add(-time.Hour), Mtime: time.Now(), TtlSec: 120},
		Extended: map[string][]byte{s3_constants.SeaweedFSExpiresS3: []byte("true")}}
	if err := store.InsertEntry(ctx, s3Expiring); err != nil {
		t.Fatal(err)
	}
	if err := f.deleteExpiredEntry(ctx, p); err != nil {
		t.Fatal(err)
	}
	if store.getEntry(string(p)) == nil {
		t.Fatal("replacement under S3 expiration removed by its creation time")
	}

	putSweepTestEntry(t, store, string(p), false, 120, time.Now().Add(-time.Hour))
	if err := f.deleteExpiredEntry(ctx, p); err != nil {
		t.Fatal(err)
	}
	if store.getEntry(string(p)) != nil {
		t.Fatal("expired entry kept")
	}
}

// Another filer on the same store does not take this filer's lock: a write it
// makes between the re-read and the delete gets the delete's commit refused.
func TestDeleteExpiredEntryKeepsAReplacementFromAnotherFiler(t *testing.T) {
	f, _ := newTestFilerWithStubStore(t)
	store := newVersionedStore()
	f.Store = NewFilerStoreWrapper(store)
	other := NewFilerStoreWrapper(store)
	ctx := context.Background()
	p := util.FullPath("/logs/current")
	old := time.Now().Add(-time.Hour)
	expired := &Entry{FullPath: p, Attr: Attr{Mode: 0o644, Crtime: old, Mtime: old, TtlSec: 120}}

	if err := other.InsertEntry(ctx, expired); err != nil {
		t.Fatal(err)
	}
	ran := store.once(t, "r"+string(p), func() error {
		return other.InsertEntry(ctx, &Entry{FullPath: p, Attr: Attr{Mode: 0o644, Crtime: time.Now(), Mtime: time.Now(), TtlSec: 120}})
	})
	if err := f.deleteExpiredEntry(ctx, p); err != nil {
		t.Fatal(err)
	}
	if !*ran {
		t.Fatal("the expired entry was not re-read in a transaction")
	}
	if store.getEntry(string(p)) == nil {
		t.Fatal("replacement written through another filer removed")
	}

	store.afterRead = nil
	if err := other.InsertEntry(ctx, expired); err != nil {
		t.Fatal(err)
	}
	if err := f.deleteExpiredEntry(ctx, p); err != nil {
		t.Fatal(err)
	}
	if store.getEntry(string(p)) != nil {
		t.Fatal("expired entry kept")
	}
}

// replacingStore writes a fresh entry over replaced once a listing has read
// the directory, as an upload would between the listing and its expiry delete.
type replacingStore struct {
	*stubFilerStore
	replaced util.FullPath
}

func (s *replacingStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc ListEachEntryFunc) (string, error) {
	last, err := s.stubFilerStore.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, prefix, eachEntryFunc)
	if err == nil && s.replaced != "" {
		now := time.Now()
		fresh := &Entry{FullPath: s.replaced, Attr: Attr{Mode: 0o644, Crtime: now, Mtime: now, TtlSec: 120}}
		s.replaced = ""
		if err := s.stubFilerStore.InsertEntry(ctx, fresh); err != nil {
			return last, err
		}
	}
	return last, err
}

func TestListingKeepsAnExpiredEntryReplacedMeanwhile(t *testing.T) {
	f, stub := newTestFilerWithStubStore(t)
	p := util.FullPath("/clips/latest.mp4")
	f.Store = NewFilerStoreWrapper(&replacingStore{stubFilerStore: stub, replaced: p})
	putSweepTestEntry(t, stub, "/clips", true, 0, time.Now())
	putSweepTestEntry(t, stub, string(p), false, 120, time.Now().Add(-time.Hour))

	if _, _, err := f.ListDirectoryEntries(context.Background(), "/clips", "", false, 100, "", "", ""); err != nil {
		t.Fatal(err)
	}
	if stub.getEntry(string(p)) == nil {
		t.Fatal("replacement written after the listing read the expired row was removed")
	}
}
