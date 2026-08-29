package weed_server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const ttlRulePrefix = "/buckets/ttl/"

// addTtlRule gives ttlRulePrefix a 3 minute volume TTL, the fs.configure setting
// whose effect every write path below has to reproduce on the entries it stores.
func addTtlRule(t *testing.T, f *filer.Filer) {
	t.Helper()
	if err := f.FilerConf.AddLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: ttlRulePrefix,
		Ttl:            "3m",
	}); err != nil {
		t.Fatalf("AddLocationConf: %v", err)
	}
}

// An object written through ObjectTransaction (the routed S3 write path) must
// pick up the path's TTL rule, the same as one written through CreateEntry.
func TestObjectTransactionPutAppliesRuleTtl(t *testing.T) {
	store := newRenameTestStore()
	store.entries[ttlRulePrefix] = newDirectoryEntry(ttlRulePrefix, 10)

	server := &FilerServer{
		filer:          newRenameTestFiler(t, store),
		option:         &FilerOption{},
		entryLockTable: util.NewLockTable[util.FullPath](),
	}
	addTtlRule(t, server.filer)

	if _, err := server.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: ttlRulePrefix + "obj",
		Mutations: []*filer_pb.ObjectMutation{{
			Type:      filer_pb.ObjectMutation_PUT,
			Directory: "/buckets/ttl",
			Entry: &filer_pb.Entry{
				Name:       "obj",
				Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
			},
		}},
	}); err != nil {
		t.Fatalf("ObjectTransaction: %v", err)
	}

	entry, err := store.FindEntry(context.Background(), ttlRulePrefix+"obj")
	if err != nil {
		t.Fatalf("FindEntry: %v", err)
	}
	if entry.TtlSec != 180 {
		t.Errorf("entry TtlSec = %d, want 180", entry.TtlSec)
	}
}

// A copy landing in a TTL path re-uploads its chunks under that path's rule, so
// the entry has to carry the rule's TTL too - whatever TTL the source had.
func TestCopyAppliesRuleTtl(t *testing.T) {
	for _, sourceTtlSec := range []int32{0, 600} {
		t.Run(fmt.Sprintf("source ttl %d", sourceTtlSec), func(t *testing.T) {
			store := newRenameTestStore()
			source := newFileEntry("/src.txt", 11)
			source.Content = []byte("hello")
			source.TtlSec = sourceTtlSec
			source.Crtime = time.Now() // a TTL entry older than its TTL reads back as expired
			store.entries["/src.txt"] = source
			store.entries[ttlRulePrefix] = newDirectoryEntry(ttlRulePrefix, 10)

			server := &FilerServer{
				filer:          newRenameTestFiler(t, store),
				option:         &FilerOption{},
				entryLockTable: util.NewLockTable[util.FullPath](),
			}
			addTtlRule(t, server.filer)

			req := httptest.NewRequest(http.MethodPost, ttlRulePrefix+"dst.txt?cp.from=/src.txt", http.NoBody)
			rec := httptest.NewRecorder()
			server.PostHandler(rec, req, 0)
			if rec.Code != http.StatusNoContent {
				t.Fatalf("copy = %d, want %d; body=%q", rec.Code, http.StatusNoContent, rec.Body.String())
			}

			entry, err := store.FindEntry(context.Background(), ttlRulePrefix+"dst.txt")
			if err != nil {
				t.Fatalf("FindEntry: %v", err)
			}
			if entry.TtlSec != 180 {
				t.Errorf("entry TtlSec = %d, want 180", entry.TtlSec)
			}
		})
	}
}

// A remote-backed entry copied into a TTL path must not expire locally: the
// remote storage owns its lifecycle, so a local expiry would drop the pointer
// to an object that is still there.
func TestCopyKeepsRemoteEntryUnexpiring(t *testing.T) {
	store := newRenameTestStore()
	source := newFileEntry("/src.txt", 11)
	source.Remote = &filer_pb.RemoteEntry{StorageName: "s3-remote", RemoteSize: 5}
	store.entries["/src.txt"] = source
	store.entries[ttlRulePrefix] = newDirectoryEntry(ttlRulePrefix, 10)

	server := &FilerServer{
		filer:          newRenameTestFiler(t, store),
		option:         &FilerOption{},
		entryLockTable: util.NewLockTable[util.FullPath](),
	}
	addTtlRule(t, server.filer)

	req := httptest.NewRequest(http.MethodPost, ttlRulePrefix+"dst.txt?cp.from=/src.txt", http.NoBody)
	rec := httptest.NewRecorder()
	server.PostHandler(rec, req, 0)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("copy = %d, want %d; body=%q", rec.Code, http.StatusNoContent, rec.Body.String())
	}

	entry, err := store.FindEntry(context.Background(), ttlRulePrefix+"dst.txt")
	if err != nil {
		t.Fatalf("FindEntry: %v", err)
	}
	if entry.TtlSec != 0 {
		t.Errorf("remote entry TtlSec = %d, want 0", entry.TtlSec)
	}
}

// A completed TUS upload uploads its chunks under the target path's rule, so the
// entry it lands has to expire with them.
func TestCompleteTusUploadAppliesRuleTtl(t *testing.T) {
	fs, store := newTusTestServer(t, nil)
	addTtlRule(t, fs.filer)

	targetPath := ttlRulePrefix + "upload.bin"
	seedTusSession(t, fs, store, TusSession{ID: tusTestUploadID, TargetPath: targetPath, Size: 8})
	seedTusChunk(t, fs, store, tusTestUploadID, 0, 8, "3,01637037d6")

	req := tusRequest(http.MethodPatch, "/.tus/.uploads/"+tusTestUploadID, map[string]string{
		"Authorization": "Bearer " + signFilerToken(t, tusTestWriteKey, nil, nil),
		"Content-Type":  "application/offset+octet-stream",
		"Upload-Offset": "8",
	}, "")
	rec := httptest.NewRecorder()
	fs.tusHandler(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("PATCH = %d, want %d; body=%q", rec.Code, http.StatusNoContent, rec.Body.String())
	}

	entry, err := store.FindEntry(context.Background(), util.FullPath(targetPath))
	if err != nil {
		t.Fatalf("FindEntry: %v", err)
	}
	if entry.TtlSec != 180 {
		t.Errorf("entry TtlSec = %d, want 180", entry.TtlSec)
	}
}
