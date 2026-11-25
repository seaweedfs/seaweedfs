package weed_server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/operation"
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

// The native LMCache path allocates chunks and publishes the Filer Entry in
// separate RPCs. Both RPCs must resolve the same storage rule, or metadata can
// expire while its Volume remains permanent (or the reverse).
func TestNativeCacheWriteUsesOneTtlForAllocationAndEntry(t *testing.T) {
	store := newRenameTestStore()
	store.entries[ttlRulePrefix] = newDirectoryEntry(ttlRulePrefix, 10)
	server := &FilerServer{
		filer:          newRenameTestFiler(t, store),
		option:         &FilerOption{},
		entryLockTable: util.NewLockTable[util.FullPath](),
	}
	addTtlRule(t, server.filer)

	allocation, err := server.resolveAssignStorageOption(context.Background(), &filer_pb.AssignVolumeRequest{
		Path:       ttlRulePrefix + "cache-key",
		Collection: "lmcache",
	})
	if err != nil {
		t.Fatalf("resolveAssignStorageOption: %v", err)
	}
	if allocation.Collection != "lmcache" {
		t.Fatalf("allocation collection = %q, want lmcache", allocation.Collection)
	}
	if allocation.TtlSeconds != 180 {
		t.Fatalf("allocation TTL = %d, want 180", allocation.TtlSeconds)
	}

	if _, err := server.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
		Directory: "/buckets/ttl",
		Entry: &filer_pb.Entry{
			Name:       "cache-key",
			Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
		},
	}); err != nil {
		t.Fatalf("CreateEntry: %v", err)
	}

	entry, err := store.FindEntry(context.Background(), ttlRulePrefix+"cache-key")
	if err != nil {
		t.Fatalf("FindEntry: %v", err)
	}
	if entry.TtlSec != allocation.TtlSeconds {
		t.Fatalf("entry TTL = %d, allocation TTL = %d", entry.TtlSec, allocation.TtlSeconds)
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

// Clients percent-encode non-ASCII path segments on the wire, so a rule has to
// be matched against the decoded path the entry is written to, not the raw
// request-target: a read-only rule on "/data/只读/" must still refuse a POST to
// "/data/%E5%8F%AA%E8%AF%BB/".
func TestPostHandlerMatchesStorageRuleOnDecodedPath(t *testing.T) {
	const readOnlyPrefix = "/data/只读/"
	store := newRenameTestStore()
	source := newFileEntry("/src.txt", 11)
	source.Content = []byte("hello")
	store.entries["/src.txt"] = source
	store.entries[readOnlyPrefix] = newDirectoryEntry(readOnlyPrefix, 10)

	server := &FilerServer{
		filer:          newRenameTestFiler(t, store),
		option:         &FilerOption{},
		entryLockTable: util.NewLockTable[util.FullPath](),
	}
	if err := server.filer.FilerConf.AddLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: readOnlyPrefix,
		ReadOnly:       true,
	}); err != nil {
		t.Fatalf("AddLocationConf: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/data/%E5%8F%AA%E8%AF%BB/dst.txt?cp.from=/src.txt", http.NoBody)
	rec := httptest.NewRecorder()
	server.PostHandler(rec, req, 0)
	if rec.Code != http.StatusInsufficientStorage {
		t.Fatalf("copy into read-only path = %d, want %d; body=%q", rec.Code, http.StatusInsufficientStorage, rec.Body.String())
	}
	if _, err := store.FindEntry(context.Background(), readOnlyPrefix+"dst.txt"); err == nil {
		t.Fatal("copy landed in the read-only path")
	}
}

// A write to an existing directory without a trailing slash lands on a child
// of it, so the read-only rule has to be matched on that child path.
func TestPostHandlerChecksReadOnlyOnFinalPath(t *testing.T) {
	const readOnlyPrefix = "/buckets/demo/"
	multipartBody := func(t *testing.T) (*bytes.Buffer, string) {
		body := &bytes.Buffer{}
		mw := multipart.NewWriter(body)
		part, err := mw.CreateFormFile("file", "marker.txt")
		if err != nil {
			t.Fatalf("CreateFormFile: %v", err)
		}
		part.Write([]byte("hello"))
		mw.Close()
		return body, mw.FormDataContentType()
	}
	tests := []struct {
		name    string
		request func(t *testing.T) *http.Request
		child   string
	}{
		{
			name: "multipart POST",
			request: func(t *testing.T) *http.Request {
				body, contentType := multipartBody(t)
				req := httptest.NewRequest(http.MethodPost, "/buckets/demo", body)
				req.Header.Set("Content-Type", contentType)
				return req
			},
			child: readOnlyPrefix + "marker.txt",
		},
		{
			name: "PUT",
			request: func(t *testing.T) *http.Request {
				return httptest.NewRequest(http.MethodPut, "/buckets/demo", strings.NewReader("hello"))
			},
			child: readOnlyPrefix + "demo",
		},
		{
			name: "copy",
			request: func(t *testing.T) *http.Request {
				return httptest.NewRequest(http.MethodPost, "/buckets/demo?cp.from=/src.txt", http.NoBody)
			},
			child: readOnlyPrefix + "src.txt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newRenameTestStore()
			source := newFileEntry("/src.txt", 11)
			source.Content = []byte("hello")
			store.entries["/src.txt"] = source
			store.entries["/buckets/demo"] = newDirectoryEntry("/buckets/demo", 10)

			server := &FilerServer{
				filer:          newRenameTestFiler(t, store),
				option:         &FilerOption{SaveToFilerLimit: 1024},
				entryLockTable: util.NewLockTable[util.FullPath](),
			}
			if err := server.filer.FilerConf.AddLocationConf(&filer_pb.FilerConf_PathConf{
				LocationPrefix: readOnlyPrefix,
				ReadOnly:       true,
			}); err != nil {
				t.Fatalf("AddLocationConf: %v", err)
			}

			rec := httptest.NewRecorder()
			server.PostHandler(rec, tt.request(t), 0)
			if rec.Code != http.StatusInsufficientStorage {
				t.Errorf("status = %d, want %d; body=%q", rec.Code, http.StatusInsufficientStorage, rec.Body.String())
			}
			if _, err := store.FindEntry(context.Background(), util.FullPath(tt.child)); err == nil {
				t.Errorf("%s landed in the read-only path", tt.child)
			}
		})
	}
}

// saveMetaData resolves the path it persists on its own, so it has to refuse a
// read-only path even when the earlier check saw a different one.
func TestSaveMetaDataChecksReadOnlyOnPersistedPath(t *testing.T) {
	const readOnlyPrefix = "/buckets/demo/"
	store := newRenameTestStore()
	store.entries["/buckets/demo"] = newDirectoryEntry("/buckets/demo", 10)
	server := &FilerServer{
		filer:          newRenameTestFiler(t, store),
		option:         &FilerOption{},
		entryLockTable: util.NewLockTable[util.FullPath](),
	}
	if err := server.filer.FilerConf.AddLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: readOnlyPrefix,
		ReadOnly:       true,
	}); err != nil {
		t.Fatalf("AddLocationConf: %v", err)
	}

	chunks := []*filer_pb.FileChunk{{FileId: "3,01637037d6", Size: 5}}
	req := httptest.NewRequest(http.MethodPost, "/buckets/demo", http.NoBody)
	_, err, uncommitted := server.saveMetaData(context.Background(), req, "marker.txt", "", &operation.StorageOption{}, 0, nil, chunks, 5, nil)
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("saveMetaData err = %v, want ErrReadOnly", err)
	}
	if len(uncommitted) != 1 {
		t.Errorf("uncommitted chunks = %d, want the uploaded chunk back for cleanup", len(uncommitted))
	}
	if _, err := store.FindEntry(context.Background(), readOnlyPrefix+"marker.txt"); err == nil {
		t.Error("entry landed in the read-only path")
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
