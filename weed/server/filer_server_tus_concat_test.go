package weed_server

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	tusTestPartialAID = "11111111-1111-1111-1111-111111111111"
	tusTestPartialBID = "22222222-2222-2222-2222-222222222222"
)

// seedTusChunk writes a chunk marker entry into a session directory.
func seedTusChunk(t *testing.T, fs *FilerServer, store *renameTestStore, uploadID string, offset, size int64, fileId string) {
	t.Helper()
	entry := &filer.Entry{
		FullPath: util.FullPath(fs.tusChunkPath(uploadID, offset, size, fileId)),
		Attr:     filer.Attr{Crtime: time.Unix(1700000000, 0)},
	}
	if err := store.InsertEntry(context.Background(), entry); err != nil {
		t.Fatalf("seed chunk for %s: %v", uploadID, err)
	}
}

func tusRequest(method, path string, headers map[string]string, body string) *http.Request {
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, http.NoBody)
	} else {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	req.Header.Set("Tus-Resumable", TusVersion)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req
}

// TestFilerServer_tusPatchHandler_PartialSkipsCompletion verifies a completed
// partial upload keeps its session and does not land at the target path, while
// the same flow without Upload-Concat completes normally.
func TestFilerServer_tusPatchHandler_PartialSkipsCompletion(t *testing.T) {
	tests := []struct {
		name        string
		concat      string
		expectEntry bool
	}{
		{"plain upload completes", "", true},
		{"partial upload retained", TusConcatPartial, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, store := newTusTestServer(t, nil)
			targetPath := "/buckets/data/file.bin"
			seedTusSession(t, fs, store, TusSession{ID: tusTestUploadID, TargetPath: targetPath, Size: 0, Concat: tt.concat})

			req := tusRequest(http.MethodPatch, "/.tus/.uploads/"+tusTestUploadID, map[string]string{
				"Authorization": "Bearer " + signFilerToken(t, tusTestWriteKey, nil, nil),
				"Content-Type":  "application/offset+octet-stream",
				"Upload-Offset": "0",
			}, "")
			rec := httptest.NewRecorder()
			fs.tusHandler(rec, req)

			if rec.Code != http.StatusNoContent {
				t.Fatalf("PATCH = %d, want %d; body=%q", rec.Code, http.StatusNoContent, rec.Body.String())
			}
			_, entryErr := store.FindEntry(context.Background(), util.FullPath(targetPath))
			_, sessionErr := store.FindEntry(context.Background(), util.FullPath(fs.tusSessionInfoPath(tusTestUploadID)))
			if tt.expectEntry && (entryErr != nil || sessionErr == nil) {
				t.Fatalf("plain upload: entry err=%v, session err=%v; want entry present and session removed", entryErr, sessionErr)
			}
			if !tt.expectEntry && (entryErr == nil || sessionErr != nil) {
				t.Fatalf("partial upload: entry err=%v, session err=%v; want no entry and session retained", entryErr, sessionErr)
			}
		})
	}
}

// TestFilerServer_tusHeadHandler_EchoesUploadConcat verifies HEAD reports the
// stored Upload-Concat value for partial sessions and omits it otherwise.
func TestFilerServer_tusHeadHandler_EchoesUploadConcat(t *testing.T) {
	fs, store := newTusTestServer(t, map[string]string{tusTestUploadID: "/buckets/data/plain.bin"})
	seedTusSession(t, fs, store, TusSession{ID: tusTestPartialAID, TargetPath: "/buckets/data/part.bin", Size: 1, Concat: TusConcatPartial})

	head := func(uploadID string) *httptest.ResponseRecorder {
		req := tusRequest(http.MethodHead, "/.tus/.uploads/"+uploadID, map[string]string{
			"Authorization": "Bearer " + signFilerToken(t, tusTestReadKey, nil, nil),
		}, "")
		rec := httptest.NewRecorder()
		fs.tusHandler(rec, req)
		return rec
	}

	partialRec := head(tusTestPartialAID)
	if partialRec.Code != http.StatusOK || partialRec.Header().Get("Upload-Concat") != TusConcatPartial {
		t.Fatalf("partial HEAD = %d, Upload-Concat=%q; want 200 with %q", partialRec.Code, partialRec.Header().Get("Upload-Concat"), TusConcatPartial)
	}
	plainRec := head(tusTestUploadID)
	if plainRec.Code != http.StatusOK || plainRec.Header().Get("Upload-Concat") != "" {
		t.Fatalf("plain HEAD = %d, Upload-Concat=%q; want 200 without the header", plainRec.Code, plainRec.Header().Get("Upload-Concat"))
	}
}

// TestFilerServer_tusConcatFinal_AssemblesPartials drives a full concatenation:
// two completed partials, referenced as a path and as an absolute URL, land as
// one entry with re-based chunks, and the consumed sessions are removed.
func TestFilerServer_tusConcatFinal_AssemblesPartials(t *testing.T) {
	fs, store := newTusTestServer(t, nil)
	targetPath := "/buckets/data/final.bin"
	fidA, fidB := "3,01637037d6", "4,02637037d6"
	seedTusSession(t, fs, store, TusSession{ID: tusTestPartialAID, TargetPath: targetPath, Size: 8, Concat: TusConcatPartial})
	seedTusChunk(t, fs, store, tusTestPartialAID, 0, 8, fidA)
	seedTusSession(t, fs, store, TusSession{ID: tusTestPartialBID, TargetPath: targetPath, Size: 4, Concat: TusConcatPartial})
	seedTusChunk(t, fs, store, tusTestPartialBID, 0, 4, fidB)

	req := tusRequest(http.MethodPost, "/.tus"+targetPath, map[string]string{
		"Authorization":   "Bearer " + signFilerToken(t, tusTestWriteKey, nil, nil),
		"Upload-Concat":   "final;/.tus/.uploads/" + tusTestPartialAID + " http://example.com/.tus/.uploads/" + tusTestPartialBID,
		"Upload-Metadata": "content-type " + base64.StdEncoding.EncodeToString([]byte("text/plain")),
	}, "")
	rec := httptest.NewRecorder()
	fs.tusHandler(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("final POST = %d, want %d; body=%q", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if location := rec.Header().Get("Location"); !strings.HasPrefix(location, "/.tus/.uploads/") {
		t.Fatalf("Location = %q, want an upload URL", location)
	}

	entry, err := store.FindEntry(context.Background(), util.FullPath(targetPath))
	if err != nil {
		t.Fatalf("final entry not created: %v", err)
	}
	if entry.Mime != "text/plain" {
		t.Errorf("entry mime = %q, want %q", entry.Mime, "text/plain")
	}
	chunks := entry.GetChunks()
	if len(chunks) != 2 {
		t.Fatalf("entry chunks = %d, want 2", len(chunks))
	}
	if chunks[0].FileId != fidA || chunks[0].Offset != 0 || chunks[0].Size != 8 {
		t.Errorf("chunk[0] = %s@%d+%d, want %s@0+8", chunks[0].FileId, chunks[0].Offset, chunks[0].Size, fidA)
	}
	if chunks[1].FileId != fidB || chunks[1].Offset != 8 || chunks[1].Size != 4 {
		t.Errorf("chunk[1] = %s@%d+%d, want %s@8+4", chunks[1].FileId, chunks[1].Offset, chunks[1].Size, fidB)
	}

	for _, uploadID := range []string{tusTestPartialAID, tusTestPartialBID} {
		if _, err := store.FindEntry(context.Background(), util.FullPath(fs.tusSessionInfoPath(uploadID))); err == nil {
			t.Errorf("partial session %s still present after concatenation", uploadID)
		}
	}
}

// TestFilerServer_tusConcatFinal_Validation covers the final upload request
// guards: header misuse, unusable references, unfinished or foreign partials.
func TestFilerServer_tusConcatFinal_Validation(t *testing.T) {
	uploadsRef := func(uploadID string) string { return "/.tus/.uploads/" + uploadID }
	completePartial := func(fs *FilerServer, store *renameTestStore, t *testing.T, uploadID, targetPath string, size int64) {
		seedTusSession(t, fs, store, TusSession{ID: uploadID, TargetPath: targetPath, Size: size, Concat: TusConcatPartial})
		seedTusChunk(t, fs, store, uploadID, 0, size, "3,01637037d6")
	}

	tests := []struct {
		name         string
		seed         func(t *testing.T, fs *FilerServer, store *renameTestStore)
		headers      map[string]string
		body         string
		prefixes     []string
		expectStatus int
	}{
		{
			name:         "upload length rejected",
			headers:      map[string]string{"Upload-Concat": "final;" + uploadsRef(tusTestPartialAID), "Upload-Length": "12"},
			expectStatus: http.StatusBadRequest,
		},
		{
			name:         "body rejected",
			headers:      map[string]string{"Upload-Concat": "final;" + uploadsRef(tusTestPartialAID)},
			body:         "x",
			expectStatus: http.StatusForbidden,
		},
		{
			name:         "reference outside uploads prefix",
			headers:      map[string]string{"Upload-Concat": "final;/elsewhere/" + tusTestPartialAID},
			expectStatus: http.StatusBadRequest,
		},
		{
			name:         "duplicate reference",
			headers:      map[string]string{"Upload-Concat": "final;" + uploadsRef(tusTestPartialAID) + " " + uploadsRef(tusTestPartialAID)},
			expectStatus: http.StatusBadRequest,
		},
		{
			name:         "empty reference list",
			headers:      map[string]string{"Upload-Concat": "final;"},
			expectStatus: http.StatusBadRequest,
		},
		{
			name:         "missing partial",
			headers:      map[string]string{"Upload-Concat": "final;" + uploadsRef(tusTestPartialAID)},
			expectStatus: http.StatusNotFound,
		},
		{
			name: "non-partial session rejected",
			seed: func(t *testing.T, fs *FilerServer, store *renameTestStore) {
				seedTusSession(t, fs, store, TusSession{ID: tusTestPartialAID, TargetPath: "/buckets/data/a.bin", Size: 1})
			},
			headers:      map[string]string{"Upload-Concat": "final;" + uploadsRef(tusTestPartialAID)},
			expectStatus: http.StatusBadRequest,
		},
		{
			name: "unfinished partial rejected",
			seed: func(t *testing.T, fs *FilerServer, store *renameTestStore) {
				seedTusSession(t, fs, store, TusSession{ID: tusTestPartialAID, TargetPath: "/buckets/data/a.bin", Size: 4, Concat: TusConcatPartial})
			},
			headers:      map[string]string{"Upload-Concat": "final;" + uploadsRef(tusTestPartialAID)},
			expectStatus: http.StatusBadRequest,
		},
		{
			name: "cross-prefix partial denied",
			seed: func(t *testing.T, fs *FilerServer, store *renameTestStore) {
				completePartial(fs, store, t, tusTestPartialAID, "/buckets/secret/victim.bin", 4)
			},
			headers:      map[string]string{"Upload-Concat": "final;" + uploadsRef(tusTestPartialAID)},
			prefixes:     []string{"/buckets/data"},
			expectStatus: http.StatusUnauthorized,
		},
		{
			name: "combined size over maximum",
			seed: func(t *testing.T, fs *FilerServer, store *renameTestStore) {
				completePartial(fs, store, t, tusTestPartialAID, "/buckets/data/a.bin", TusDefaultMaxSize)
				seedTusSession(t, fs, store, TusSession{ID: tusTestPartialBID, TargetPath: "/buckets/data/b.bin", Size: 1, Concat: TusConcatPartial})
			},
			headers:      map[string]string{"Upload-Concat": "final;" + uploadsRef(tusTestPartialAID) + " " + uploadsRef(tusTestPartialBID)},
			expectStatus: http.StatusRequestEntityTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, store := newTusTestServer(t, nil)
			if tt.seed != nil {
				tt.seed(t, fs, store)
			}
			headers := map[string]string{
				"Authorization": "Bearer " + signFilerToken(t, tusTestWriteKey, tt.prefixes, nil),
			}
			for k, v := range tt.headers {
				headers[k] = v
			}
			req := tusRequest(http.MethodPost, "/.tus/buckets/data/final.bin", headers, tt.body)
			rec := httptest.NewRecorder()
			fs.tusHandler(rec, req)

			if rec.Code != tt.expectStatus {
				t.Fatalf("final POST = %d, want %d; body=%q", rec.Code, tt.expectStatus, rec.Body.String())
			}
			if _, err := store.FindEntry(context.Background(), util.FullPath("/buckets/data/final.bin")); err == nil {
				t.Fatal("rejected concatenation still created the target entry")
			}
		})
	}
}

// TestFilerServer_tusConcatFinal_ClaimedPartialRejected verifies a partial
// already claimed by a concurrent final cannot be consumed again, and that the
// foreign claim is left in place.
func TestFilerServer_tusConcatFinal_ClaimedPartialRejected(t *testing.T) {
	fs, store := newTusTestServer(t, nil)
	seedTusSession(t, fs, store, TusSession{ID: tusTestPartialAID, TargetPath: "/buckets/data/a.bin", Size: 4, Concat: TusConcatPartial})
	seedTusChunk(t, fs, store, tusTestPartialAID, 0, 4, "3,01637037d6")
	markerPath := util.FullPath(fs.tusSessionConsumedPath(tusTestPartialAID))
	if err := store.InsertEntry(context.Background(), &filer.Entry{FullPath: markerPath}); err != nil {
		t.Fatalf("seed consumed marker: %v", err)
	}

	req := tusRequest(http.MethodPost, "/.tus/buckets/data/final.bin", map[string]string{
		"Authorization": "Bearer " + signFilerToken(t, tusTestWriteKey, nil, nil),
		"Upload-Concat": "final;/.tus/.uploads/" + tusTestPartialAID,
	}, "")
	rec := httptest.NewRecorder()
	fs.tusHandler(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("final POST = %d, want %d; body=%q", rec.Code, http.StatusConflict, rec.Body.String())
	}
	if _, err := store.FindEntry(context.Background(), markerPath); err != nil {
		t.Fatal("foreign claim was released by the losing request")
	}
}

// TestFilerServer_tusDeleteHandler_ConsumedSessionKeepsChunks verifies deleting
// a consumed session removes its metadata without freeing its chunks. The test
// filer has no chunk deletion queue, so an attempt to free chunks would panic.
func TestFilerServer_tusDeleteHandler_ConsumedSessionKeepsChunks(t *testing.T) {
	fs, store := newTusTestServer(t, nil)
	seedTusSession(t, fs, store, TusSession{ID: tusTestPartialAID, TargetPath: "/buckets/data/a.bin", Size: 4, Concat: TusConcatPartial})
	seedTusChunk(t, fs, store, tusTestPartialAID, 0, 4, "3,01637037d6")
	if err := store.InsertEntry(context.Background(), &filer.Entry{FullPath: util.FullPath(fs.tusSessionConsumedPath(tusTestPartialAID))}); err != nil {
		t.Fatalf("seed consumed marker: %v", err)
	}

	req := tusRequest(http.MethodDelete, "/.tus/.uploads/"+tusTestPartialAID, map[string]string{
		"Authorization": "Bearer " + signFilerToken(t, tusTestWriteKey, nil, nil),
	}, "")
	rec := httptest.NewRecorder()
	fs.tusHandler(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE = %d, want %d; body=%q", rec.Code, http.StatusNoContent, rec.Body.String())
	}
	if _, err := store.FindEntry(context.Background(), util.FullPath(fs.tusSessionInfoPath(tusTestPartialAID))); err == nil {
		t.Fatal("consumed session metadata still present after DELETE")
	}
}

// TestFilerServer_tusPatchHandler_FinalRejected verifies PATCH against a final
// upload URL is refused, per the concatenation extension.
func TestFilerServer_tusPatchHandler_FinalRejected(t *testing.T) {
	fs, store := newTusTestServer(t, nil)
	seedTusSession(t, fs, store, TusSession{ID: tusTestUploadID, TargetPath: "/buckets/data/final.bin", Size: 4, Concat: "final;/.tus/.uploads/" + tusTestPartialAID})

	req := tusRequest(http.MethodPatch, "/.tus/.uploads/"+tusTestUploadID, map[string]string{
		"Authorization": "Bearer " + signFilerToken(t, tusTestWriteKey, nil, nil),
		"Content-Type":  "application/offset+octet-stream",
		"Upload-Offset": "0",
	}, "")
	rec := httptest.NewRecorder()
	fs.tusHandler(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("PATCH final = %d, want %d; body=%q", rec.Code, http.StatusForbidden, rec.Body.String())
	}
}
