package weed_server

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func buildTestZip(t *testing.T, members map[string]string, dirs []string, unsafe []string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, dir := range dirs {
		h := &zip.FileHeader{Name: dir, Method: zip.Store, Modified: time.Unix(1700000000, 0)}
		h.SetMode(0755 | 1<<31)
		if _, err := zw.CreateHeader(h); err != nil {
			t.Fatalf("create dir %s: %v", dir, err)
		}
	}
	write := func(name, content string) {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		if _, err := w.Write([]byte(content)); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	for name, content := range members {
		write(name, content)
	}
	for _, name := range unsafe {
		write(name, "evil")
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip: %v", err)
	}
	return buf.Bytes()
}

func newExtractTestServer(t *testing.T) (*FilerServer, *filer.Filer) {
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)
	return &FilerServer{
		filer: f,
		option: &FilerOption{
			MaxMB:            1,
			SaveToFilerLimit: 1 << 20,
		},
	}, f
}

func TestExtractZipArchivePut(t *testing.T) {
	fs, f := newExtractTestServer(t)
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	dirHeader := &zip.FileHeader{Name: "empty/", Method: zip.Store, Modified: time.Unix(1700000000, 0)}
	dirHeader.SetMode(0755 | 1<<31)
	if _, err := zw.CreateHeader(dirHeader); err != nil {
		t.Fatal(err)
	}
	fileHeader := &zip.FileHeader{Name: "a/b.txt", Modified: time.Unix(1600000000, 0)}
	fileHeader.SetMode(0600)
	fw, err := zw.CreateHeader(fileHeader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fw.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	pngW, err := zw.Create("c.png")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pngW.Write([]byte("\x89png")); err != nil {
		t.Fatal(err)
	}
	evil, err := zw.Create("../evil.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := evil.Write([]byte("evil")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	zipBytes := buf.Bytes()

	r := httptest.NewRequest(http.MethodPut, "/dst/archive.zip?extract=zip", bytes.NewReader(zipBytes))
	w := httptest.NewRecorder()
	fs.PostHandler(w, r, int64(len(zipBytes)))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201, body: %s", w.Code, w.Body.String())
	}
	var result FilerExtractResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if len(result.Extracted) != 2 {
		t.Fatalf("extracted %d members, want 2: %+v", len(result.Extracted), result)
	}
	if len(result.Errors) != 1 {
		t.Fatalf("errors = %v, want 1 unsafe-name entry", result.Errors)
	}

	entry, err := f.FindEntry(r.Context(), util.FullPath("/dst/a/b.txt"))
	if err != nil {
		t.Fatalf("find extracted member: %v", err)
	}
	if string(entry.Content) != "hello" {
		t.Fatalf("member content = %q", entry.Content)
	}
	if entry.Mtime.Unix() != 1600000000 {
		t.Fatalf("member mtime = %v, want %v", entry.Mtime, time.Unix(1600000000, 0))
	}
	if entry.Mode.Perm() != 0600 {
		t.Fatalf("member mode = %v, want 0600", entry.Mode.Perm())
	}

	dirEntry, err := f.FindEntry(r.Context(), util.FullPath("/dst/empty"))
	if err != nil || !dirEntry.IsDirectory() {
		t.Fatalf("dir member missing: %v %+v", err, dirEntry)
	}
	if dirEntry.Mtime.Unix() != 1700000000 {
		t.Fatalf("dir mtime = %v, want %v", dirEntry.Mtime, time.Unix(1700000000, 0))
	}
	if _, err := f.FindEntry(r.Context(), util.FullPath("/dst/archive.zip")); err == nil {
		t.Fatal("archive should not be stored without extract.keep")
	}
	if _, err := f.FindEntry(r.Context(), util.FullPath("/dst/../evil.txt")); err == nil {
		t.Fatal("unsafe member should not be extracted")
	}
	if _, err := f.FindEntry(r.Context(), util.FullPath("/evil.txt")); err == nil {
		t.Fatal("zip-slip member escaped the extraction root")
	}
}

func TestExtractZipArchiveKeep(t *testing.T) {
	fs, f := newExtractTestServer(t)
	zipBytes := buildTestZip(t, map[string]string{"x.txt": "data"}, nil, nil)

	r := httptest.NewRequest(http.MethodPut, "/dst/pack.zip?extract=zip&extract.keep=true", bytes.NewReader(zipBytes))
	w := httptest.NewRecorder()
	fs.PostHandler(w, r, int64(len(zipBytes)))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, body: %s", w.Code, w.Body.String())
	}
	archive, err := f.FindEntry(r.Context(), util.FullPath("/dst/pack.zip"))
	if err != nil {
		t.Fatalf("archive not stored with extract.keep: %v", err)
	}
	if int64(len(archive.Content)) != int64(len(zipBytes)) {
		t.Fatalf("archive size = %d, want %d", len(archive.Content), len(zipBytes))
	}
	if _, err := f.FindEntry(r.Context(), util.FullPath("/dst/x.txt")); err != nil {
		t.Fatalf("member missing: %v", err)
	}
}

func TestExtractZipArchiveKeepSkipsParentName(t *testing.T) {
	fs, f := newExtractTestServer(t)
	zipBytes := buildTestZip(t, map[string]string{"x.txt": "data"}, nil, nil)

	var body bytes.Buffer
	mw := multipartWriter(t, &body, "..", zipBytes)
	r := httptest.NewRequest(http.MethodPost, "/a/b/?extract=zip&extract.keep=true", &body)
	r.Header.Set("Content-Type", mw)
	w := httptest.NewRecorder()
	fs.PostHandler(w, r, int64(body.Len()))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, body: %s", w.Code, w.Body.String())
	}
	if _, err := f.FindEntry(r.Context(), util.FullPath("/a/b/..")); err == nil {
		t.Fatal("archive kept under a parent-directory name")
	}
	if _, err := f.FindEntry(r.Context(), util.FullPath("/a/b/x.txt")); err != nil {
		t.Fatalf("member missing: %v", err)
	}
}

func TestExtractZipArchiveNotAZip(t *testing.T) {
	fs, _ := newExtractTestServer(t)
	r := httptest.NewRequest(http.MethodPut, "/dst/notzip.bin?extract=zip", bytes.NewReader([]byte("not a zip")))
	w := httptest.NewRecorder()
	fs.PostHandler(w, r, 9)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// A member larger than chunkSize must not take the inline fast path when
// SaveToFilerLimit exceeds chunkSize — the first chunk is not the whole member.
func TestExtractZipArchiveMemberLargerThanChunk(t *testing.T) {
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)
	fs, _ := newInlineAppendUploadServer(t, f, 4<<20, 1) // inline limit exceeds the 1 MiB chunkSize
	member := bytes.Repeat([]byte("z"), (1<<20)+1)
	zipBytes := buildTestZip(t, map[string]string{"big.bin": string(member)}, nil, nil)

	r := httptest.NewRequest(http.MethodPut, "/dst/pack.zip?extract=zip", bytes.NewReader(zipBytes))
	w := httptest.NewRecorder()
	fs.PostHandler(w, r, int64(len(zipBytes)))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, body: %s", w.Code, w.Body.String())
	}
	var result FilerExtractResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if len(result.Errors) != 0 {
		t.Fatalf("member errors: %v", result.Errors)
	}
	entry, err := f.FindEntry(r.Context(), util.FullPath("/dst/big.bin"))
	if err != nil {
		t.Fatalf("member missing: %v", err)
	}
	if int64(entry.Attr.FileSize) != int64(len(member)) {
		t.Fatalf("member stored truncated: %d bytes, want %d", entry.Attr.FileSize, len(member))
	}
}

func TestExtractZipArchiveRawPost(t *testing.T) {
	fs, f := newExtractTestServer(t)
	zipBytes := buildTestZip(t, map[string]string{"raw.txt": "raw"}, nil, nil)

	r := httptest.NewRequest(http.MethodPost, "/dst/?extract=zip", bytes.NewReader(zipBytes))
	r.Header.Set("Content-Type", "application/zip")
	w := httptest.NewRecorder()
	fs.PostHandler(w, r, int64(len(zipBytes)))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, body: %s", w.Code, w.Body.String())
	}
	entry, err := f.FindEntry(r.Context(), util.FullPath("/dst/raw.txt"))
	if err != nil {
		t.Fatalf("member missing: %v", err)
	}
	if string(entry.Content) != "raw" {
		t.Fatalf("member content = %q", entry.Content)
	}
}

func TestExtractZipArchiveMultipartPost(t *testing.T) {
	fs, f := newExtractTestServer(t)
	zipBytes := buildTestZip(t, map[string]string{"img/1.png": "png-bytes"}, nil, nil)

	var body bytes.Buffer
	mw := multipartWriter(t, &body, "bundle.zip", zipBytes)
	r := httptest.NewRequest(http.MethodPost, "/assets/?extract=zip", &body)
	r.Header.Set("Content-Type", mw)
	w := httptest.NewRecorder()
	fs.PostHandler(w, r, int64(body.Len()))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, body: %s", w.Code, w.Body.String())
	}
	entry, err := f.FindEntry(r.Context(), util.FullPath("/assets/img/1.png"))
	if err != nil {
		t.Fatalf("member missing: %v", err)
	}
	if string(entry.Content) != "png-bytes" {
		t.Fatalf("member content = %q", entry.Content)
	}
	if _, err := f.FindEntry(r.Context(), util.FullPath("/assets/bundle.zip")); err == nil {
		t.Fatal("archive should not be stored")
	}
}

func multipartWriter(t *testing.T, buf *bytes.Buffer, fileName string, data []byte) string {
	t.Helper()
	mw := multipart.NewWriter(buf)
	part, err := mw.CreateFormFile("file", fileName)
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err := part.Write(data); err != nil {
		t.Fatalf("write part: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart: %v", err)
	}
	return mw.FormDataContentType()
}
