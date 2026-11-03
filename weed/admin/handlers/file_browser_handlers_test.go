package handlers

import (
	"bytes"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/filer"
)

func TestValidateAndCleanFilePath_AllowsControlChars(t *testing.T) {
	h := &FileBrowserHandlers{}

	// S3 object keys may legally contain any UTF-8 bytes, including control
	// characters like \n, \r, and \x00. The admin UI must be able to browse
	// and manage such entries rather than silently stripping or rejecting them.
	cases := []struct {
		in   string
		want string
	}{
		{"/buckets/profilebuilder/3testGB.zip\n ", "/buckets/profilebuilder/3testGB.zip\n "},
		{"/foo\rbar", "/foo\rbar"},
		{"/foo\x00bar", "/foo\x00bar"},
		{"/normal/path.txt", "/normal/path.txt"},
		// Missing leading slash should be added back.
		{"relative/path.txt", "/relative/path.txt"},
		// Duplicate slashes should be collapsed by path.Clean.
		{"/a//b", "/a/b"},
		// ".." inside a file or directory name is not a parent component.
		{"/buckets/reports/report..csv", "/buckets/reports/report..csv"},
		{"/a../b", "/a../b"},
	}
	for _, tc := range cases {
		got, err := h.validateAndCleanFilePath(tc.in)
		if err != nil {
			t.Errorf("validateAndCleanFilePath(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("validateAndCleanFilePath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestValidateAndCleanFilePath_RejectsEmpty(t *testing.T) {
	h := &FileBrowserHandlers{}
	if _, err := h.validateAndCleanFilePath(""); err == nil {
		t.Errorf("expected empty path rejection")
	}
}

func TestValidateAndCleanFilePath_RejectsParentComponents(t *testing.T) {
	h := &FileBrowserHandlers{}
	for _, p := range []string{"../etc", "../../etc/iam", "..", "a/../../b"} {
		if _, err := h.validateAndCleanFilePath(p); err == nil {
			t.Errorf("validateAndCleanFilePath(%q): expected traversal rejection", p)
		}
	}
}

func TestCleanReadPath_DeniesEtcForReadOnly(t *testing.T) {
	h := &FileBrowserHandlers{}

	denied := []string{"/etc", "/etc/iam", "/etc/iam/identities/root.json",
		"/etc/s3/kek.json", "/etc/../etc/iam/identity.json", "/a/../../etc/x"}
	for _, p := range denied {
		req := httptest.NewRequest(http.MethodGet, "/api/files/download?path="+p, nil)
		req = req.WithContext(dash.WithAuthContext(req.Context(), "ro", dash.RoleReadOnly, "csrf"))
		w := httptest.NewRecorder()

		if _, ok := h.cleanReadPath(w, req); ok {
			t.Errorf("path %q: expected denial for read-only session", p)
			continue
		}
		if w.Code != http.StatusForbidden {
			t.Errorf("path %q: expected 403, got %d", p, w.Code)
		}
	}
}

func TestCleanReadPath_AllowsEtcForAdminAndNorRole(t *testing.T) {
	h := &FileBrowserHandlers{}

	for _, role := range []string{"admin", ""} {
		req := httptest.NewRequest(http.MethodGet, "/api/files/download?path=/etc/iam/identities/root.json", nil)
		req = req.WithContext(dash.WithAuthContext(req.Context(), "u", role, "csrf"))
		w := httptest.NewRecorder()

		if _, ok := h.cleanReadPath(w, req); !ok {
			t.Errorf("role %q: expected /etc path to be allowed, got %d", role, w.Code)
		}
	}
}

func TestCleanReadPath_AllowsReadOnlyOutsideEtc(t *testing.T) {
	h := &FileBrowserHandlers{}

	for _, p := range []string{"/buckets/foo/bar.txt", "/etcx/file", "/etcetera", "/"} {
		req := httptest.NewRequest(http.MethodGet, "/api/files/download?path="+p, nil)
		req = req.WithContext(dash.WithAuthContext(req.Context(), "ro", dash.RoleReadOnly, "csrf"))
		w := httptest.NewRecorder()

		if _, ok := h.cleanReadPath(w, req); !ok {
			t.Errorf("path %q: expected allowed for read-only session, got %d", p, w.Code)
		}
	}
}

func TestExportExcludedPrefixes_ReadOnlyDropsEtc(t *testing.T) {
	etcPrefix := strings.TrimSuffix(filer.DirectoryEtcRoot, "/")

	req := httptest.NewRequest(http.MethodGet, "/api/files/metadata/export?path=/", nil)
	req = req.WithContext(dash.WithAuthContext(req.Context(), "ro", dash.RoleReadOnly, "csrf"))
	if got := exportExcludedPrefixes(req); !slices.Contains(got, etcPrefix) {
		t.Errorf("read-only export should exclude %q, got %v", etcPrefix, got)
	}

	for _, role := range []string{"admin", ""} {
		req := httptest.NewRequest(http.MethodGet, "/api/files/metadata/export?path=/", nil)
		req = req.WithContext(dash.WithAuthContext(req.Context(), "u", role, "csrf"))
		if got := exportExcludedPrefixes(req); slices.Contains(got, etcPrefix) {
			t.Errorf("role %q: export should keep %q, got %v", role, etcPrefix, got)
		}
	}
}

func TestListFolders_RejectsPathOutsideBuckets(t *testing.T) {
	h := &FileBrowserHandlers{adminServer: &dash.AdminServer{}}

	cases := []string{"/etc/seaweedfs", "/", "/notbuckets/foo", "/buckets-not-really/foo",
		"/buckets/../etc", "/buckets/..", "/buckets/foo/../../etc", `/buckets\..\etc`}
	for _, path := range cases {
		req := httptest.NewRequest(http.MethodGet, "/api/files/list-folders?path="+path, nil)
		w := httptest.NewRecorder()

		h.ListFolders(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("path %q: expected 400, got %d (body: %s)", path, w.Code, w.Body.String())
		}
	}
}

func TestUploadFile_RejectsOversizedUpload(t *testing.T) {
	adminServer := &dash.AdminServer{}
	adminServer.SetUploadMaxSize(1 << 20)
	h := &FileBrowserHandlers{adminServer: adminServer}

	// 1.5MB and two 600KB files fit the body allowance but exceed the file total;
	// 3MB trips the body cap.
	for _, sizes := range [][]int{{3 << 19}, {600 << 10, 600 << 10}, {3 << 20}} {
		var body bytes.Buffer
		mw := multipart.NewWriter(&body)
		for i, size := range sizes {
			part, err := mw.CreateFormFile("files", fmt.Sprintf("big%d.bin", i))
			if err != nil {
				t.Fatal(err)
			}
			part.Write(make([]byte, size))
		}
		mw.WriteField("path", "/buckets/b")
		mw.Close()

		req := httptest.NewRequest(http.MethodPost, "/api/files/upload", &body)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		w := httptest.NewRecorder()

		h.UploadFile(w, req)

		if w.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("sizes %v: expected 413, got %d (body: %s)", sizes, w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "1 MB limit") {
			t.Errorf("sizes %v: expected the limit in the error, got %s", sizes, w.Body.String())
		}
	}
}

func TestUploadFile_RemovesSpooledParts(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("TMPDIR", tmpDir)

	adminServer := &dash.AdminServer{}
	adminServer.SetUploadMaxSize(34 << 20)
	h := &FileBrowserHandlers{adminServer: adminServer}

	// The 33MB part spills to disk; together the parts exceed the limit, so the upload stops there.
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for i, size := range []int{33 << 20, 3 << 19} {
		part, err := mw.CreateFormFile("files", fmt.Sprintf("part%d.bin", i))
		if err != nil {
			t.Fatal(err)
		}
		part.Write(make([]byte, size))
	}
	mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/files/upload", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	w := httptest.NewRecorder()

	h.UploadFile(w, req.WithContext(req.Context()))

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d (body: %s)", w.Code, w.Body.String())
	}
	if left, _ := os.ReadDir(tmpDir); len(left) != 0 {
		t.Errorf("spooled parts left behind: %v", left)
	}
}

func TestIsRelativeUploadPath(t *testing.T) {
	for name, want := range map[string]bool{
		"a.txt":         true,
		"photos/a.jpg":  true,
		"a/b/c/..x.txt": true,
		"/etc/a":        false,
		"a/../../b":     false,
		"..":            false,
		"a//b":          false,
		"./a":           false,
		"a/":            false,
	} {
		if got := isRelativeUploadPath(name); got != want {
			t.Errorf("isRelativeUploadPath(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestUploadFile_FolderRelativePaths(t *testing.T) {
	h := &FileBrowserHandlers{adminServer: &dash.AdminServer{}}
	upload := func(relativePaths string) *httptest.ResponseRecorder {
		var body bytes.Buffer
		mw := multipart.NewWriter(&body)
		for _, name := range []string{"a.txt", "b.txt"} {
			part, err := mw.CreateFormFile("files", name)
			if err != nil {
				t.Fatal(err)
			}
			part.Write([]byte(name))
		}
		mw.WriteField("path", "/buckets/b")
		mw.WriteField("relativePaths", relativePaths)
		mw.Close()
		req := httptest.NewRequest(http.MethodPost, "/api/files/upload", &body)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		w := httptest.NewRecorder()
		h.UploadFile(w, req)
		return w
	}

	for _, paths := range []string{`["dir/a.txt"]`, `not json`, ``, `null`} {
		if w := upload(paths); w.Code != http.StatusBadRequest {
			t.Errorf("relativePaths %s: expected 400, got %d (body: %s)", paths, w.Code, w.Body.String())
		}
	}

	w := upload(`["../a.txt", "dir\\..\\..\\b.txt"]`)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected every escaping path to fail, got %d (body: %s)", w.Code, w.Body.String())
	}
	if strings.Count(w.Body.String(), "invalid relative path") != 2 {
		t.Errorf("expected both paths rejected before upload, got %s", w.Body.String())
	}
}
