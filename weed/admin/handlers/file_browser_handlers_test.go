package handlers

import (
	"net/http"
	"net/http/httptest"
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
