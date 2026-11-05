package weed_server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

func TestListDirectoryWriteControlsFollowPort(t *testing.T) {
	mtime := time.Unix(1700000000, 0)
	store := newRenameTestStore()
	store.entries["/docs"] = &filer.Entry{FullPath: "/docs", Attr: filer.Attr{Mtime: mtime, Mode: os.ModeDir | 0755}}
	store.entries["/docs/sub"] = &filer.Entry{FullPath: "/docs/sub", Attr: filer.Attr{Mtime: mtime, Mode: os.ModeDir | 0755}}
	store.entries["/docs/a.txt"] = &filer.Entry{FullPath: "/docs/a.txt", Attr: filer.Attr{Mtime: mtime, Mode: 0644}}
	store.entries["/empty"] = &filer.Entry{FullPath: "/empty", Attr: filer.Attr{Mtime: mtime, Mode: os.ModeDir | 0755}}
	fs := &FilerServer{
		filer:      newRenameTestFiler(t, store),
		option:     &FilerOption{ExposeDirectoryData: true, DirListingLimit: 100, ShowUIDirectoryDelete: true},
		filerGuard: security.NewGuard(nil, "", 0, "", 0),
	}

	writeControls := []string{
		`onclick="handleCreateDir()"`,
		`for="fileElem"`,
		`for="dirElem"`,
		`onclick="handleRename(`,
		`onclick="handleDelete(`,
		`class="row add-files"`,
		"function uploadFile(",
	}

	render := func(t *testing.T, handler http.HandlerFunc, path string) string {
		t.Helper()
		rec := httptest.NewRecorder()
		handler(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s: status %d, body %s", path, rec.Code, rec.Body.String())
		}
		return rec.Body.String()
	}

	for _, path := range []string{"/docs/", "/empty/"} {
		readOnly := render(t, fs.readonlyFilerHandler, path)
		if !strings.Contains(readOnly, "<ol class=\"breadcrumb\">") {
			t.Fatalf("read-only listing of %s is not the directory page:\n%s", path, readOnly)
		}
		for _, control := range writeControls {
			if strings.Contains(readOnly, control) {
				t.Errorf("read-only listing of %s offers %q", path, control)
			}
		}
	}
	if readOnly := render(t, fs.readonlyFilerHandler, "/docs/"); !strings.Contains(readOnly, "a.txt") || !strings.Contains(readOnly, "sub") {
		t.Errorf("read-only listing lost its entries:\n%s", readOnly)
	}

	writable := render(t, fs.GetOrHeadHandler, "/docs/") + render(t, fs.GetOrHeadHandler, "/empty/")
	for _, control := range writeControls {
		if !strings.Contains(writable, control) {
			t.Errorf("listing on the main port is missing %q", control)
		}
	}
}
