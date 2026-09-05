package winfsp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// The names come from a report of a folder tree showing up in the WinFsp
// drive percent-encoded: 负极全景 as %E8%B4%9F%E6%9E%81%E5%85%A8%E6%99%AF. The
// literal percent-encoded sibling pins the other direction: a name that
// really contains percent signs must not be decoded on the way through.
var unicodeDirs = []string{
	"负极全景",
	"负极全景/OK原图",
	"负极全景/OK渲染图",
	"%E8%B4%9F%E6%9E%81%E5%85%A8%E6%99%AF",
}

// filerPathOf maps a path under the mount to the filer path it is stored at,
// assuming the mount root is the filer root, which is how the CI job mounts.
func filerPathOf(t *testing.T, mountPath string) string {
	t.Helper()
	rel, err := filepath.Rel(*mountPoint, mountPath)
	if err != nil {
		t.Fatalf("relative path of %s under %s: %v", mountPath, *mountPoint, err)
	}
	return "/" + filepath.ToSlash(rel)
}

func filerURL(filerPath string) string {
	return (&url.URL{Scheme: "http", Host: *filerAddr, Path: filerPath}).String()
}

// listFromFiler returns the child names the filer holds for a directory,
// straight from its JSON listing, so a name the mount stored percent-encoded
// shows up as such instead of being decoded on the way back.
func listFromFiler(filerDir string) ([]string, error) {
	req, err := http.NewRequest(http.MethodGet, filerURL(strings.TrimSuffix(filerDir, "/")+"/"), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", req.URL, resp.Status)
	}
	var listing struct {
		Entries []struct{ FullPath string }
	}
	if err := json.NewDecoder(resp.Body).Decode(&listing); err != nil {
		return nil, fmt.Errorf("decode listing of %s: %v", filerDir, err)
	}
	names := make([]string, 0, len(listing.Entries))
	for _, e := range listing.Entries {
		names = append(names, path.Base(e.FullPath))
	}
	sort.Strings(names)
	return names, nil
}

func filerRequest(method, filerPath string, body []byte) error {
	req, err := http.NewRequest(method, filerURL(filerPath), bytes.NewReader(body))
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s %s: %s %s", method, req.URL, resp.Status, msg)
	}
	return nil
}

// mkdirOnFiler is the filer's own mkdir: a POST to a trailing-slash path with
// no content type.
func mkdirOnFiler(filerDir string) error {
	return filerRequest(http.MethodPost, strings.TrimSuffix(filerDir, "/")+"/", nil)
}

func mountNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir %s: %v", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

func expectNames(t *testing.T, where string, got, want []string) {
	t.Helper()
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("%s lists %q, want %q", where, got, want)
	}
}

// TestUnicodeDirectoryTree builds the reported folder tree through the mount
// and checks the names at every level, first as the mount lists them back and
// then as the filer stored them.
func TestUnicodeDirectoryTree(t *testing.T) {
	dir := testRoot(t)
	for _, rel := range unicodeDirs {
		if err := os.Mkdir(filepath.Join(dir, filepath.FromSlash(rel)), 0755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
	}
	leafFile := filepath.Join(dir, "负极全景", "OK原图", "图片.jpg")
	if err := os.WriteFile(leafFile, []byte("图片"), 0644); err != nil {
		t.Fatalf("write %s: %v", leafFile, err)
	}

	// Lookups by path are what Explorer does when it opens a folder, and go
	// through a different route than listing.
	for _, rel := range unicodeDirs {
		info, err := os.Stat(filepath.Join(dir, filepath.FromSlash(rel)))
		if err != nil {
			t.Errorf("stat %s: %v", rel, err)
		} else if !info.IsDir() {
			t.Errorf("%s is not a directory", rel)
		}
	}
	expectNames(t, "mount root", mountNames(t, dir), []string{"负极全景", "%E8%B4%9F%E6%9E%81%E5%85%A8%E6%99%AF"})
	expectNames(t, "mount 负极全景", mountNames(t, filepath.Join(dir, "负极全景")), []string{"OK原图", "OK渲染图"})
	expectNames(t, "mount 负极全景/OK原图", mountNames(t, filepath.Join(dir, "负极全景", "OK原图")), []string{"图片.jpg"})
	if got, err := os.ReadFile(leafFile); err != nil {
		t.Errorf("read %s: %v", leafFile, err)
	} else if string(got) != "图片" {
		t.Errorf("read %s: %q", leafFile, got)
	}

	if err := os.Rename(filepath.Join(dir, "负极全景", "OK渲染图"), filepath.Join(dir, "负极全景", "OK渲染图-改")); err != nil {
		t.Fatalf("rename: %v", err)
	}
	expectNames(t, "mount 负极全景 after rename", mountNames(t, filepath.Join(dir, "负极全景")), []string{"OK原图", "OK渲染图-改"})

	if *filerAddr == "" {
		t.Log("no -filer given; skipping the check of what the filer stored")
		return
	}
	root := filerPathOf(t, dir)
	for _, check := range []struct {
		dir  string
		want []string
	}{
		{root, []string{"负极全景", "%E8%B4%9F%E6%9E%81%E5%85%A8%E6%99%AF"}},
		{root + "/负极全景", []string{"OK原图", "OK渲染图-改"}},
		{root + "/负极全景/OK原图", []string{"图片.jpg"}},
	} {
		got, err := listFromFiler(check.dir)
		if err != nil {
			t.Errorf("filer listing: %v", err)
			continue
		}
		expectNames(t, "filer "+check.dir, got, check.want)
	}
}

// TestUnicodeNamesCreatedOnFiler is the other direction: the tree is created
// through the filer's HTTP API, in a directory the mount has not listed yet,
// so the mount's first look at it has to fetch the names from the filer.
func TestUnicodeNamesCreatedOnFiler(t *testing.T) {
	if *filerAddr == "" {
		t.Skip("no -filer given; this test creates entries through the filer")
	}
	dir := testRoot(t)
	root := filerPathOf(t, dir)
	for _, rel := range unicodeDirs {
		if err := mkdirOnFiler(root + "/" + rel); err != nil {
			t.Fatalf("filer mkdir %s: %v", rel, err)
		}
	}
	if err := filerRequest(http.MethodPut, root+"/负极全景/OK原图/图片.jpg", []byte("图片")); err != nil {
		t.Fatalf("filer put: %v", err)
	}

	expectNames(t, "mount root", mountNames(t, dir), []string{"负极全景", "%E8%B4%9F%E6%9E%81%E5%85%A8%E6%99%AF"})
	expectNames(t, "mount 负极全景", mountNames(t, filepath.Join(dir, "负极全景")), []string{"OK原图", "OK渲染图"})
	expectNames(t, "mount 负极全景/OK原图", mountNames(t, filepath.Join(dir, "负极全景", "OK原图")), []string{"图片.jpg"})
	for _, rel := range unicodeDirs {
		if info, err := os.Stat(filepath.Join(dir, filepath.FromSlash(rel))); err != nil {
			t.Errorf("stat %s: %v", rel, err)
		} else if !info.IsDir() {
			t.Errorf("%s is not a directory", rel)
		}
	}
	leafFile := filepath.Join(dir, "负极全景", "OK原图", "图片.jpg")
	if got, err := os.ReadFile(leafFile); err != nil {
		t.Errorf("read %s: %v", leafFile, err)
	} else if string(got) != "图片" {
		t.Errorf("read %s: %q", leafFile, got)
	}
}
