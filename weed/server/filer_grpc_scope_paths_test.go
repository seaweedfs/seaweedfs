package weed_server

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

func TestScopePathRulesRejectNonCanonicalAndSiblingPrefixes(t *testing.T) {
	grants, err := scopeGrantsFromClaims([]string{aliceWS}, []string{groupG1})
	if err != nil {
		t.Fatalf("scope grants: %v", err)
	}
	cases := []struct {
		name        string
		path        string
		read, write bool
	}{
		{name: "grant root", path: aliceWS, read: true, write: true},
		{name: "nested path", path: aliceWS + "/child", read: true, write: true},
		{name: "component boundary", path: aliceWS + "2", read: false, write: false},
		{name: "parent", path: "/buckets/t1/users/alice", read: false, write: false},
		{name: "read-only grant", path: groupG1 + "/report", read: true, write: false},
		{name: "sibling user", path: bobWS, read: false, write: false},
		{name: "relative", path: "buckets/t1/users/alice", read: false, write: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := scopePathAllowed(grants, tc.path, false); got != tc.read {
				t.Errorf("read %q = %v, want %v", tc.path, got, tc.read)
			}
			if got := scopePathAllowed(grants, tc.path, true); got != tc.write {
				t.Errorf("write %q = %v, want %v", tc.path, got, tc.write)
			}
		})
	}

	for _, bad := range []string{"", "relative", aliceWS + "/", "/a//b", "/a/./b", "/a/../b"} {
		if _, err := scopeGrantsFromClaims([]string{bad}, nil); err == nil {
			t.Errorf("unclean grant %q was accepted", bad)
		}
	}
	for _, tc := range []struct {
		dir, name string
	}{
		{dir: aliceWS + "/", name: "file"},
		{dir: aliceWS + "/../bob", name: "file"},
		{dir: "relative", name: "file"},
		{dir: aliceWS, name: "../file"},
		{dir: aliceWS, name: "nested/file"},
	} {
		if _, err := scopeFullPath(tc.dir, tc.name); err == nil {
			t.Errorf("scopeFullPath(%q, %q) accepted a non-canonical path", tc.dir, tc.name)
		}
	}
}

func TestScopeReadAndWritePrefixAccess(t *testing.T) {
	h := newScopeHarness(t)
	h.fake.putDir(aliceWS)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, []string{groupG1}))

	expectScopeForwarded(t, h, "LookupDirectoryEntry", "read in the write prefix", func() error {
		_, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: aliceWS, Name: "file"})
		return err
	})
	expectScopeForwarded(t, h, "LookupDirectoryEntry", "read in the read-only prefix", func() error {
		_, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: groupG1, Name: "report"})
		return err
	})
	expectScopeForwarded(t, h, "CreateEntry", "metadata create in the write prefix", func() error {
		_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: entryWith("metadata-only")})
		return err
	})
	expectScopeDenied(t, h, "CreateEntry", "create in the read-only prefix", func() error {
		_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{Directory: groupG1, Entry: entryWith("refused")})
		return err
	})
	expectScopeDenied(t, h, "DeleteEntry", "delete in the read-only prefix", func() error {
		_, err := c.DeleteEntry(scopeCtx(t), &filer_pb.DeleteEntryRequest{Directory: groupG1, Name: "report"})
		return err
	})

	for _, tc := range []struct {
		name, dir, entry string
	}{
		{name: "t10 sibling", dir: "/buckets/t10/users/alice", entry: "file"},
		{name: "dot-dot escape", dir: aliceWS + "/../../bob/workspace", entry: "file"},
		{name: "trailing slash", dir: aliceWS + "/", entry: "file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectScopeDenied(t, h, "LookupDirectoryEntry", tc.name, func() error {
				_, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: tc.dir, Name: tc.entry})
				return err
			})
		})
	}
}

func TestAuthorizeFilerJwtPathsHonoursReadPrefixes(t *testing.T) {
	claims := &security.SeaweedFilerClaims{
		AllowedPrefixes: []string{aliceWS},
		ReadPrefixes:    []string{groupG1},
	}
	cases := []struct {
		name, method, path string
		want               bool
	}{
		{name: "read-only GET", method: "GET", path: groupG1 + "/report", want: true},
		{name: "read-only HEAD", method: "HEAD", path: groupG1 + "/report", want: true},
		{name: "read-only PUT", method: "PUT", path: groupG1 + "/report", want: false},
		{name: "write prefix PUT", method: "PUT", path: aliceWS + "/file", want: true},
		{name: "sibling GET", method: "GET", path: "/buckets/t10/users/alice/file", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			if got := authorizeFilerJwtPaths(r, claims, []string{r.URL.Path}); got != tc.want {
				t.Errorf("authorizeFilerJwtPaths(%s %s) = %v, want %v", tc.method, tc.path, got, tc.want)
			}
		})
	}
}

func TestAuthorizeFilerJwtPathsReadOnlyClaimsWithoutWritePrefixes(t *testing.T) {
	claims := &security.SeaweedFilerClaims{ReadPrefixes: []string{groupG1}}
	for _, tc := range []struct {
		name, method, path string
		want               bool
	}{
		{name: "GET in read-only prefix", method: "GET", path: groupG1 + "/report", want: true},
		{name: "HEAD in read-only prefix", method: "HEAD", path: groupG1 + "/report", want: true},
		{name: "PUT in read-only prefix", method: "PUT", path: groupG1 + "/report", want: false},
		{name: "GET outside read-only prefix", method: "GET", path: aliceWS + "/report", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			if got := authorizeFilerJwtPaths(r, claims, []string{r.URL.Path}); got != tc.want {
				t.Errorf("authorizeFilerJwtPaths(%s %s) = %v, want %v", tc.method, tc.path, got, tc.want)
			}
		})
	}
}

func TestAuthorizeFilerJwtPathsRejectsNoncanonicalReadPrefixes(t *testing.T) {
	for _, prefix := range []string{".", "./", "relative", "/a/../", "/a/"} {
		t.Run(prefix, func(t *testing.T) {
			claims := &security.SeaweedFilerClaims{ReadPrefixes: []string{prefix}}
			for _, method := range []string{"GET", "HEAD"} {
				r := httptest.NewRequest(method, "/outside/file", nil)
				if authorizeFilerJwtPaths(r, claims, []string{r.URL.Path}) {
					t.Fatalf("%s accepted noncanonical read prefix %q", method, prefix)
				}
			}
		})
	}
}
