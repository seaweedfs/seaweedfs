package sftpd

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
)

func TestPathWithin(t *testing.T) {
	tests := []struct {
		base      string
		candidate string
		want      bool
	}{
		{"/tenants/alice", "/tenants/alice", true},
		{"/tenants/alice", "/tenants/alice/file.txt", true},
		{"/tenants/alice", "/tenants/alice/sub/dir", true},
		{"/tenants/alice", "/tenants/alice-archive", false},
		{"/tenants/alice", "/tenants/alice-archive/secret.txt", false},
		{"/tenants/alice", "/tenants/alice2", false},
		{"/tenants/alice", "/tenants", false},
		{"/tenants/alice/", "/tenants/alice/file.txt", true},
		{"/tenants/alice", "/tenants/alice/../alice-archive", false},
		{"/tenants/alice", "/tenants/alice/..", false},
		{"/", "/", true},
		{"/", "/anything", true},
	}
	for _, tt := range tests {
		if got := pathWithin(tt.base, tt.candidate); got != tt.want {
			t.Errorf("pathWithin(%q, %q) = %v, want %v", tt.base, tt.candidate, got, tt.want)
		}
	}
}

func TestHasExplicitPermissionSiblingPrefix(t *testing.T) {
	u := &user.User{
		Username: "scoped",
		HomeDir:  "/",
		Permissions: map[string][]string{
			"/tenants/alice": {"read", "write", "list"},
		},
	}

	if !HasExplicitPermission(u, "/tenants/alice/file.txt", PermRead, false) {
		t.Error("expected read permission under /tenants/alice")
	}
	if HasExplicitPermission(u, "/tenants/alice-archive/secret.txt", PermRead, false) {
		t.Error("ACL for /tenants/alice must not grant read on sibling /tenants/alice-archive")
	}
	if HasExplicitPermission(u, "/tenants/alice-archive/secret.txt", PermWrite, false) {
		t.Error("ACL for /tenants/alice must not grant write on sibling /tenants/alice-archive")
	}
}

func TestIsPathInHomeDirectorySiblingPrefix(t *testing.T) {
	u := &user.User{Username: "alice", HomeDir: "/tenants/alice"}

	if !isPathInHomeDirectory(u, "/tenants/alice/file.txt") {
		t.Error("expected /tenants/alice/file.txt to be within home /tenants/alice")
	}
	if isPathInHomeDirectory(u, "/tenants/alice-archive/secret.txt") {
		t.Error("home /tenants/alice must not match sibling /tenants/alice-archive")
	}
}
