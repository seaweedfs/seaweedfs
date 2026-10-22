//go:build linux

package fuse

import "testing"

func TestMountedAt(t *testing.T) {
	for _, p := range []string{"/proc", "/proc/"} {
		if !mountedAt(p) {
			t.Errorf("mountedAt(%q) = false, want true", p)
		}
	}
	plain := t.TempDir()
	for _, p := range []string{plain, plain + "/"} {
		if mountedAt(p) {
			t.Errorf("mountedAt(%q) = true, want false", p)
		}
	}
}

func TestFuseFilesystem(t *testing.T) {
	for _, p := range []string{"/proc", t.TempDir()} {
		if fuseFilesystem(p) {
			t.Errorf("fuseFilesystem(%q) = true, want false", p)
		}
	}
}
