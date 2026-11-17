package command

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestGrpcLocalSocketPath checks that -grpc.localSocketDir defaults to /tmp
// for `weed server` and `weed mini` and that every role's socket lands in it.
func TestGrpcLocalSocketPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-socket routing is disabled on Windows")
	}

	for _, tc := range []struct {
		command string
		dir     *string
	}{
		{"weed server", serverGrpcLocalSocketDir},
		{"weed mini", miniGrpcLocalSocketDir},
	} {
		t.Run(tc.command, func(t *testing.T) {
			if *tc.dir != "/tmp" {
				t.Fatalf("-grpc.localSocketDir default = %q, want /tmp", *tc.dir)
			}
			if got, want := grpcLocalSocketPath(*tc.dir, "volume", 17334), "/tmp/seaweedfs-volume-grpc-17334.sock"; got != want {
				t.Fatalf("default socket path = %q, want %q", got, want)
			}

			dir := t.TempDir()
			prev := *tc.dir
			*tc.dir = dir
			t.Cleanup(func() { *tc.dir = prev })

			for _, role := range []string{"master", "volume", "filer", "s3", "admin"} {
				path := grpcLocalSocketPath(*tc.dir, role, 17334)
				if got := filepath.Dir(path); got != dir {
					t.Fatalf("%s socket path %q is not in %q", role, path, dir)
				}
				if got, want := filepath.Base(path), "seaweedfs-"+role+"-grpc-17334.sock"; got != want {
					t.Fatalf("%s socket file name = %q, want %q", role, got, want)
				}
			}
		})
	}
}

// TestGrpcLocalSocketDir checks that -grpc.localSocketDir is refused when one
// of its socket paths cannot fit in the platform's sockaddr_un.
func TestGrpcLocalSocketDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-socket routing is disabled on Windows")
	}

	sockets := []grpcLocalSocket{{"volume", 17334}}
	limit := grpcLocalSocketPathLimit()

	// /tmp is the historical default and short enough on every Unix for any
	// socket name; t.TempDir() is not, since macOS roots it under a long
	// /var/folders path.
	if err := checkGrpcLocalSocketDir("/tmp", sockets); err != nil {
		t.Fatalf("checkGrpcLocalSocketDir(%q): %v", "/tmp", err)
	}

	dir := t.TempDir()
	for len(grpcLocalSocketPath(dir, "volume", 17334)) <= limit {
		dir = filepath.Join(dir, "abcdefghijklmnop")
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatalf("create %q: %v", dir, err)
		}
	}
	err := checkGrpcLocalSocketDir(dir, sockets)
	if err == nil {
		t.Fatalf("checkGrpcLocalSocketDir accepted %q, whose socket path is longer than the %d-byte limit", dir, limit)
	}
	if path := grpcLocalSocketPath(dir, "volume", 17334); !strings.Contains(err.Error(), path) {
		t.Fatalf("error %q does not name the socket path %q", err, path)
	}
	if !strings.Contains(err.Error(), strconv.Itoa(limit)) {
		t.Fatalf("error %q does not name the %d-byte limit", err, limit)
	}
}

// TestGrpcLocalSocketDirAtLimit checks that a folder whose longest socket
// path is exactly the platform limit is accepted and that the socket binds.
func TestGrpcLocalSocketDirAtLimit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-socket routing is disabled on Windows")
	}

	base, err := os.MkdirTemp("/tmp", "seaweedfs-grpc-limit-")
	if err != nil {
		t.Fatalf("create folder: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(base); err != nil {
			t.Errorf("remove %q: %v", base, err)
		}
	})

	sockets := []grpcLocalSocket{{"s3", 18333}, {"volume", 18080}}
	limit := grpcLocalSocketPathLimit()
	name := filepath.Base(grpcLocalSocketPath(base, "volume", 18080))
	dir := filepath.Join(base, strings.Repeat("d", limit-len(base)-len(name)-2))
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("create %q: %v", dir, err)
	}
	path := grpcLocalSocketPath(dir, "volume", 18080)
	if len(path) != limit {
		t.Fatalf("socket path %q is %d bytes, want %d", path, len(path), limit)
	}

	if err := checkGrpcLocalSocketDir(dir, sockets); err != nil {
		t.Fatalf("checkGrpcLocalSocketDir(%q): %v", dir, err)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("bind %q: %v", path, err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close %q: %v", path, err)
	}
}

// TestGrpcLocalSocketDirUnwritable checks that -grpc.localSocketDir is
// refused when the server's user cannot create a socket in the folder.
func TestGrpcLocalSocketDirUnwritable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-socket routing is disabled on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses folder mode bits")
	}

	// A folder directly under /tmp keeps the probe socket path well inside the
	// sockaddr_un limit, which t.TempDir() does not on macOS.
	dir, err := os.MkdirTemp("/tmp", "seaweedfs-grpc-unwritable-")
	if err != nil {
		t.Fatalf("create folder: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(dir, 0o755); err != nil {
			t.Errorf("restore mode of %q: %v", dir, err)
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove %q: %v", dir, err)
		}
	})
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("chmod %q: %v", dir, err)
	}

	err = checkGrpcLocalSocketDir(dir, []grpcLocalSocket{{"volume", 17334}})
	if err == nil {
		t.Fatalf("checkGrpcLocalSocketDir accepted %q, where the server cannot create a socket", dir)
	}
	if !strings.Contains(err.Error(), dir) {
		t.Fatalf("error %q does not name the folder %q", err, dir)
	}
	if !strings.Contains(err.Error(), "cannot create a Unix socket") {
		t.Fatalf("error %q does not report the failed socket bind", err)
	}
}

// TestGrpcLocalSocketDirProbeLeavesNothing checks that the startup probe
// removes its socket, so repeated starts never fill a shared folder.
func TestGrpcLocalSocketDirProbeLeavesNothing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-socket routing is disabled on Windows")
	}

	// See TestGrpcLocalSocketDirUnwritable for why the folder is under /tmp.
	dir, err := os.MkdirTemp("/tmp", "seaweedfs-grpc-probe-")
	if err != nil {
		t.Fatalf("create folder: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove %q: %v", dir, err)
		}
	})

	for i := 0; i < 3; i++ {
		if err := checkGrpcLocalSocketDir(dir, []grpcLocalSocket{{"volume", 17334}}); err != nil {
			t.Fatalf("checkGrpcLocalSocketDir(%q): %v", dir, err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %q: %v", dir, err)
	}
	if len(entries) != 0 {
		t.Fatalf("probe left %d entries in %q, first %q", len(entries), dir, entries[0].Name())
	}
}
