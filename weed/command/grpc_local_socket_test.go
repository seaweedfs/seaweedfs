package command

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestGrpcLocalSocketPath covers the -grpc.localSocketDir folder that
// `weed server` and `weed mini` hand to pb.RegisterLocalGrpcSocket. Unset, the
// path stays the historical /tmp one; set, every role's socket lands in that
// folder (issue #12692).
func TestGrpcLocalSocketPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-socket routing is disabled on Windows (#9430)")
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
// of its socket paths cannot fit in the platform's sockaddr_un (issue #12692).
// A folder whose socket binds fail leaves every local RPC dialing a socket
// that was never created.
func TestGrpcLocalSocketDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-socket routing is disabled on Windows (#9430)")
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
