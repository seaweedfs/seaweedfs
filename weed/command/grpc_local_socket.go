package command

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

// grpcLocalSocket names a local gRPC socket this process registers: the
// service name and the resolved gRPC port it listens on.
type grpcLocalSocket struct {
	name string
	port int
}

// grpcLocalSocketPath returns the Unix socket path a gRPC service listens on
// for local inter-service RPCs, inside dir.
func grpcLocalSocketPath(dir, name string, grpcPort int) string {
	return filepath.Join(dir, fmt.Sprintf("seaweedfs-%s-grpc-%d.sock", name, grpcPort))
}

// grpcLocalSocketPathLimit returns the longest Unix socket path the platform
// accepts. sun_path is a fixed-size field in sockaddr_un (108 bytes on Linux,
// 104 on macOS and the BSDs) and bind fails with EINVAL for a longer path; the
// terminating NUL occupies one byte of it.
func grpcLocalSocketPathLimit() int {
	return len(syscall.RawSockaddrUnix{}.Path) - 1
}

// checkGrpcLocalSocketDir rejects a socket folder that does not exist, is not
// a directory, or is too long to hold one of its sockets as a Unix socket path, so
// a bad -grpc.localSocketDir fails at startup instead of leaving every local
// RPC to dial a socket that was never bound.
//
// Windows never creates these sockets (see pb.RegisterLocalGrpcSocket), so
// there the folder is irrelevant and is not checked.
func checkGrpcLocalSocketDir(dir string, sockets []grpcLocalSocket) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("not a directory")
	}
	limit := grpcLocalSocketPathLimit()
	for _, socket := range sockets {
		path := grpcLocalSocketPath(dir, socket.name, socket.port)
		if len(path) > limit {
			return fmt.Errorf("socket path %q is %d bytes and does not fit the platform's %d-byte Unix socket path limit; use a shorter -grpc.localSocketDir", path, len(path), limit)
		}
	}
	return nil
}
