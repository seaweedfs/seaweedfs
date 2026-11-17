package command

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net"
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
// a directory, is too long to hold one of its sockets as a Unix socket path, or
// refuses a socket bind by the server's user, so a bad -grpc.localSocketDir
// fails at startup instead of leaving every local RPC to dial a socket that
// was never bound.
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

	// An existing folder can still refuse a bind, for example because it is
	// owned by another user or mounted read-only. ListenOnLocalSocket only
	// logs that failure while GrpcDial keeps dialing the missing socket, so
	// prove now that this process can create one. The probe name is random,
	// so a file another user left in a shared folder cannot block startup.
	return probeGrpcLocalSocketDir(dir, limit)
}

// probeGrpcLocalSocketDir binds and removes a randomly named Unix socket in
// dir, retrying a name that is already taken.
func probeGrpcLocalSocketDir(dir string, limit int) error {
	const attempts = 5
	var lastErr error
	for i := 0; i < attempts; i++ {
		suffix := make([]byte, 8)
		if _, err := rand.Read(suffix); err != nil {
			return fmt.Errorf("cannot pick a Unix socket probe name: %v", err)
		}
		probePath := filepath.Join(dir, fmt.Sprintf("seaweedfs-probe-%x.sock", suffix))
		if len(probePath) > limit {
			return fmt.Errorf("probe socket path %q is %d bytes and does not fit the platform's %d-byte Unix socket path limit; use a shorter -grpc.localSocketDir", probePath, len(probePath), limit)
		}
		listener, err := net.Listen("unix", probePath)
		if errors.Is(err, syscall.EADDRINUSE) {
			lastErr = err
			continue
		}
		if err != nil {
			return fmt.Errorf("cannot create a Unix socket in it: %v; give the server's user write access or choose another -grpc.localSocketDir", err)
		}
		// Closing a listener the Go runtime created unlinks its socket file.
		if err := listener.Close(); err != nil {
			return fmt.Errorf("cannot close the Unix socket probe %q: %v", probePath, err)
		}
		return nil
	}
	return fmt.Errorf("cannot create a Unix socket in it: %d random probe names were all taken, last: %v", attempts, lastErr)
}
