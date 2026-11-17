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

// grpcLocalSocket is a service name and the resolved gRPC port it listens on.
type grpcLocalSocket struct {
	name string
	port int
}

// grpcLocalSocketPath returns the Unix socket path for local RPCs to a gRPC
// service, inside dir.
func grpcLocalSocketPath(dir, name string, grpcPort int) string {
	return filepath.Join(dir, fmt.Sprintf("seaweedfs-%s-grpc-%d.sock", name, grpcPort))
}

// grpcLocalSocketPathLimit returns the longest Unix socket path the platform
// accepts: sockaddr_un's sun_path less the terminating NUL.
func grpcLocalSocketPathLimit() int {
	return len(syscall.RawSockaddrUnix{}.Path) - 1
}

// checkGrpcLocalSocketDir rejects a socket folder that does not exist, is not
// a directory, is too long for one of its socket paths, or refuses a socket
// bind, since local RPCs keep dialing a registered socket that was never bound.
//
// The folder is unused, and not checked, on Windows (see
// pb.RegisterLocalGrpcSocket) or when no service registers a socket.
func checkGrpcLocalSocketDir(dir string, sockets []grpcLocalSocket) error {
	if runtime.GOOS == "windows" || len(sockets) == 0 {
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
	return probeGrpcLocalSocketDir(dir)
}

// probeGrpcLocalSocketDir binds and removes a randomly named Unix socket in
// dir, so a file another user left in a shared folder cannot block startup.
// The 23-byte name is shorter than any service socket name.
func probeGrpcLocalSocketDir(dir string) error {
	const attempts = 5
	var lastErr error
	for i := 0; i < attempts; i++ {
		suffix := make([]byte, 4)
		if _, err := rand.Read(suffix); err != nil {
			return fmt.Errorf("cannot pick a Unix socket probe name: %v", err)
		}
		probePath := filepath.Join(dir, fmt.Sprintf("seaweedfs-%x.sock", suffix))
		listener, err := net.Listen("unix", probePath)
		if errors.Is(err, syscall.EADDRINUSE) {
			lastErr = err
			continue
		}
		if err != nil {
			return fmt.Errorf("cannot create a Unix socket in it: %v; give the server's user write access or choose another -grpc.localSocketDir", err)
		}
		if err := listener.Close(); err != nil {
			return fmt.Errorf("cannot close the Unix socket probe %q: %v", probePath, err)
		}
		return nil
	}
	return fmt.Errorf("cannot create a Unix socket in it: %d random probe names were all taken, last: %v", attempts, lastErr)
}
