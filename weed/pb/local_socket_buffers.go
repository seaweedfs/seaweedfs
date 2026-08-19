//go:build !windows

package pb

import "syscall"

// Unix socket buffers default small and never autotune (208KB on Linux, 8KB on
// macOS). Once one cannot absorb what gRPC's loopyWriter emits for the in-flight
// streams the writer blocks, both peers latch grpc-go's control-buffer throttle,
// and the connection deadlocks for good. Best effort: a value the OS clamps down
// only lowers the concurrency this survives.
const localSocketBufBytes = 8 << 20

func setLocalSocketBuffers(_, _ string, c syscall.RawConn) error {
	return c.Control(func(fd uintptr) {
		syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, localSocketBufBytes)
		syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, localSocketBufBytes)
	})
}
