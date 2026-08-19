//go:build !windows

package pb

import (
	"net"
	"syscall"
)

// Unix socket buffers default small and never autotune (208KB on Linux, 8KB on
// macOS). Once a buffer cannot absorb what gRPC's loopyWriter emits for the
// in-flight streams the writer blocks, both peers latch grpc-go's control-buffer
// throttle, and the connection deadlocks for good. Best effort: a value the OS
// clamps down only lowers the concurrency this survives.
const localSocketBufBytes = 8 << 20

func setLocalSocketBufferFD(fd uintptr) {
	syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, localSocketBufBytes)
	syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, localSocketBufBytes)
}

func setLocalSocketBuffers(_, _ string, c syscall.RawConn) error {
	return c.Control(setLocalSocketBufferFD)
}

// applyLocalSocketBuffers tunes an already-established connection. Linux does
// not carry the listener's buffer sizes onto accepted sockets, so the server
// half needs setting explicitly or only the dialing side gets the headroom.
func applyLocalSocketBuffers(c net.Conn) {
	sc, ok := c.(syscall.Conn)
	if !ok {
		return
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return
	}
	raw.Control(setLocalSocketBufferFD)
}
