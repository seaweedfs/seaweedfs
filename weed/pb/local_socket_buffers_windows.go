//go:build windows

package pb

import (
	"net"
	"syscall"
)

// Windows never registers local Unix sockets, so there is nothing to tune.
func setLocalSocketBuffers(_, _ string, _ syscall.RawConn) error { return nil }

func applyLocalSocketBuffers(_ net.Conn) {}
