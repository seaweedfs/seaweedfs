//go:build windows

package pb

import "syscall"

// Windows never registers local Unix sockets, so there is nothing to tune.
func setLocalSocketBuffers(_, _ string, _ syscall.RawConn) error { return nil }
