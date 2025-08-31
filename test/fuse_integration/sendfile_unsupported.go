//go:build !linux && !darwin

package fuse

import (
	"errors"
)

// Sendfile support for unsupported platforms

var ErrSendfileNotSupported = errors.New("sendfile not supported on this platform")

func sendfileTransfer(outFd int, inFd int, offset *int64, count int) (int, error) {
	return 0, ErrSendfileNotSupported
}

func isSendfileSupported() bool {
	return false
}
