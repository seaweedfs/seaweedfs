//go:build !windows

package http

import (
	"errors"
	"syscall"
)

func connRefused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED)
}

func noRoute(err error) bool {
	return errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTDOWN)
}
