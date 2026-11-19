package http

import (
	"errors"

	"golang.org/x/sys/windows"
)

func connRefused(err error) bool {
	return errors.Is(err, windows.WSAECONNREFUSED)
}

func noRoute(err error) bool {
	return errors.Is(err, windows.WSAEHOSTUNREACH) || errors.Is(err, windows.WSAENETUNREACH) || errors.Is(err, windows.WSAEHOSTDOWN)
}
