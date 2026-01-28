//go:build !darwin && !freebsd && !linux
// +build !darwin,!freebsd,!linux

package command

import (
	"fmt"
	"runtime"
)

func runFuse(cmd *Command, args []string) bool {
	fmt.Printf("Fuse is not supported on %s %s\n", runtime.GOOS, runtime.GOARCH)

	return true
}
