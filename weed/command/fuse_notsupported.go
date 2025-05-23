//go:build !linux && !darwin
// +build !linux,!darwin

package command

import (
	"fmt"
	"runtime"
)

func runFuse(cmd *Command, args []string) bool {
	fmt.Printf("Fuse is not supported on %s %s\n", runtime.GOOS, runtime.GOARCH)

	return true
}
