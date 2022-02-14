//go:build !linux && !darwin && !freebsd
// +build !linux,!darwin,!freebsd

package command

import (
	"fmt"
	"runtime"
)

func runMount2(cmd *Command, args []string) bool {
	fmt.Printf("Mount is not supported on %s %s\n", runtime.GOOS, runtime.GOARCH)

	return true
}
