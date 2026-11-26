//go:build linux

package util

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestProcessHugePagesDisabled(t *testing.T) {
	if inherited, err := unix.PrctlRetInt(unix.PR_GET_THP_DISABLE, 0, 0, 0, 0); err != nil || inherited != 0 {
		t.Skipf("PR_GET_THP_DISABLE = %d, %v; want a process with THP allowed", inherited, err)
	}
	t.Cleanup(func() { unix.Prctl(unix.PR_SET_THP_DISABLE, 0, 0, 0, 0) })

	if err := unix.Prctl(unix.PR_SET_THP_DISABLE, 1, 0, 0, 0); err != nil {
		t.Skipf("PR_SET_THP_DISABLE: %v", err)
	}
	if !processHugePagesDisabled() {
		t.Error("processHugePagesDisabled() = false after PR_SET_THP_DISABLE=1")
	}
	if err := unix.Prctl(unix.PR_SET_THP_DISABLE, 0, 0, 0, 0); err != nil {
		t.Fatalf("clear PR_SET_THP_DISABLE: %v", err)
	}
	if processHugePagesDisabled() {
		t.Error("processHugePagesDisabled() = true after PR_SET_THP_DISABLE=0")
	}
}
