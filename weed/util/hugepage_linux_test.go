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
	if disabled, known := processHugePagesDisabled(); !disabled || !known {
		t.Errorf("processHugePagesDisabled() = %t, %t after PR_SET_THP_DISABLE=1", disabled, known)
	}
	if err := unix.Prctl(unix.PR_SET_THP_DISABLE, 0, 0, 0, 0); err != nil {
		t.Fatalf("clear PR_SET_THP_DISABLE: %v", err)
	}
	if disabled, known := processHugePagesDisabled(); disabled || !known {
		t.Errorf("processHugePagesDisabled() = %t, %t after PR_SET_THP_DISABLE=0", disabled, known)
	}
}
