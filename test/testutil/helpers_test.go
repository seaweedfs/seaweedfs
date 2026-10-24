package testutil

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
)

func TestIsIllegalInstruction(t *testing.T) {
	if code := os.Getenv("SEAWEEDFS_TEST_EXIT_CODE"); code != "" {
		status, _ := strconv.Atoi(code)
		os.Exit(status)
	}

	for _, tc := range []struct {
		name string
		code string
		want bool
	}{
		{"illegal instruction", "132", true},
		{"ordinary failure", "1", false},
		{"out of memory", "137", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestIsIllegalInstruction$")
			cmd.Env = append(os.Environ(), "SEAWEEDFS_TEST_EXIT_CODE="+tc.code)
			err := cmd.Run()
			if got := IsIllegalInstruction(fmt.Errorf("docker run: %w", err)); got != tc.want {
				t.Fatalf("IsIllegalInstruction(%v) = %v, want %v", err, got, tc.want)
			}
		})
	}
	if IsIllegalInstruction(nil) || IsIllegalInstruction(errors.New("exit status 132")) {
		t.Fatal("non-process errors must not be treated as illegal instructions")
	}
}
