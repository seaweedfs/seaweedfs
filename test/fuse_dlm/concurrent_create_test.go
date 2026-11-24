//go:build linux || darwin

package fuse_dlm

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestConcurrentCreateWithoutDLM has two mounts without -dlm create the same
// new file, each before the other closes, with different bytes. Mount 0
// closes first; mount 1 closes once mount 0's entry has had time to reach it.
//
// A close() that returns success must not lose its writes. If mount 1's close
// succeeds, both mounts must read mount 1's bytes; if it fails, the conflict
// was reported and both must read mount 0's bytes.
func TestConcurrentCreateWithoutDLM(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping FUSE integration test in short mode")
	}

	cluster := startTestCluster(t, false)
	t.Cleanup(cluster.Stop)

	wantA := bytes.Repeat([]byte("A"), 4096)
	wantB := bytes.Repeat([]byte("B"), 4096)

	for _, tc := range []struct {
		name  string
		flags int
	}{
		{"trunc", os.O_CREATE | os.O_WRONLY | os.O_TRUNC},
		{"excl", os.O_CREATE | os.O_WRONLY | os.O_EXCL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pathA := filepath.Join(cluster.mountPoints[0], tc.name)
			pathB := filepath.Join(cluster.mountPoints[1], tc.name)

			fa, err := os.OpenFile(pathA, tc.flags, 0644)
			require.NoError(t, err)
			fb, err := os.OpenFile(pathB, tc.flags, 0644)
			if err != nil {
				// Refusing the second exclusive create up front is also correct.
				require.NoError(t, fa.Close())
				require.True(t, tc.flags&os.O_EXCL != 0 && errors.Is(err, os.ErrExist), "open on mount 1: %v", err)
				return
			}
			_, err = fa.Write(wantA)
			require.NoError(t, err)
			_, err = fb.Write(wantB)
			require.NoError(t, err)

			require.NoError(t, fa.Close())
			// Give mount 0's create time to reach mount 1 before it closes.
			time.Sleep(2 * time.Second)
			closeB := fb.Close()
			t.Logf("close on mount 1: %v", closeB)

			want, which := wantB, "mount 1's close succeeded, so its"
			if closeB != nil {
				want, which = wantA, "mount 1's close failed, so mount 0's"
			}
			require.Eventually(t, func() bool {
				for _, p := range []string{pathA, pathB} {
					got, err := os.ReadFile(p)
					if err != nil || !bytes.Equal(got, want) {
						return false
					}
				}
				return true
			}, 10*time.Second, 200*time.Millisecond, "%s bytes must be the file's content on both mounts", which)
		})
	}
}
