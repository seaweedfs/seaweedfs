//go:build darwin || freebsd || linux

package command

import (
	"fmt"
	"strings"
	"testing"
)

func TestRunFuseDoesNotSkipOptionAfterConcurrentWriters(t *testing.T) {
	oldUmask := *mountOptions.umaskString
	oldWriters := *mountOptions.concurrentWriters
	oldReaders := *mountOptions.concurrentReaders
	oldFuseCommandPid := mountOptions.fuseCommandPid
	oldDir := *mountOptions.dir
	defer func() {
		*mountOptions.umaskString = oldUmask
		*mountOptions.concurrentWriters = oldWriters
		*mountOptions.concurrentReaders = oldReaders
		mountOptions.fuseCommandPid = oldFuseCommandPid
		*mountOptions.dir = oldDir

		recovered := recover()
		if recovered == nil {
			t.Fatal("expected invalid concurrentReaders option to be parsed")
		}
		if !strings.Contains(fmt.Sprint(recovered), "concurrentReaders") {
			t.Fatalf("expected concurrentReaders parse error, got %v", recovered)
		}
	}()

	runFuse(cmdMount, []string{
		"/mnt",
		"-o",
		"child=1,concurrentWriters=2,concurrentReaders=bad,umask=bad",
	})
}
