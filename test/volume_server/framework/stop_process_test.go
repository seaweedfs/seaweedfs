package framework

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

type errorRecorder struct {
	testing.TB
	errs []string
}

func (r *errorRecorder) Errorf(format string, args ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

func TestStopProcessReportsTheThreadsOfAServerIgnoringSIGINT(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("thread states come from /proc")
	}
	defer func(d time.Duration) { stopTimeout = d }(stopTimeout)
	stopTimeout = 200 * time.Millisecond

	cmd := exec.Command("sh", "-c", `trap "" INT; exec sleep 30`)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	rec := &errorRecorder{TB: t}
	stopProcess(rec, "server", cmd)

	if len(rec.errs) != 1 || !strings.Contains(rec.errs[0], "server did not exit within") || !strings.Contains(rec.errs[0], "1x[sleep S ") {
		t.Fatalf("errors = %q", rec.errs)
	}
}

func TestProcessThreadsFindsNoDiskWaitInAnIdleProcess(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("thread states come from /proc")
	}
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	time.Sleep(100 * time.Millisecond)

	threads, inDiskWait := processThreads(cmd.Process.Pid)
	if inDiskWait || !strings.HasPrefix(threads, "1x[sleep S ") {
		t.Fatalf("threads = %q, inDiskWait = %v", threads, inDiskWait)
	}
}
