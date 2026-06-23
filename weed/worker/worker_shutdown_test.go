package worker

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// A task goroutine can outlive a forced shutdown: Stop() only drains for 30s,
// then terminates the manager loop while the task is still running. When the
// task finally reports completion it goes through w.cmds (getAdmin in
// completeTask, the ActionIncTask* send, removeTask). With the loop gone and an
// unbuffered channel, those sends - and the response reads behind getAdmin /
// getTaskLoad - block forever, leaking the goroutine. They must abort instead.
func TestManagerLoopSendersAbortAfterStop(t *testing.T) {
	w, err := NewWorker(&types.WorkerConfig{
		BaseWorkingDir: t.TempDir(),
		MaxConcurrent:  2,
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	// No Start(): the drain loop sees zero load and ActionStop terminates the
	// manager loop immediately, modelling the post-shutdown state a lingering
	// task goroutine would race into.
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Every interaction a lingering executeTask goroutine has with the manager
	// loop must return promptly now that the loop no longer receives.
	mustNotBlock(t, "getAdmin", func() { w.getAdmin() })
	mustNotBlock(t, "getTaskLoad", func() { w.getTaskLoad() })
	mustNotBlock(t, "removeTask", func() { w.removeTask(&types.TaskInput{ID: "t1"}) })
	mustNotBlock(t, "completeTask", func() { w.completeTask("t1", true, "") })
	mustNotBlock(t, "dispatch", func() { w.dispatch(workerCommand{action: ActionIncTaskComplete}) })

	// The abort path returns zero values rather than blocking on real state.
	if got := w.getAdmin(); got != nil {
		t.Errorf("getAdmin after stop = %v, want nil", got)
	}
	if got := w.getTaskLoad(); got != 0 {
		t.Errorf("getTaskLoad after stop = %d, want 0", got)
	}
	if w.dispatch(workerCommand{action: ActionIncTaskComplete}) {
		t.Errorf("dispatch after stop = true, want false")
	}
}

func mustNotBlock(t *testing.T, name string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s blocked after manager loop stopped (goroutine leak)", name)
	}
}
