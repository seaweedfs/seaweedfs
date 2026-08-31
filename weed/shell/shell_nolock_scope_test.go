package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/wdclient/exclusive_locks"
)

// noLock says "this invocation changes nothing" -- volume.balance,
// volume.fix.replication and others set it for a dry run. It is a property of
// the invocation, not of the session: the CommandEnv is reused across every
// command, so without a reset a simulation leaves later commands unlocked and a
// real mutation run straight after one skips its lock silently.
func TestNoLockDoesNotOutliveTheCommandThatSetIt(t *testing.T) {
	env := &CommandEnv{locker: exclusive_locks.NewExclusiveLocker(nil, cluster.AdminShellLockName)}

	env.SetNoLock(true)
	if err := env.confirmIsLocked([]string{"volume.balance"}); err != nil {
		t.Fatalf("a dry run needs no lock: %v", err)
	}

	env.SetNoLock(false)

	if err := env.confirmIsLocked([]string{"volume.move"}); err == nil {
		t.Error("the next command mutates and must require the lock again")
	}
}

// Both dispatchers reuse one CommandEnv: the interactive shell across the lines
// an operator types, and the master's maintenance script runner across the lines
// of a script. Resetting has to restore the lock requirement however many
// invocations share the environment.
func TestNoLockResetRestoresTheRequirementForEveryInvocation(t *testing.T) {
	env := &CommandEnv{locker: exclusive_locks.NewExclusiveLocker(nil, cluster.AdminShellLockName)}

	for i := 0; i < 3; i++ {
		env.SetNoLock(true)
		if err := env.confirmIsLocked([]string{"dry run"}); err != nil {
			t.Fatalf("run %d: a dry run needs no lock: %v", i, err)
		}

		env.SetNoLock(false)
		if err := env.confirmIsLocked([]string{"mutation"}); err == nil {
			t.Fatalf("run %d: the mutation after it must require the lock again", i)
		}
	}
}
