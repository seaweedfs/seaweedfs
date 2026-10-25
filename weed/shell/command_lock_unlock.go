package shell

import (
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandUnlock{})
	Commands = append(Commands, &commandLock{})
}

// =========== Lock ==============
type commandLock struct {
}

func (c *commandLock) Name() string {
	return "lock"
}

func (c *commandLock) Help() string {
	return `lock in order to exclusively manage the cluster

	This is a blocking operation if there is already another lock.
`
}

func (c *commandLock) HasTag(CommandTag) bool {
	return false
}

func (c *commandLock) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	waited := false
	if !commandEnv.locker.IsLocked() {
		if holder, message, held := commandEnv.shellLockHolder(); held {
			waited = true
			if holder == "" {
				holder = "another client"
			}
			if message != "" {
				fmt.Fprintf(writer, "waiting for lock held by %s: %s\n", holder, message)
			} else {
				fmt.Fprintf(writer, "waiting for lock held by %s\n", holder)
			}
		}
	}

	commandEnv.locker.RequestLock(util.DetectedHostAddress())

	if waited {
		fmt.Fprintln(writer, "lock acquired")
	}

	return nil
}

// =========== Unlock ==============

type commandUnlock struct {
}

func (c *commandUnlock) Name() string {
	return "unlock"
}

func (c *commandUnlock) Help() string {
	return `unlock the cluster-wide lock

`
}

func (c *commandUnlock) HasTag(CommandTag) bool {
	return false
}

func (c *commandUnlock) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	commandEnv.locker.ReleaseLock()

	return nil
}
