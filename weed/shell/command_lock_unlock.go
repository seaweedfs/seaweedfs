package shell

import (
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
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

	commandEnv.locker.RequestLock(util.DetectedHostAddress())

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
