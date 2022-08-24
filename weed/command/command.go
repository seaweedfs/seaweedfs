package command

import (
	"fmt"
	"os"
	"strings"

	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"
)

var Commands = []*Command{
	cmdAutocomplete,
	cmdUnautocomplete,
	cmdBackup,
	cmdBenchmark,
	cmdCompact,
	cmdDownload,
	cmdExport,
	cmdFiler,
	cmdFilerBackup,
	cmdFilerCat,
	cmdFilerCopy,
	cmdFilerMetaBackup,
	cmdFilerMetaTail,
	cmdFilerRemoteGateway,
	cmdFilerRemoteSynchronize,
	cmdFilerReplicate,
	cmdFilerSynchronize,
	cmdFix,
	cmdFuse,
	cmdIam,
	cmdMaster,
	cmdMasterFollower,
	cmdMount,
	cmdMqBroker,
	cmdS3,
	cmdScaffold,
	cmdServer,
	cmdShell,
	cmdUpdate,
	cmdUpload,
	cmdVersion,
	cmdVolume,
	cmdWebDav,
}

type Command struct {
	// Run runs the command.
	// The args are the arguments after the command name.
	Run func(cmd *Command, args []string) bool

	// UsageLine is the one-line usage message.
	// The first word in the line is taken to be the command name.
	UsageLine string

	// Short is the short description shown in the 'go help' output.
	Short string

	// Long is the long message shown in the 'go help <this-command>' output.
	Long string

	// Flag is a set of flags specific to this command.
	Flag flag.FlagSet

	IsDebug *bool
}

// Name returns the command's name: the first word in the usage line.
func (c *Command) Name() string {
	name := c.UsageLine
	i := strings.Index(name, " ")
	if i >= 0 {
		name = name[:i]
	}
	return name
}

func (c *Command) Usage() {
	fmt.Fprintf(os.Stderr, "Example: weed %s\n", c.UsageLine)
	fmt.Fprintf(os.Stderr, "Default Usage:\n")
	c.Flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "Description:\n")
	fmt.Fprintf(os.Stderr, "  %s\n", strings.TrimSpace(c.Long))
	os.Exit(2)
}

// Runnable reports whether the command can be run; otherwise
// it is a documentation pseudo-command such as importpath.
func (c *Command) Runnable() bool {
	return c.Run != nil
}
