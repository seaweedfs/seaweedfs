package command

import (
	"fmt"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/shell"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	shellOptions      shell.ShellOptions
	shellInitialFiler *string
	shellCluster      *string
	shellDebug        *bool
)

func init() {
	cmdShell.Run = runShell // break init cycle
	shellOptions.Masters = cmdShell.Flag.String("master", "", "comma-separated master servers, e.g. localhost:9333")
	shellOptions.FilerGroup = cmdShell.Flag.String("filerGroup", "", "filerGroup for the filers")
	shellInitialFiler = cmdShell.Flag.String("filer", "", "filer host and port for initial connection, e.g. localhost:8888")
	shellCluster = cmdShell.Flag.String("cluster", "", "cluster defined in shell.toml")
	shellDebug = cmdShell.Flag.Bool("debug", false, "print informational logs to stderr")
}

var cmdShell = &Command{
	UsageLine: "shell",
	Short:     "run interactive administrative commands",
	Long: `run interactive administrative commands.

	Generate shell.toml via "weed scaffold -config=shell"

`,
}

func runShell(command *Command, args []string) bool {

	util.LoadSecurityConfiguration()
	shellOptions.GrpcDialOption = security.LoadClientTLS(util.GetViper(), "grpc.client")
	shellOptions.Directory = "/"

	util.LoadConfiguration("shell", false)
	viper := util.GetViper()
	cluster := viper.GetString("cluster.default")
	if *shellCluster != "" {
		cluster = *shellCluster
	}

	if *shellOptions.Masters == "" {
		if cluster == "" {
			*shellOptions.Masters = "localhost:9333"
		} else {
			*shellOptions.Masters = viper.GetString("cluster." + cluster + ".master")
		}
	}

	filerAddress := *shellInitialFiler
	if filerAddress == "" && cluster != "" {
		filerAddress = viper.GetString("cluster." + cluster + ".filer")
	}
	shellOptions.FilerAddress = pb.ServerAddress(filerAddress)
	shellOptions.Debug = *shellDebug
	if shellOptions.Debug {
		fmt.Fprintf(os.Stderr, "master: %s filer: %s\n", *shellOptions.Masters, shellOptions.FilerAddress)
	}

	if err := shell.RunShell(shellOptions); err != nil {
		// Non-interactive (piped) mode surfaced a command failure. The command
		// already printed "error: ..."; exit non-zero so scripts and CronJobs
		// see the failure -- previously a run that aborted partway (e.g.
		// s3.lifecycle.run-shard dying mid-walk) still exited 0 and schedulers
		// reported it green. Recorded rather than os.Exit'ed so the process
		// still goes through main's shutdown path (atexit hooks, sentry flush).
		SetCommandExitStatus(2)
	}

	return true

}
