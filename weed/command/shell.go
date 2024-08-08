package command

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/shell"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	shellOptions      shell.ShellOptions
	shellInitialFiler *string
	shellCluster      *string
)

func init() {
	cmdShell.Run = runShell // break init cycle
	shellOptions.Masters = cmdShell.Flag.String("master", "", "comma-separated master servers, e.g. localhost:9333")
	shellOptions.FilerGroup = cmdShell.Flag.String("filerGroup", "", "filerGroup for the filers")
	shellInitialFiler = cmdShell.Flag.String("filer", "", "filer host and port for initial connection, e.g. localhost:8888")
	shellCluster = cmdShell.Flag.String("cluster", "", "cluster defined in shell.toml")
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
	fmt.Printf("master: %s filer: %s\n", *shellOptions.Masters, shellOptions.FilerAddress)

	shell.RunShell(shellOptions)

	return true

}
