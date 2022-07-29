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
	shellInitialFiler = cmdShell.Flag.String("filer", "", "filer host and port, e.g. localhost:8888")
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

	util.LoadConfiguration("security", false)
	shellOptions.GrpcDialOption = security.LoadClientTLS(util.GetViper(), "grpc.client")

	if *shellOptions.Masters == "" {
		util.LoadConfiguration("shell", false)
		v := util.GetViper()
		cluster := v.GetString("cluster.default")
		if *shellCluster != "" {
			cluster = *shellCluster
		}
		if cluster == "" {
			*shellOptions.Masters = "localhost:9333"
		} else {
			*shellOptions.Masters = v.GetString("cluster." + cluster + ".master")
			*shellInitialFiler = v.GetString("cluster." + cluster + ".filer")
			fmt.Printf("master: %s filer: %s\n", *shellOptions.Masters, *shellInitialFiler)
		}
	}

	shellOptions.FilerAddress = pb.ServerAddress(*shellInitialFiler)
	shellOptions.Directory = "/"

	shell.RunShell(shellOptions)

	return true

}
