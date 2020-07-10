package command

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/shell"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	shellOptions      shell.ShellOptions
	shellInitialFiler *string
)

func init() {
	cmdShell.Run = runShell // break init cycle
	shellOptions.Masters = cmdShell.Flag.String("master", "localhost:9333", "comma-separated master servers")
	shellInitialFiler = cmdShell.Flag.String("filer", "localhost:8888", "filer host and port")
}

var cmdShell = &Command{
	UsageLine: "shell",
	Short:     "run interactive administrative commands",
	Long: `run interactive administrative commands.

  `,
}

func runShell(command *Command, args []string) bool {

	util.LoadConfiguration("security", false)
	shellOptions.GrpcDialOption = security.LoadClientTLS(util.GetViper(), "grpc.client")

	var err error
	shellOptions.FilerHost, shellOptions.FilerPort, err = util.ParseHostPort(*shellInitialFiler)
	if err != nil {
		fmt.Printf("failed to parse filer %s: %v\n", *shellInitialFiler, err)
		return false
	}
	shellOptions.Directory = "/"

	shell.RunShell(shellOptions)

	return true

}
