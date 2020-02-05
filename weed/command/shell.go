package command

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/shell"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	shellOptions         shell.ShellOptions
	shellInitialFilerUrl *string
)

func init() {
	cmdShell.Run = runShell // break init cycle
	shellOptions.Masters = cmdShell.Flag.String("master", "localhost:9333", "comma-separated master servers")
	shellInitialFilerUrl = cmdShell.Flag.String("filer.url", "http://localhost:8888/", "initial filer url")
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

	var filerPwdErr error
	shellOptions.FilerHost, shellOptions.FilerPort, shellOptions.Directory, filerPwdErr = util.ParseFilerUrl(*shellInitialFilerUrl)
	if filerPwdErr != nil {
		fmt.Printf("failed to parse url filer.url=%s : %v\n", *shellInitialFilerUrl, filerPwdErr)
		return false
	}

	shell.RunShell(shellOptions)

	return true

}
