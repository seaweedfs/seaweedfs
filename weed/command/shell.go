package command

import (
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/shell"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/spf13/viper"
)

var (
	shellOptions shell.ShellOptions
)

func init() {
	cmdShell.Run = runShell // break init cycle
	shellOptions.Masters = cmdShell.Flag.String("master", "localhost:9333", "comma-separated master servers")
	filerHost := cmdShell.Flag.String("filer.host", "localhost", "comma-separated filer server host")
	flierPort := cmdShell.Flag.Int64("filer.port", 9333, "comma-separated filer server port")
	directory := cmdShell.Flag.String("filer.dir", "/", "comma-separated filer server directory")
	shellOptions.FilerHost = *filerHost
	shellOptions.FilerPort = *flierPort
	shellOptions.Directory = *directory
}

var cmdShell = &Command{
	UsageLine: "shell",
	Short:     "run interactive administrative commands",
	Long: `run interactive administrative commands.

  `,
}


func runShell(command *Command, args []string) bool {

	util.LoadConfiguration("security", false)
	shellOptions.GrpcDialOption = security.LoadClientTLS(viper.Sub("grpc"), "client")

	shell.RunShell(shellOptions)

	return true

}