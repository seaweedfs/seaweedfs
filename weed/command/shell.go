package command

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/shell"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/spf13/viper"
)

var (
	shellOptions          shell.ShellOptions
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
	shellOptions.GrpcDialOption = security.LoadClientTLS(viper.Sub("grpc"), "client")


	var filerPwdErr error
	shellOptions.FilerHost, shellOptions.FilerPort, shellOptions.Directory, filerPwdErr = parseFilerUrl(*shellInitialFilerUrl)
	if filerPwdErr != nil {
		fmt.Printf("failed to parse url filer.url=%s : %v\n", *shellInitialFilerUrl, filerPwdErr)
		return false
	}

	shell.RunShell(shellOptions)

	return true

}

func parseFilerUrl(entryPath string) (filerServer string, filerPort int64, path string, err error) {
	if !strings.HasPrefix(entryPath, "http://") && !strings.HasPrefix(entryPath, "https://") {
		entryPath = "http://" + entryPath
	}

	var u *url.URL
	u, err = url.Parse(entryPath)
	if err != nil {
		return
	}
	filerServer = u.Hostname()
	portString := u.Port()
	if portString != "" {
		filerPort, err = strconv.ParseInt(portString, 10, 32)
	}
	path = u.Path
	return
}
