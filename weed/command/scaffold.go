package command

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/util"
	"path/filepath"

	"github.com/chrislusf/seaweedfs/weed/command/scaffold"
)

func init() {
	cmdScaffold.Run = runScaffold // break init cycle
}

var cmdScaffold = &Command{
	UsageLine: "scaffold -config=[filer|notification|replication|security|master]",
	Short:     "generate basic configuration files",
	Long: `Generate filer.toml with all possible configurations for you to customize.

	The options can also be overwritten by environment variables.
	For example, the filer.toml mysql password can be overwritten by environment variable
		export WEED_MYSQL_PASSWORD=some_password
	Environment variable rules:
		* Prefix the variable name with "WEED_"
		* Upppercase the reset of variable name.
		* Replace '.' with '_'

  `,
}

var (
	outputPath = cmdScaffold.Flag.String("output", "", "if not empty, save the configuration file to this directory")
	config     = cmdScaffold.Flag.String("config", "filer", "[filer|notification|replication|security|master] the configuration file to generate")
)

func runScaffold(cmd *Command, args []string) bool {

	content := ""
	switch *config {
	case "filer":
		content = scaffold.Filer
	case "notification":
		content = scaffold.Notification
	case "replication":
		content = scaffold.Replication
	case "security":
		content = scaffold.Security
	case "master":
		content = scaffold.Master
	case "shell":
		content = scaffold.Shell
	}
	if content == "" {
		println("need a valid -config option")
		return false
	}

	if *outputPath != "" {
		util.WriteFile(filepath.Join(*outputPath, *config+".toml"), []byte(content), 0644)
	} else {
		fmt.Println(content)
	}
	return true
}
