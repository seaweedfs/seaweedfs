package command

import (
	"fmt"
	"runtime"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

var cmdVersion = &Command{
	Run:       runVersion,
	UsageLine: "version",
	Short:     "print SeaweedFS version",
	Long:      `Version prints the SeaweedFS version`,
}

func runVersion(cmd *Command, args []string) bool {
	if len(args) != 0 {
		cmd.Usage()
	}

	fmt.Printf("version %s %s %s\n", util.Version(), runtime.GOOS, runtime.GOARCH)
	return true
}
