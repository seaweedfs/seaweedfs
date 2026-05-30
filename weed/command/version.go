package command

import (
	"fmt"
	"runtime"

	"github.com/seaweedfs/seaweedfs/weed/util/version"
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

	fmt.Printf("version %s %s %s\n", version.Version(), runtime.GOOS, runtime.GOARCH)
	println()
	println("For enterprise users, please visit https://seaweedfs.com for the SeaweedFS Enterprise Edition,")
	println("which has advanced features, including data recovery, self-healing storage, customizable erasure coding, EC vacuum and repair, etc.")
	return true
}
