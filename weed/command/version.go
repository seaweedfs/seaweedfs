package command

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"runtime"
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
	println("For enterprise users, please visit https://seaweedfs.com for SeaweedFS Enterprise Edition,")
	println("which has a self-healing storage format with better data protection.")
	return true
}
