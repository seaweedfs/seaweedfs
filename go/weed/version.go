package main

import (
	"fmt"
	"runtime"

	"github.com/mcqueenorama/weed-fs/go/util"
)

var cmdVersion = &Command{
	Run:       runVersion,
	UsageLine: "version",
	Short:     "print Seaweed File System version",
	Long:      `Version prints the Seaweed File System version`,
}

func runVersion(cmd *Command, args []string) bool {
	if len(args) != 0 {
		cmd.Usage()
	}

	fmt.Printf("version %s %s %s\n", util.VERSION, runtime.GOOS, runtime.GOARCH)
	return true
}
