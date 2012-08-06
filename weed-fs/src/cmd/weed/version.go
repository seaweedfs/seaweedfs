// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"runtime"
)

var cmdVersion = &Command{
	Run:       runVersion,
	UsageLine: "version",
	Short:     "print Weed File System version",
	Long:      `Version prints the Weed File System version`,
}

func runVersion(cmd *Command, args []string) {
	if len(args) != 0 {
		cmd.Usage()
	}

	fmt.Printf("version 0.15 %s %s\n",runtime.GOOS, runtime.GOARCH)
}
