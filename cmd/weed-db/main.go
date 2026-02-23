package main

import (
	"os"

	"github.com/seaweedfs/seaweedfs/weed/dbcmd"
)

func main() {
	os.Exit(dbcmd.Run(os.Args[1:]))
}
