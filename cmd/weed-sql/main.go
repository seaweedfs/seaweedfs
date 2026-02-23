package main

import (
	"os"

	"github.com/seaweedfs/seaweedfs/weed/sqlcmd"
)

func main() {
	os.Exit(sqlcmd.Run(os.Args[1:]))
}
