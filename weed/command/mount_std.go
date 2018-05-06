// +build linux darwin

package command

import (
	"fmt"
	"runtime"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/filesys"
)

func runMount(cmd *Command, args []string) bool {
	fmt.Printf("This is SeaweedFS version %s %s %s\n", util.VERSION, runtime.GOOS, runtime.GOARCH)
	if *mountOptions.dir == "" {
		fmt.Printf("Please specify the mount directory via \"-dir\"")
		return false
	}

	fuse.Unmount(*mountOptions.dir)

	c, err := fuse.Mount(*mountOptions.dir, fuse.LocalVolume())
	if err != nil {
		glog.Fatal(err)
		return false
	}

	util.OnInterrupt(func() {
		fuse.Unmount(*mountOptions.dir)
		c.Close()
	})

	err = fs.Serve(c, filesys.NewSeaweedFileSystem(*mountOptions.filer))
	if err != nil {
		fuse.Unmount(*mountOptions.dir)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		glog.Fatal(err)
	}

	return true
}
