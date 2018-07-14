// +build linux darwin

package command

import (
	"fmt"
	"runtime"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/chrislusf/seaweedfs/weed/filesys"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"strings"
	"strconv"
)

func runMount(cmd *Command, args []string) bool {
	fmt.Printf("This is SeaweedFS version %s %s %s\n", util.VERSION, runtime.GOOS, runtime.GOARCH)
	if *mountOptions.dir == "" {
		fmt.Printf("Please specify the mount directory via \"-dir\"")
		return false
	}
	if *mountOptions.chunkSizeLimitMB <= 0 {
		fmt.Printf("Please specify a reasonable buffer size.")
		return false
	}

	fuse.Unmount(*mountOptions.dir)

	c, err := fuse.Mount(
		*mountOptions.dir,
		fuse.VolumeName("SeaweedFS"),
		fuse.FSName("SeaweedFS"),
		fuse.NoAppleDouble(),
		fuse.NoAppleXattr(),
		fuse.ExclCreate(),
		fuse.DaemonTimeout("3600"),
		fuse.AllowOther(),
		fuse.AllowSUID(),
		fuse.DefaultPermissions(),
		// fuse.MaxReadahead(1024*128), // TODO: not tested yet, possibly improving read performance
		fuse.AsyncRead(),
		fuse.WritebackCache(),
	)
	if err != nil {
		glog.Fatal(err)
		return false
	}

	util.OnInterrupt(func() {
		fuse.Unmount(*mountOptions.dir)
		c.Close()
	})

	hostnameAndPort := strings.Split(*mountOptions.filer, ":")
	if len(hostnameAndPort) != 2 {
		fmt.Printf("The filer should have hostname:port format: %v\n", hostnameAndPort)
		return false
	}

	filerPort, parseErr := strconv.ParseUint(hostnameAndPort[1], 10, 64)
	if parseErr != nil {
		fmt.Printf("The filer filer port parse error: %v\n", parseErr)
		return false
	}

	filerGrpcPort := filerPort + 10000
	if *mountOptions.filerGrpcPort != 0 {
		filerGrpcPort = uint64(*copy.filerGrpcPort)
	}

	filerAddress := fmt.Sprintf("%s:%d", hostnameAndPort[0], filerGrpcPort)

	err = fs.Serve(c, filesys.NewSeaweedFileSystem(
		filerAddress, *mountOptions.collection, *mountOptions.replication, int32(*mountOptions.ttlSec),
		*mountOptions.chunkSizeLimitMB, *mountOptions.dataCenter))
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
