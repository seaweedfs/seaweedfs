package command

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/mount"
	"github.com/chrislusf/seaweedfs/weed/mount/unmount"
	"github.com/hanwen/go-fuse/v2/fs"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/grace"
)

func runMount2(cmd *Command, args []string) bool {

	if *mountOptions.debug {
		go http.ListenAndServe(fmt.Sprintf(":%d", *mountOptions.debugPort), nil)
	}

	grace.SetupProfiling(*mountCpuProfile, *mountMemProfile)
	if *mountReadRetryTime < time.Second {
		*mountReadRetryTime = time.Second
	}
	util.RetryWaitTime = *mountReadRetryTime

	umask, umaskErr := strconv.ParseUint(*mountOptions.umaskString, 8, 64)
	if umaskErr != nil {
		fmt.Printf("can not parse umask %s", *mountOptions.umaskString)
		return false
	}

	if len(args) > 0 {
		return false
	}

	return RunMount2(&mount2Options, os.FileMode(umask))
}

func RunMount2(option *Mount2Options, umask os.FileMode) bool {

	opts := &fs.Options{}
	opts.Debug = true

	unmount.Unmount(*option.dir)
	grace.OnInterrupt(func() {
		unmount.Unmount(*option.dir)
	})

	server, err := fs.Mount(*option.dir, &mount.WeedFS{}, opts)
	if err != nil {
		glog.Fatalf("Mount fail: %v", err)
	}
	server.Wait()

	return true
}
