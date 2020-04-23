package command

import (
	"os"
)

type MountOptions struct {
	filer                       *string
	filerMountRootPath          *string
	dir                         *string
	dirListCacheLimit           *int64
	collection                  *string
	replication                 *string
	ttlSec                      *int
	chunkSizeLimitMB            *int
	cacheDir                    *string
	cacheSizeMB                 *int64
	dataCenter                  *string
	allowOthers                 *bool
	umaskString                 *string
	nonempty                    *bool
	outsideContainerClusterMode *bool
	asyncMetaDataCaching        *bool
}

var (
	mountOptions    MountOptions
	mountCpuProfile *string
	mountMemProfile *string
)

func init() {
	cmdMount.Run = runMount // break init cycle
	mountOptions.filer = cmdMount.Flag.String("filer", "localhost:8888", "weed filer location")
	mountOptions.filerMountRootPath = cmdMount.Flag.String("filer.path", "/", "mount this remote path from filer server")
	mountOptions.dir = cmdMount.Flag.String("dir", ".", "mount weed filer to this directory")
	mountOptions.dirListCacheLimit = cmdMount.Flag.Int64("dirListCacheLimit", 1000000, "limit cache size to speed up directory long format listing")
	mountOptions.collection = cmdMount.Flag.String("collection", "", "collection to create the files")
	mountOptions.replication = cmdMount.Flag.String("replication", "", "replication(e.g. 000, 001) to create to files. If empty, let filer decide.")
	mountOptions.ttlSec = cmdMount.Flag.Int("ttl", 0, "file ttl in seconds")
	mountOptions.chunkSizeLimitMB = cmdMount.Flag.Int("chunkSizeLimitMB", 16, "local write buffer size, also chunk large files")
	mountOptions.cacheDir = cmdMount.Flag.String("cacheDir", os.TempDir(), "local cache directory for file chunks")
	mountOptions.cacheSizeMB = cmdMount.Flag.Int64("cacheCapacityMB", 1000, "local cache capacity in MB (0 will disable cache)")
	mountOptions.dataCenter = cmdMount.Flag.String("dataCenter", "", "prefer to write to the data center")
	mountOptions.allowOthers = cmdMount.Flag.Bool("allowOthers", true, "allows other users to access the file system")
	mountOptions.umaskString = cmdMount.Flag.String("umask", "022", "octal umask, e.g., 022, 0111")
	mountOptions.nonempty = cmdMount.Flag.Bool("nonempty", false, "allows the mounting over a non-empty directory")
	mountCpuProfile = cmdMount.Flag.String("cpuprofile", "", "cpu profile output file")
	mountMemProfile = cmdMount.Flag.String("memprofile", "", "memory profile output file")
	mountOptions.outsideContainerClusterMode = cmdMount.Flag.Bool("outsideContainerClusterMode", false, "allows other users to access the file system")
	mountOptions.asyncMetaDataCaching = cmdMount.Flag.Bool("asyncMetaDataCaching", true, "async meta data caching. this feature will be permanent and this option will be removed.")
}

var cmdMount = &Command{
	UsageLine: "mount -filer=localhost:8888 -dir=/some/dir",
	Short:     "mount weed filer to a directory as file system in userspace(FUSE)",
	Long: `mount weed filer to userspace.

  Pre-requisites:
  1) have SeaweedFS master and volume servers running
  2) have a "weed filer" running
  These 2 requirements can be achieved with one command "weed server -filer=true"

  This uses github.com/seaweedfs/fuse, which enables writing FUSE file systems on
  Linux, and OS X.

  On OS X, it requires OSXFUSE (http://osxfuse.github.com/).

  If the SeaweedFS system runs in a container cluster, e.g. managed by kubernetes or docker compose,
  the volume servers are not accessible by their own ip addresses. 
  In "outsideContainerClusterMode", the mount will use the filer ip address instead, assuming:
    * All volume server containers are accessible through the same hostname or IP address as the filer.
    * All volume server container ports are open external to the cluster.

  `,
}
