package command

import (
	"os"
	"time"
)

type MountOptions struct {
	filer              *string
	filerMountRootPath *string
	dir                *string
	dirAutoCreate      *bool
	collection         *string
	collectionQuota    *int
	replication        *string
	diskType           *string
	ttlSec             *int
	chunkSizeLimitMB   *int
	concurrentWriters  *int
	cacheMetaTtlSec    *int
	cacheDirForRead    *string
	cacheDirForWrite   *string
	cacheSizeMBForRead *int64
	dataCenter         *string
	allowOthers        *bool
	umaskString        *string
	nonempty           *bool
	volumeServerAccess *string
	uidMap             *string
	gidMap             *string
	readOnly           *bool
	debug              *bool
	debugPort          *int
	localSocket        *string
	disableXAttr       *bool
	extraOptions       []string
}

var (
	mountOptions       MountOptions
	mountCpuProfile    *string
	mountMemProfile    *string
	mountReadRetryTime *time.Duration
)

func init() {
	cmdMount.Run = runMount // break init cycle
	mountOptions.filer = cmdMount.Flag.String("filer", "localhost:8888", "comma-separated weed filer location")
	mountOptions.filerMountRootPath = cmdMount.Flag.String("filer.path", "/", "mount this remote path from filer server")
	mountOptions.dir = cmdMount.Flag.String("dir", ".", "mount weed filer to this directory")
	mountOptions.dirAutoCreate = cmdMount.Flag.Bool("dirAutoCreate", false, "auto create the directory to mount to")
	mountOptions.collection = cmdMount.Flag.String("collection", "", "collection to create the files")
	mountOptions.collectionQuota = cmdMount.Flag.Int("collectionQuotaMB", 0, "quota for the collection")
	mountOptions.replication = cmdMount.Flag.String("replication", "", "replication(e.g. 000, 001) to create to files. If empty, let filer decide.")
	mountOptions.diskType = cmdMount.Flag.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	mountOptions.ttlSec = cmdMount.Flag.Int("ttl", 0, "file ttl in seconds")
	mountOptions.chunkSizeLimitMB = cmdMount.Flag.Int("chunkSizeLimitMB", 2, "local write buffer size, also chunk large files")
	mountOptions.concurrentWriters = cmdMount.Flag.Int("concurrentWriters", 32, "limit concurrent goroutine writers")
	mountOptions.cacheDirForRead = cmdMount.Flag.String("cacheDir", os.TempDir(), "local cache directory for file chunks and meta data")
	mountOptions.cacheSizeMBForRead = cmdMount.Flag.Int64("cacheCapacityMB", 128, "file chunk read cache capacity in MB")
	mountOptions.cacheDirForWrite = cmdMount.Flag.String("cacheDirWrite", "", "buffer writes mostly for large files")
	mountOptions.cacheMetaTtlSec = cmdMount.Flag.Int("cacheMetaTtlSec", 60, "metadata cache validity seconds")
	mountOptions.dataCenter = cmdMount.Flag.String("dataCenter", "", "prefer to write to the data center")
	mountOptions.allowOthers = cmdMount.Flag.Bool("allowOthers", true, "allows other users to access the file system")
	mountOptions.umaskString = cmdMount.Flag.String("umask", "022", "octal umask, e.g., 022, 0111")
	mountOptions.nonempty = cmdMount.Flag.Bool("nonempty", false, "allows the mounting over a non-empty directory")
	mountOptions.volumeServerAccess = cmdMount.Flag.String("volumeServerAccess", "direct", "access volume servers by [direct|publicUrl|filerProxy]")
	mountOptions.uidMap = cmdMount.Flag.String("map.uid", "", "map local uid to uid on filer, comma-separated <local_uid>:<filer_uid>")
	mountOptions.gidMap = cmdMount.Flag.String("map.gid", "", "map local gid to gid on filer, comma-separated <local_gid>:<filer_gid>")
	mountOptions.readOnly = cmdMount.Flag.Bool("readOnly", false, "read only")
	mountOptions.debug = cmdMount.Flag.Bool("debug", false, "serves runtime profiling data, e.g., http://localhost:<debug.port>/debug/pprof/goroutine?debug=2")
	mountOptions.debugPort = cmdMount.Flag.Int("debug.port", 6061, "http port for debugging")
	mountOptions.localSocket = cmdMount.Flag.String("localSocket", "", "default to /tmp/seaweedfs-mount-<mount_dir_hash>.sock")
	mountOptions.disableXAttr = cmdMount.Flag.Bool("disableXAttr", false, "disable xattr")

	mountCpuProfile = cmdMount.Flag.String("cpuprofile", "", "cpu profile output file")
	mountMemProfile = cmdMount.Flag.String("memprofile", "", "memory profile output file")
	mountReadRetryTime = cmdMount.Flag.Duration("readRetryTime", 6*time.Second, "maximum read retry wait time")
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

  On OS X, it requires OSXFUSE (https://osxfuse.github.io/).

  `,
}
