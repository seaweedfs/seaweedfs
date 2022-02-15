package command

import (
	"os"
	"time"
)

type Mount2Options struct {
	filer              *string
	filerMountRootPath *string
	dir                *string
	dirAutoCreate      *bool
	collection         *string
	replication        *string
	diskType           *string
	ttlSec             *int
	chunkSizeLimitMB   *int
	concurrentWriters  *int
	cacheDir           *string
	cacheSizeMB        *int64
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
}

var (
	mount2Options Mount2Options
)

func init() {
	cmdMount2.Run = runMount2 // break init cycle
	mount2Options.filer = cmdMount2.Flag.String("filer", "localhost:8888", "comma-separated weed filer location")
	mount2Options.filerMountRootPath = cmdMount2.Flag.String("filer.path", "/", "mount this remote path from filer server")
	mount2Options.dir = cmdMount2.Flag.String("dir", ".", "mount weed filer to this directory")
	mount2Options.dirAutoCreate = cmdMount2.Flag.Bool("dirAutoCreate", false, "auto create the directory to mount to")
	mount2Options.collection = cmdMount2.Flag.String("collection", "", "collection to create the files")
	mount2Options.replication = cmdMount2.Flag.String("replication", "", "replication(e.g. 000, 001) to create to files. If empty, let filer decide.")
	mount2Options.diskType = cmdMount2.Flag.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	mount2Options.ttlSec = cmdMount2.Flag.Int("ttl", 0, "file ttl in seconds")
	mount2Options.chunkSizeLimitMB = cmdMount2.Flag.Int("chunkSizeLimitMB", 2, "local write buffer size, also chunk large files")
	mount2Options.concurrentWriters = cmdMount2.Flag.Int("concurrentWriters", 32, "limit concurrent goroutine writers if not 0")
	mount2Options.cacheDir = cmdMount2.Flag.String("cacheDir", os.TempDir(), "local cache directory for file chunks and meta data")
	mount2Options.cacheSizeMB = cmdMount2.Flag.Int64("cacheCapacityMB", 0, "local file chunk cache capacity in MB")
	mount2Options.dataCenter = cmdMount2.Flag.String("dataCenter", "", "prefer to write to the data center")
	mount2Options.allowOthers = cmdMount2.Flag.Bool("allowOthers", true, "allows other users to access the file system")
	mount2Options.umaskString = cmdMount2.Flag.String("umask", "022", "octal umask, e.g., 022, 0111")
	mount2Options.nonempty = cmdMount2.Flag.Bool("nonempty", false, "allows the mounting over a non-empty directory")
	mount2Options.volumeServerAccess = cmdMount2.Flag.String("volumeServerAccess", "direct", "access volume servers by [direct|publicUrl|filerProxy]")
	mount2Options.uidMap = cmdMount2.Flag.String("map.uid", "", "map local uid to uid on filer, comma-separated <local_uid>:<filer_uid>")
	mount2Options.gidMap = cmdMount2.Flag.String("map.gid", "", "map local gid to gid on filer, comma-separated <local_gid>:<filer_gid>")
	mount2Options.readOnly = cmdMount2.Flag.Bool("readOnly", false, "read only")
	mount2Options.debug = cmdMount2.Flag.Bool("debug", false, "serves runtime profiling data, e.g., http://localhost:<debug.port>/debug/pprof/goroutine?debug=2")
	mount2Options.debugPort = cmdMount2.Flag.Int("debug.port", 6061, "http port for debugging")

	mountCpuProfile = cmdMount2.Flag.String("cpuprofile", "", "cpu profile output file")
	mountMemProfile = cmdMount2.Flag.String("memprofile", "", "memory profile output file")
	mountReadRetryTime = cmdMount2.Flag.Duration("readRetryTime", 6*time.Second, "maximum read retry wait time")
}

var cmdMount2 = &Command{
	UsageLine: "mount2 -filer=localhost:8888 -dir=/some/dir",
	Short:     "<WIP> mount weed filer to a directory as file system in userspace(FUSE)",
	Long: `mount weed filer to userspace.

  Pre-requisites:
  1) have SeaweedFS master and volume servers running
  2) have a "weed filer" running
  These 2 requirements can be achieved with one command "weed server -filer=true"

  This uses github.com/seaweedfs/fuse, which enables writing FUSE file systems on
  Linux, and OS X.

  On OS X, it requires OSXFUSE (http://osxfuse.github.com/).

  `,
}
