package command

type MountOptions struct {
	filer            *string
	filerGrpcPort    *int
	dir              *string
	collection       *string
	replication      *string
	ttlSec           *int
	chunkSizeLimitMB *int
}

var (
	mountOptions MountOptions
)

func init() {
	cmdMount.Run = runMount // break init cycle
	mountOptions.filer = cmdMount.Flag.String("filer", "localhost:8888", "weed filer location")
	mountOptions.filerGrpcPort = cmdMount.Flag.Int("filer.grpc.port", 0, "filer grpc server listen port, default to http port + 10000")
	mountOptions.dir = cmdMount.Flag.String("dir", ".", "mount weed filer to this directory")
	mountOptions.collection = cmdMount.Flag.String("collection", "", "collection to create the files")
	mountOptions.replication = cmdMount.Flag.String("replication", "000", "replication to create to files")
	mountOptions.ttlSec = cmdMount.Flag.Int("ttl", 0, "file ttl in seconds")
	mountOptions.chunkSizeLimitMB = cmdMount.Flag.Int("chunkSizeLimitMB", 16, "local write buffer size, also chunk large files")
}

var cmdMount = &Command{
	UsageLine: "mount -filer=localhost:8888 -dir=/some/dir",
	Short:     "mount weed filer to a directory as file system in userspace(FUSE)",
	Long: `mount weed filer to userspace.

  Pre-requisites:
  1) have SeaweedFS master and volume servers running
  2) have a "weed filer" running
  These 2 requirements can be achieved with one command "weed server -filer=true"

  This uses bazil.org/fuse, which enables writing FUSE file systems on
  Linux, and OS X.

  On OS X, it requires OSXFUSE (http://osxfuse.github.com/).

  `,
}
