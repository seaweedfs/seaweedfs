package command

import (
	"fmt"
	"strconv"
	"strings"
)

type MountOptions struct {
	filer              *string
	filerGrpcPort      *int
	filerMountRootPath *string
	dir                *string
	dirListingLimit    *int
	collection         *string
	replication        *string
	ttlSec             *int
	chunkSizeLimitMB   *int
	dataCenter         *string
}

var (
	mountOptions    MountOptions
	mountCpuProfile *string
	mountMemProfile *string
)

func init() {
	cmdMount.Run = runMount // break init cycle
	mountOptions.filer = cmdMount.Flag.String("filer", "localhost:8888", "weed filer location")
	mountOptions.filerGrpcPort = cmdMount.Flag.Int("filer.grpc.port", 0, "filer grpc server listen port, default to http port + 10000")
	mountOptions.filerMountRootPath = cmdMount.Flag.String("filer.path", "/", "mount this remote path from filer server")
	mountOptions.dir = cmdMount.Flag.String("dir", ".", "mount weed filer to this directory")
	mountOptions.dirListingLimit = cmdMount.Flag.Int("dirListLimit", 100000, "limit directory listing size")
	mountOptions.collection = cmdMount.Flag.String("collection", "", "collection to create the files")
	mountOptions.replication = cmdMount.Flag.String("replication", "", "replication(e.g. 000, 001) to create to files. If empty, let filer decide.")
	mountOptions.ttlSec = cmdMount.Flag.Int("ttl", 0, "file ttl in seconds")
	mountOptions.chunkSizeLimitMB = cmdMount.Flag.Int("chunkSizeLimitMB", 4, "local write buffer size, also chunk large files")
	mountOptions.dataCenter = cmdMount.Flag.String("dataCenter", "", "prefer to write to the data center")
	mountCpuProfile = cmdMount.Flag.String("cpuprofile", "", "cpu profile output file")
	mountMemProfile = cmdMount.Flag.String("memprofile", "", "memory profile output file")
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

  `,
}

func parseFilerGrpcAddress(filer string, optionalGrpcPort int) (filerGrpcAddress string, err error) {
	hostnameAndPort := strings.Split(filer, ":")
	if len(hostnameAndPort) != 2 {
		return "", fmt.Errorf("The filer should have hostname:port format: %v", hostnameAndPort)
	}

	filerPort, parseErr := strconv.ParseUint(hostnameAndPort[1], 10, 64)
	if parseErr != nil {
		return "", fmt.Errorf("The filer filer port parse error: %v", parseErr)
	}

	filerGrpcPort := int(filerPort) + 10000
	if optionalGrpcPort != 0 {
		filerGrpcPort = optionalGrpcPort
	}

	return fmt.Sprintf("%s:%d", hostnameAndPort[0], filerGrpcPort), nil
}
