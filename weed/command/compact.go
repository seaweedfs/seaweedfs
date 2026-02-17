package command

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	cmdCompact.Run = runCompact // break init cycle
}

var cmdCompact = &Command{
	UsageLine: "compact -dir=/tmp -volumeId=234",
	Short:     "run weed tool compact on volume file",
	Long: `Force an compaction to remove deleted files from volume files.
  The compacted .dat file is stored as .cpd file.
  The compacted .idx file is stored as .cpx file.

  Supports two compaction methods:
    * data:  compacts based on the .dat file, works if .idx file is corrupted.
    * index: compacts based on the .idx file, works if deletion happened but not written to .dat files.

  `,
}

var (
	compactVolumePath        = cmdCompact.Flag.String("dir", ".", "data directory to store files")
	compactVolumeCollection  = cmdCompact.Flag.String("collection", "", "volume collection name")
	compactVolumeId          = cmdCompact.Flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir.")
	compactMethod            = cmdCompact.Flag.String("method", "data", "option to choose which compact method (data/index)")
	compactVolumePreallocate = cmdCompact.Flag.Int64("preallocateMB", 0, "preallocate volume disk space")
)

func runCompact(cmd *Command, args []string) bool {

	if *compactVolumeId == -1 {
		return false
	}

	preallocateBytes := *compactVolumePreallocate * (1 << 20)

	vid := needle.VolumeId(*compactVolumeId)
	v, err := storage.NewVolume(util.ResolvePath(*compactVolumePath), util.ResolvePath(*compactVolumePath), *compactVolumeCollection, vid, storage.NeedleMapInMemory, nil, nil, preallocateBytes, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		glog.Fatalf("Load Volume [ERROR] %s\n", err)
	}

	opts := &storage.CompactOptions{
		PreallocateBytes:  preallocateBytes,
		MaxBytesPerSecond: 0, // unlimited
	}
	switch strings.ToLower(*compactMethod) {
	case "data":
		if err = v.CompactByVolumeData(opts); err != nil {
			glog.Fatalf("Compact Volume [ERROR] %s\n", err)
		}
	case "index":
		if err = v.CompactByIndex(opts); err != nil {
			glog.Fatalf("Compact Volume [ERROR] %s\n", err)
		}
	default:
		glog.Fatalf("unsupported compaction method %q", *compactMethod)
	}

	return true
}
