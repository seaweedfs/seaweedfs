package command

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
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

  `,
}

var (
	compactVolumePath        = cmdCompact.Flag.String("dir", ".", "data directory to store files")
	compactVolumeCollection  = cmdCompact.Flag.String("collection", "", "volume collection name")
	compactVolumeId          = cmdCompact.Flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir.")
	compactMethod            = cmdCompact.Flag.Int("method", 0, "option to choose which compact method. use 0 or 1.")
	compactVolumePreallocate = cmdCompact.Flag.Int64("preallocateMB", 0, "preallocate volume disk space")
)

func runCompact(cmd *Command, args []string) bool {

	if *compactVolumeId == -1 {
		return false
	}

	preallocate := *compactVolumePreallocate * (1 << 20)

	vid := storage.VolumeId(*compactVolumeId)
	v, err := storage.NewVolume(*compactVolumePath, *compactVolumeCollection, vid,
		storage.NeedleMapInMemory, nil, nil, preallocate)
	if err != nil {
		glog.Fatalf("Load Volume [ERROR] %s\n", err)
	}
	if *compactMethod == 0 {
		if err = v.Compact(preallocate); err != nil {
			glog.Fatalf("Compact Volume [ERROR] %s\n", err)
		}
	} else {
		if err = v.Compact2(); err != nil {
			glog.Fatalf("Compact Volume [ERROR] %s\n", err)
		}
	}

	return true
}
