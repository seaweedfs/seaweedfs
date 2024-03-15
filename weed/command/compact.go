package command

import (
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

  For method=0, it compacts based on the .dat file, works if .idx file is corrupted.
  For method=1, it compacts based on the .idx file, works if deletion happened but not written to .dat files.

  `,
}

var (
	compactVolumePath        = cmdCompact.Flag.String("dir", ".", "data directory to store files")
	compactVolumeCollection  = cmdCompact.Flag.String("collection", "", "volume collection name")
	compactVolumeId          = cmdCompact.Flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir.")
	compactMethod            = cmdCompact.Flag.Int("method", 0, "option to choose which compact method. use 0 (default) or 1.")
	compactVolumePreallocate = cmdCompact.Flag.Int64("preallocateMB", 0, "preallocate volume disk space")
)

func runCompact(cmd *Command, args []string) bool {

	if *compactVolumeId == -1 {
		return false
	}

	preallocate := *compactVolumePreallocate * (1 << 20)

	vid := needle.VolumeId(*compactVolumeId)
	v, err := storage.NewVolume(util.ResolvePath(*compactVolumePath), util.ResolvePath(*compactVolumePath), *compactVolumeCollection, vid, storage.NeedleMapInMemory, nil, nil, preallocate, 0, 0)
	if err != nil {
		glog.Fatalf("Load Volume [ERROR] %s\n", err)
	}
	if *compactMethod == 0 {
		if err = v.Compact(preallocate, 0); err != nil {
			glog.Fatalf("Compact Volume [ERROR] %s\n", err)
		}
	} else {
		if err = v.Compact2(preallocate, 0, nil); err != nil {
			glog.Fatalf("Compact Volume [ERROR] %s\n", err)
		}
	}

	return true
}
