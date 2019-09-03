package command

import (
	"github.com/joeslay/seaweedfs/weed/glog"
	"github.com/joeslay/seaweedfs/weed/storage"
	"github.com/joeslay/seaweedfs/weed/storage/needle"
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
	compactVolumeInMemory    = cmdCompact.Flag.Bool("volumeInMemory", false, "create the volume in memory")
)

func runCompact(cmd *Command, args []string) bool {

	if *compactVolumeId == -1 {
		return false
	}

	preallocate := *compactVolumePreallocate * (1 << 20)
	inMemory := *compactVolumeInMemory

	vid := needle.VolumeId(*compactVolumeId)
	v, err := storage.NewVolume(*compactVolumePath, *compactVolumeCollection, vid,
		storage.NeedleMapInMemory, nil, nil, preallocate, inMemory)
	if err != nil {
		glog.Fatalf("Load Volume [ERROR] %s\n", err)
	}
	if *compactMethod == 0 {
		if err = v.Compact(preallocate, 0, inMemory); err != nil {
			glog.Fatalf("Compact Volume [ERROR] %s\n", err)
		}
	} else {
		if err = v.Compact2(); err != nil {
			glog.Fatalf("Compact Volume [ERROR] %s\n", err)
		}
	}

	return true
}
