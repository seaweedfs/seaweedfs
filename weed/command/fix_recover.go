package command

import (
	"fmt"
	"path"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

func init() {
	cmdFixRecover.Run = runFixRecover // break init cycle
}

var cmdFixRecover = &Command{
	UsageLine: "fix.recover [-volumeId=234] [-collection=bigData] [-dryRun] [-truncateTail] /data/dir | /data/dir/bigData_234.dat ...",
	Short:     "recover the needles that survive damage in .dat files and rebuild their index",
	Long: `fix.recover reads every needle of the given .dat files (or of all .dat files in the given folders) and
checks its structure and CRC. When a record is damaged it searches forward for the next record that
verifies, reports the skipped region, and goes on, so one bad region does not hide the rest of the
volume. The .idx is rebuilt from the verified needles only. Damaged needles are reported, not repaired:
restore them from a replica.

  Stop the volume server before running this command.
  Each damaged region is reported with its offset, length, kind (zero-fill, garbage, garbage+zeros)
  and, when still readable, the file id of the header that was hit. A key whose newest copy is
  damaged while an older copy is indexed is reported as a stale-version warning.
  The rebuilt .idx is written next to the .dat; a volume server started with -dir.idx uses an .idx
  next to the .dat in preference to the one in that directory. The previous .idx is kept as .idx.bak,
  and stale .ldb, .sdx and .rdb caches next to it are removed; the volume server rebuilds them.
  -dryRun only reports. -truncateTail cuts garbage after the last valid needle, so the load-time
  integrity check passes; a tail that is not all zeros is saved to <dat>.tail.<offset>.bak first.
  The command refuses a volume while a .cpc compaction commit marker exists (start the volume server
  once so it reconciles the interrupted vacuum, then re-run), a volume on a remote tier, and an
  erasure-coded layout.
  Exit status is non-zero when damage or a stale version was found or a volume could not be read;
  with -ignoreError the run goes on with the next volume.
`,
}

var (
	fixRecoverCollection     = cmdFixRecover.Flag.String("collection", "", "an optional volume collection name, if specified only it will be processed")
	fixRecoverVolumeId       = cmdFixRecover.Flag.Int64("volumeId", 0, "an optional volume id, if not 0 (default) only it will be processed")
	fixRecoverIncludeDeleted = cmdFixRecover.Flag.Bool("includeDeleted", true, "include deleted entries in the index file")
	fixRecoverIgnoreError    = cmdFixRecover.Flag.Bool("ignoreError", false, "report a volume that cannot be recovered and go on with the next one")
	fixRecoverDryRun         = cmdFixRecover.Flag.Bool("dryRun", false, "only report, do not touch .idx or .dat")
	fixRecoverTruncateTail   = cmdFixRecover.Flag.Bool("truncateTail", false, "cut garbage after the last valid needle off the .dat (non-zero tails are saved to <dat>.tail.<offset>.bak first), so the load-time integrity check passes")
	fixRecoverMaxNeedleMB    = cmdFixRecover.Flag.Int("maxNeedleSizeMB", 64, "largest needle payload accepted while searching for the next valid needle after damage; a needle with a corrupted header larger than this cannot be recovered")
)

func runFixRecover(cmd *Command, args []string) bool {
	if len(args) == 0 {
		fmt.Println("fix.recover: give at least one folder or .dat file")
		return false
	}
	for _, arg := range args {
		basePath, files, err := listVolumeFiles(arg)
		if err != nil {
			fmt.Println(err)
			return false
		}
		for _, file := range files {
			vf, ok, parseErr := selectVolumeFile(file.Name(), ".dat", *fixRecoverCollection, *fixRecoverVolumeId)
			if parseErr != nil {
				fmt.Printf("Failed to parse volume id from %s: %v\n", vf.baseFileName, parseErr)
				if *fixRecoverIgnoreError {
					SetCommandExitStatus(1)
					continue
				}
				return false
			}
			if !ok {
				continue
			}
			doRecoverOneVolume(basePath, vf.baseFileName, vf.volumeId)
		}
	}
	return true
}

// doRecoverOneVolume recovers one volume with the command line options and
// honours -ignoreError.
func doRecoverOneVolume(basePath, baseFileName string, volumeId int64) {
	err := recoverOneVolume(basePath, baseFileName, volumeId, path.Join(basePath, baseFileName+".idx"), recoverOptions{
		includeDeleted:  *fixRecoverIncludeDeleted,
		dryRun:          *fixRecoverDryRun,
		truncateTail:    *fixRecoverTruncateTail,
		maxNeedleSizeMB: *fixRecoverMaxNeedleMB,
	})
	if err == nil {
		return
	}
	if *fixRecoverIgnoreError {
		glog.Error(err)
		SetCommandExitStatus(1)
		return
	}
	glog.Fatal(err)
}
