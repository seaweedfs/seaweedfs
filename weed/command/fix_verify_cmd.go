package command

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var cmdFixVerify = &Command{
	UsageLine: "fix.verify [-volumeId=234] [-collection=bigData] /data/dir [/data/dir2 /data/dir/bigData_234.dat ...]",
	Short:     "verify .dat files (needle structure and CRC32) and report damaged regions; never modifies anything",
	Long: `fix.verify reads every needle of the given .dat files (or of all .dat files in the given folders),
checks the needle structure and CRC32, and after damage searches for the next valid needle, so one bad
region does not hide the rest of the volume. Damaged regions are reported as zero-fill, garbage or
garbage+zeros with their offset, length, zero-window statistics and, when still readable, the file id of
the needle that was hit.

It only reads, so it is safe to run while the volume server is up, but the numbers are only stable for
volumes that are not being written (read-only or full volumes).

Exit status: 0 when every file verified cleanly, 3 when damage was found, 1 when a file could not be read
or verified, 2 on wrong usage.

To act on the result, run "weed fix -verify" (rebuilds the index from verified needles, -truncateTail cuts a
garbage tail) with the volume server stopped. This command shares its scan options with "weed fix -verify".
`,
}

var fixVerifyCmdFlags = bindDatVerifyFlags(&cmdFixVerify.Flag, "")

var (
	fixVerifyCmdCollection = cmdFixVerify.Flag.String("collection", "", "an optional volume collection name, if specified only it will be processed")
	fixVerifyCmdVolumeId   = cmdFixVerify.Flag.Int64("volumeId", 0, "an optional volume id, if not 0 (default) only it will be processed")
)

func init() {
	cmdFixVerify.Run = runFixVerify // break init cycle
}

// verifyTarget is one .dat file together with the volume it belongs to.
type verifyTarget struct {
	datName    string
	collection string
	volumeId   int64
}

func runFixVerify(cmd *Command, args []string) bool {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "fix.verify: give at least one folder or .dat file")
		return false
	}
	damaged, failed, err := fixVerifyPaths(args, *fixVerifyCmdCollection, *fixVerifyCmdVolumeId, fixVerifyCmdFlags)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if failed > 0 {
		os.Exit(1)
	}
	if damaged > 0 {
		os.Exit(3)
	}
	return true
}

// fixVerifyPaths verifies every selected .dat file and returns how many had
// damaged regions and how many could not be verified at all.
func fixVerifyPaths(args []string, collection string, volumeId int64, flags *datVerifyFlags) (damaged, failed int, err error) {
	var targets []verifyTarget
	for _, arg := range args {
		found, err := collectVerifyTargets(arg)
		if err != nil {
			return 0, 0, err
		}
		for _, t := range found {
			if collection != "" && t.collection != collection {
				continue
			}
			if volumeId != 0 && t.volumeId != volumeId {
				continue
			}
			targets = append(targets, t)
		}
	}
	if len(targets) == 0 {
		return 0, 0, fmt.Errorf("fix.verify: no matching .dat files found")
	}

	opts := flags.options()
	for _, t := range targets {
		started := time.Now()
		sum, anomalies, verr := verifyDatFile(t.datName, needle.VolumeId(t.volumeId), opts, func(*needle.Needle, int64) {})
		if verr != nil {
			fmt.Fprintf(os.Stderr, "volume %d: cannot verify %s: %v\n", t.volumeId, t.datName, verr)
			failed++
			continue
		}
		if perr := flags.printAnomalies(t.volumeId, anomalies); perr != nil {
			return damaged, failed, perr
		}
		printVerifySummary(t.volumeId, sum, time.Since(started))
		if sum.BadRegions > 0 {
			damaged++
			if sum.TailGarbage {
				fmt.Printf("volume %d: %d bytes after the last valid needle (offset %d); the volume stays read-only until the tail is cut\n",
					t.volumeId, sum.FileSize-sum.CleanEnd, sum.CleanEnd)
			}
		}
	}
	fmt.Printf("fix.verify: %d files checked, %d with damaged regions, %d could not be verified\n", len(targets), damaged, failed)
	return damaged, failed, nil
}

func collectVerifyTargets(arg string) ([]verifyTarget, error) {
	resolved := util.ResolvePath(arg)
	var datFiles []string
	isDir := util.FolderExists(resolved)
	if isDir {
		entries, err := os.ReadDir(resolved)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".dat") {
				datFiles = append(datFiles, filepath.Join(resolved, e.Name()))
			}
		}
		sort.Strings(datFiles)
	} else {
		if _, err := os.Stat(resolved); err != nil {
			return nil, err
		}
		if !strings.HasSuffix(resolved, ".dat") {
			return nil, fmt.Errorf("%s is not a folder or a .dat file", arg)
		}
		datFiles = []string{resolved}
	}

	var targets []verifyTarget
	for _, datName := range datFiles {
		base := strings.TrimSuffix(path.Base(filepath.ToSlash(datName)), ".dat")
		collection, idStr := "", base
		if sep := strings.LastIndex(base, "_"); sep > 0 {
			collection, idStr = base[:sep], base[sep+1:]
		}
		vid, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil || vid < 0 || vid > 0xFFFFFFFF {
			if isDir {
				fmt.Fprintf(os.Stderr, "skipping %s: cannot parse a volume id from the file name\n", datName)
				continue
			}
			return nil, fmt.Errorf("cannot parse volume id from %s", datName)
		}
		targets = append(targets, verifyTarget{datName: datName, collection: collection, volumeId: vid})
	}
	return targets, nil
}
