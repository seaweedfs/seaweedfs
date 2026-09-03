package storage

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// VolumeFileScanner4RebuildIdx writes one .idx row per .dat record, in .dat
// append order, which is the shape the volume server's own writes leave behind.
type VolumeFileScanner4RebuildIdx struct {
	writer *bufio.Writer
}

func (scanner *VolumeFileScanner4RebuildIdx) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	return nil
}

func (scanner *VolumeFileScanner4RebuildIdx) ReadNeedleBody() bool {
	return false
}

func (scanner *VolumeFileScanner4RebuildIdx) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	// An all-zero header is unwritten space, not a record: stop rather than
	// index a truncated .dat's tail as millions of needle 0 rows.
	if n.Size == 0 && n.Id == 0 {
		return io.EOF
	}
	size := n.Size
	if !size.IsValid() {
		size = types.TombstoneFileSize
	}
	_, err := scanner.writer.Write(needle_map.ToBytes(n.Id, types.ToOffset(offset), size))
	return err
}

// rebuildIdxFile regenerates the volume's .idx from its .dat. The whole index
// is derivable from the data file, so a volume whose index directory has no
// .idx -- a -dir.idx pointed at an empty directory, or a lost index -- comes
// back on its own instead of taking the volume server down. The rows go to a
// temp file that is renamed in, so an interrupted rebuild leaves no partial
// index behind.
func (v *Volume) rebuildIdxFile() error {
	if v.DataBackend == nil {
		return fmt.Errorf("volume %d has no data backend", v.Id)
	}

	idxFileName := v.FileName(".idx")
	tmpFileName := idxFileName + ".tmp"
	tmpFile, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmpFileName, err)
	}
	defer os.Remove(tmpFileName)

	scanner := &VolumeFileScanner4RebuildIdx{writer: bufio.NewWriter(tmpFile)}
	err = ScanVolumeFileFrom(v.Version(), v.DataBackend, int64(v.SuperBlock.BlockSize()), scanner)
	if err == nil {
		err = scanner.writer.Flush()
	}
	if err == nil {
		err = tmpFile.Sync()
	}
	if closeErr := tmpFile.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("rebuild %s from %s: %w", idxFileName, v.FileName(".dat"), err)
	}

	if err := os.Rename(tmpFileName, idxFileName); err != nil {
		return fmt.Errorf("rename %s: %w", tmpFileName, err)
	}
	return fsyncDir(filepath.Dir(idxFileName))
}
