package erasure_coding

import (
	"fmt"
	"io"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	MarkNeedleDeleted = func(file *os.File, offset int64) error {
		b := make([]byte, types.SizeSize)
		types.SizeToBytes(b, types.TombstoneFileSize)
		n, err := file.WriteAt(b, offset+types.NeedleIdSize+types.OffsetSize)
		if err != nil {
			return fmt.Errorf("sorted needle write error: %w", err)
		}
		if n != types.SizeSize {
			return fmt.Errorf("sorted needle written %d bytes, expecting %d", n, types.SizeSize)
		}
		return nil
	}
)

func (ev *EcVolume) DeleteNeedleFromEcx(needleId types.NeedleId) (err error) {

	_, _, err = SearchNeedleFromSortedIndex(ev.ecxFile, ev.ecxFileSize, needleId, MarkNeedleDeleted)

	if err != nil {
		if err == NotFoundError {
			glog.Infof("❓ EC NEEDLE NOT FOUND: needle %d not in .ecx index for volume %d generation %d - skipping .ecj recording",
				needleId, ev.VolumeId, ev.Generation)
			return nil
		}
		glog.Errorf("❌ EC INDEX SEARCH ERROR: needle %d volume %d generation %d: %v", needleId, ev.VolumeId, ev.Generation, err)
		return err
	}

	// Needle found and marked deleted in .ecx, now record in .ecj
	glog.Infof("📝 EC NEEDLE FOUND: recording needle %d in .ecj for volume %d generation %d",
		needleId, ev.VolumeId, ev.Generation)

	b := make([]byte, types.NeedleIdSize)
	types.NeedleIdToBytes(b, needleId)

	ev.ecjFileAccessLock.Lock()
	defer ev.ecjFileAccessLock.Unlock()

	if ev.ecjFile == nil {
		glog.Errorf("EC deletion: .ecj file is nil for volume %d generation %d", ev.VolumeId, ev.Generation)
		return fmt.Errorf("ecjFile is nil")
	}

	_, err = ev.ecjFile.Seek(0, io.SeekEnd)
	if err != nil {
		glog.Errorf("EC deletion: failed to seek .ecj file for volume %d generation %d: %v", ev.VolumeId, ev.Generation, err)
		return err
	}

	n, err := ev.ecjFile.Write(b)
	if err != nil {
		glog.Errorf("EC deletion: failed to write to .ecj file for volume %d generation %d: %v", ev.VolumeId, ev.Generation, err)
		return err
	}

	if n != len(b) {
		glog.Errorf("EC deletion: partial write to .ecj file for volume %d generation %d: wrote %d bytes, expected %d",
			ev.VolumeId, ev.Generation, n, len(b))
		return fmt.Errorf("partial write: wrote %d bytes, expected %d", n, len(b))
	}

	glog.Infof("✅ EC JOURNAL WRITE SUCCESS: wrote %d bytes to .ecj for volume %d generation %d", n, ev.VolumeId, ev.Generation)

	return
}

func RebuildEcxFile(baseFileName string) error {

	if !util.FileExists(baseFileName + ".ecj") {
		return nil
	}

	ecxFile, err := os.OpenFile(baseFileName+".ecx", os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("rebuild: failed to open ecx file: %w", err)
	}
	defer ecxFile.Close()

	fstat, err := ecxFile.Stat()
	if err != nil {
		return err
	}

	ecxFileSize := fstat.Size()

	ecjFile, err := os.OpenFile(baseFileName+".ecj", os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("rebuild: failed to open ecj file: %w", err)
	}

	buf := make([]byte, types.NeedleIdSize)
	for {
		n, _ := ecjFile.Read(buf)
		if n != types.NeedleIdSize {
			break
		}

		needleId := types.BytesToNeedleId(buf)

		_, _, err = SearchNeedleFromSortedIndex(ecxFile, ecxFileSize, needleId, MarkNeedleDeleted)

		if err != nil && err != NotFoundError {
			ecxFile.Close()
			return err
		}

	}

	ecxFile.Close()

	os.Remove(baseFileName + ".ecj")

	return nil
}
