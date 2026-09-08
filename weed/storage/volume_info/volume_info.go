package volume_info

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
)

// MaybeLoadVolumeInfo load the file data as *volume_server_pb.VolumeInfo, the returned volumeInfo will not be nil
func MaybeLoadVolumeInfo(fileName string) (volumeInfo *volume_server_pb.VolumeInfo, hasRemoteFile bool, hasVolumeInfoFile bool, err error) {

	volumeInfo = &volume_server_pb.VolumeInfo{}

	glog.V(1).Infof("maybeLoadVolumeInfo checks %s", fileName)
	if exists, canRead, _, _, _ := util.CheckFile(fileName); !exists || !canRead {
		if !exists {
			return
		}
		hasVolumeInfoFile = true
		if !canRead {
			glog.Warningf("can not read %s", fileName)
			err = fmt.Errorf("can not read %s", fileName)
			return
		}
		return
	}

	hasVolumeInfoFile = true

	glog.V(1).Infof("maybeLoadVolumeInfo reads %s", fileName)
	fileData, readErr := os.ReadFile(fileName)
	if readErr != nil {
		glog.Warningf("fail to read %s : %v", fileName, readErr)
		err = fmt.Errorf("fail to read %s : %v", fileName, readErr)
		return

	}

	// Handle empty .vif files gracefully - treat as if file doesn't exist
	// This can happen when ec.decode copies from a source that doesn't have a .vif file
	if len(fileData) == 0 {
		glog.Warningf("empty volume info file %s, treating as non-existent", fileName)
		hasVolumeInfoFile = false
		return
	}

	glog.V(1).Infof("maybeLoadVolumeInfo Unmarshal volume info %v", fileName)
	if err = jsonpb.Unmarshal(fileData, volumeInfo); err != nil {
		if oldVersionErr := tryOldVersionVolumeInfo(fileData, volumeInfo); oldVersionErr != nil {
			glog.Warningf("unmarshal error: %v oldFormat: %v", err, oldVersionErr)
			err = fmt.Errorf("unmarshal error: %w oldFormat: %v", err, oldVersionErr)
			return
		} else {
			err = nil
		}
	}

	if len(volumeInfo.GetFiles()) == 0 {
		return
	}

	hasRemoteFile = true

	return
}

// NotCrashDurableError indicates that the .vif file was renamed
// successfully but the directory entry may not survive a crash. The
// on-disk file already holds the new metadata, so callers should keep
// in-memory state aligned with the file rather than rolling back, while
// still propagating the durability failure to the user.
type NotCrashDurableError struct {
	FileName string
	Err      error
}

func (e *NotCrashDurableError) Error() string {
	return fmt.Sprintf("volume info %s saved but not crash-durable: %v", e.FileName, e.Err)
}

func (e *NotCrashDurableError) Unwrap() error { return e.Err }

func SaveVolumeInfo(fileName string, volumeInfo *volume_server_pb.VolumeInfo) error {

	if exists, _, canWrite, _, _ := util.CheckFile(fileName); exists && !canWrite {
		return fmt.Errorf("failed to check %s not writable", fileName)
	}

	m := jsonpb.MarshalOptions{
		AllowPartial:    true,
		EmitUnpopulated: true,
		Indent:          "  ",
	}

	text, marshalErr := m.Marshal(volumeInfo)
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal %s: %v", fileName, marshalErr)
	}

	// Write atomically so a write/sync/close failure leaves the existing
	// .vif file intact. PersistReadOnly rolls back in-memory state on
	// error; the atomic rename guarantees the durable file still matches
	// that rolled-back state rather than the requested mode. Use a
	// unique temp file so concurrent saves for the same volume do not
	// collide on a shared .tmp path.
	f, err := os.CreateTemp(filepath.Dir(fileName), filepath.Base(fileName)+".tmp.*")
	if err != nil {
		return fmt.Errorf("failed to create temp file for %s: %w", fileName, err)
	}
	tmpName := f.Name()
	if _, err := f.Write(text); err != nil {
		f.Close()
		os.Remove(tmpName)
		return fmt.Errorf("failed to write %s: %w", fileName, err)
	}
	if err := f.Chmod(0644); err != nil {
		f.Close()
		os.Remove(tmpName)
		return fmt.Errorf("failed to chmod %s: %w", fileName, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpName)
		return fmt.Errorf("failed to sync %s: %w", fileName, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("failed to close %s: %w", fileName, err)
	}
	if err := os.Rename(tmpName, fileName); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("failed to rename %s: %w", fileName, err)
	}
	// The rename has committed the new metadata to the on-disk file.
	// A directory fsync failure only risks losing the rename across a
	// crash; the file content is already correct, so callers must not
	// roll back in-memory state. Return NotCrashDurableError so they
	// can distinguish this from a pre-commit failure and keep state
	// aligned with the renamed file while still reporting the issue.
	if err := util.FsyncDir(filepath.Dir(fileName)); err != nil {
		glog.Warningf("fsync dir for %s: %v", fileName, err)
		return &NotCrashDurableError{FileName: fileName, Err: err}
	}

	return nil
}

func tryOldVersionVolumeInfo(data []byte, volumeInfo *volume_server_pb.VolumeInfo) error {
	oldVersionVolumeInfo := &volume_server_pb.OldVersionVolumeInfo{}
	if err := jsonpb.Unmarshal(data, oldVersionVolumeInfo); err != nil {
		return fmt.Errorf("failed to unmarshal old version volume info: %w", err)
	}
	volumeInfo.Files = oldVersionVolumeInfo.Files
	volumeInfo.Version = oldVersionVolumeInfo.Version
	volumeInfo.Replication = oldVersionVolumeInfo.Replication
	volumeInfo.BytesOffset = oldVersionVolumeInfo.BytesOffset
	volumeInfo.DatFileSize = oldVersionVolumeInfo.DatFileSize
	volumeInfo.ExpireAtSec = oldVersionVolumeInfo.DestroyTime
	volumeInfo.ReadOnly = oldVersionVolumeInfo.ReadOnly

	return nil
}
