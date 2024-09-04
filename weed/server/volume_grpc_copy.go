package weed_server

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const BufferSizeLimit = 1024 * 1024 * 2

// VolumeCopy copy the .idx .dat .vif files, and mount the volume
func (vs *VolumeServer) VolumeCopy(req *volume_server_pb.VolumeCopyRequest, stream volume_server_pb.VolumeServer_VolumeCopyServer) error {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v != nil {

		glog.V(0).Infof("volume %d already exists. deleted before copying...", req.VolumeId)

		err := vs.store.DeleteVolume(needle.VolumeId(req.VolumeId), false)
		if err != nil {
			return fmt.Errorf("failed to delete existing volume %d: %v", req.VolumeId, err)
		}

		glog.V(0).Infof("deleted existing volume %d before copying.", req.VolumeId)
	}

	// the master will not start compaction for read-only volumes, so it is safe to just copy files directly
	// copy .dat and .idx files
	//   read .idx .dat file size and timestamp
	//   send .idx file
	//   send .dat file
	//   confirm size and timestamp
	var volFileInfoResp *volume_server_pb.ReadVolumeFileStatusResponse
	var dataBaseFileName, indexBaseFileName, idxFileName, datFileName string
	var hasRemoteDatFile bool
	err := operation.WithVolumeServerClient(true, pb.ServerAddress(req.SourceDataNode), vs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		var err error
		volFileInfoResp, err = client.ReadVolumeFileStatus(context.Background(),
			&volume_server_pb.ReadVolumeFileStatusRequest{
				VolumeId: req.VolumeId,
			})
		if nil != err {
			return fmt.Errorf("read volume file status failed, %v", err)
		}

		diskType := volFileInfoResp.DiskType
		if req.DiskType != "" {
			diskType = req.DiskType
		}
		location := vs.store.FindFreeLocation(func(location *storage.DiskLocation) bool {
			return location.DiskType == types.ToDiskType(diskType)
		})
		if location == nil {
			return fmt.Errorf("no space left for disk type %s", types.ToDiskType(diskType).ReadableString())
		}

		dataBaseFileName = storage.VolumeFileName(location.Directory, volFileInfoResp.Collection, int(req.VolumeId))
		indexBaseFileName = storage.VolumeFileName(location.IdxDirectory, volFileInfoResp.Collection, int(req.VolumeId))
		hasRemoteDatFile = volFileInfoResp.VolumeInfo != nil && len(volFileInfoResp.VolumeInfo.Files) > 0

		util.WriteFile(dataBaseFileName+".note", []byte(fmt.Sprintf("copying from %s", req.SourceDataNode)), 0755)

		defer func() {
			if err != nil {
				os.Remove(dataBaseFileName + ".dat")
				os.Remove(indexBaseFileName + ".idx")
				os.Remove(dataBaseFileName + ".vif")
				os.Remove(dataBaseFileName + ".note")
			}
		}()

		var preallocateSize int64
		if grpcErr := pb.WithMasterClient(false, vs.GetMaster(context.Background()), vs.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			resp, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get master %s configuration: %v", vs.GetMaster(context.Background()), err)
			}
			if resp.VolumePreallocate {
				preallocateSize = int64(resp.VolumeSizeLimitMB) * (1 << 20)
			}
			return nil
		}); grpcErr != nil {
			glog.V(0).Infof("connect to %s: %v", vs.GetMaster(context.Background()), grpcErr)
		}

		if preallocateSize > 0 && !hasRemoteDatFile {
			volumeFile := dataBaseFileName + ".dat"
			_, err := backend.CreateVolumeFile(volumeFile, preallocateSize, 0)
			if err != nil {
				return fmt.Errorf("create volume file %s: %v", volumeFile, err)
			}
		}

		// println("source:", volFileInfoResp.String())
		copyResponse := &volume_server_pb.VolumeCopyResponse{}
		reportInterval := int64(1024 * 1024 * 128)
		nextReportTarget := reportInterval
		var modifiedTsNs int64
		var sendErr error
		var ioBytePerSecond int64
		if req.IoBytePerSecond <= 0 {
			ioBytePerSecond = vs.compactionBytePerSecond
		} else {
			ioBytePerSecond = req.IoBytePerSecond
		}
		throttler := util.NewWriteThrottler(ioBytePerSecond)

		if !hasRemoteDatFile {
			if modifiedTsNs, err = vs.doCopyFileWithThrottler(client, false, req.Collection, req.VolumeId, volFileInfoResp.CompactionRevision, volFileInfoResp.DatFileSize, dataBaseFileName, ".dat", false, true, func(processed int64) bool {
				if processed > nextReportTarget {
					copyResponse.ProcessedBytes = processed
					if sendErr = stream.Send(copyResponse); sendErr != nil {
						return false
					}
					nextReportTarget = processed + reportInterval
				}
				return true
			}, throttler); err != nil {
				return err
			}
			if sendErr != nil {
				return sendErr
			}
			if modifiedTsNs > 0 {
				os.Chtimes(dataBaseFileName+".dat", time.Unix(0, modifiedTsNs), time.Unix(0, modifiedTsNs))
			}
		}

		if modifiedTsNs, err = vs.doCopyFileWithThrottler(client, false, req.Collection, req.VolumeId, volFileInfoResp.CompactionRevision, volFileInfoResp.IdxFileSize, indexBaseFileName, ".idx", false, false, nil, throttler); err != nil {
			return err
		}
		if modifiedTsNs > 0 {
			os.Chtimes(indexBaseFileName+".idx", time.Unix(0, modifiedTsNs), time.Unix(0, modifiedTsNs))
		}

		if modifiedTsNs, err = vs.doCopyFileWithThrottler(client, false, req.Collection, req.VolumeId, volFileInfoResp.CompactionRevision, 1024*1024, dataBaseFileName, ".vif", false, true, nil, throttler); err != nil {
			return err
		}
		if modifiedTsNs > 0 {
			os.Chtimes(dataBaseFileName+".vif", time.Unix(0, modifiedTsNs), time.Unix(0, modifiedTsNs))
		}

		os.Remove(dataBaseFileName + ".note")

		return nil
	})

	if err != nil {
		return err
	}
	if dataBaseFileName == "" {
		return fmt.Errorf("not found volume %d file", req.VolumeId)
	}

	idxFileName = indexBaseFileName + ".idx"
	datFileName = dataBaseFileName + ".dat"

	defer func() {
		if err != nil && dataBaseFileName != "" {
			os.Remove(idxFileName)
			os.Remove(datFileName)
			os.Remove(dataBaseFileName + ".vif")
		}
	}()

	if err = checkCopyFiles(volFileInfoResp, hasRemoteDatFile, idxFileName, datFileName); err != nil { // added by panyc16
		return err
	}

	// mount the volume
	err = vs.store.MountVolume(needle.VolumeId(req.VolumeId))
	if err != nil {
		return fmt.Errorf("failed to mount volume %d: %v", req.VolumeId, err)
	}

	if err = stream.Send(&volume_server_pb.VolumeCopyResponse{
		LastAppendAtNs: volFileInfoResp.DatFileTimestampSeconds * uint64(time.Second),
	}); err != nil {
		glog.Errorf("send response: %v", err)
	}

	return err
}

func (vs *VolumeServer) doCopyFile(client volume_server_pb.VolumeServerClient, isEcVolume bool, collection string, vid, compactRevision uint32, stopOffset uint64, baseFileName, ext string, isAppend, ignoreSourceFileNotFound bool, progressFn storage.ProgressFunc) (modifiedTsNs int64, err error) {
	return vs.doCopyFileWithThrottler(client, isEcVolume, collection, vid, compactRevision, stopOffset, baseFileName, ext, isAppend, ignoreSourceFileNotFound, progressFn, util.NewWriteThrottler(vs.compactionBytePerSecond))
}

func (vs *VolumeServer) doCopyFileWithThrottler(client volume_server_pb.VolumeServerClient, isEcVolume bool, collection string, vid, compactRevision uint32, stopOffset uint64, baseFileName, ext string, isAppend, ignoreSourceFileNotFound bool, progressFn storage.ProgressFunc, throttler *util.WriteThrottler) (modifiedTsNs int64, err error) {

	copyFileClient, err := client.CopyFile(context.Background(), &volume_server_pb.CopyFileRequest{
		VolumeId:                 vid,
		Ext:                      ext,
		CompactionRevision:       compactRevision,
		StopOffset:               stopOffset,
		Collection:               collection,
		IsEcVolume:               isEcVolume,
		IgnoreSourceFileNotFound: ignoreSourceFileNotFound,
	})
	if err != nil {
		return modifiedTsNs, fmt.Errorf("failed to start copying volume %d %s file: %v", vid, ext, err)
	}

	modifiedTsNs, err = writeToFile(copyFileClient, baseFileName+ext, throttler, isAppend, progressFn)
	if err != nil {
		return modifiedTsNs, fmt.Errorf("failed to copy %s file: %v", baseFileName+ext, err)
	}

	return modifiedTsNs, nil

}

/*
*
only check the differ of the file size
todo: maybe should check the received count and deleted count of the volume
*/
func checkCopyFiles(originFileInf *volume_server_pb.ReadVolumeFileStatusResponse, hasRemoteDatFile bool, idxFileName, datFileName string) error {
	stat, err := os.Stat(idxFileName)
	if err != nil {
		return fmt.Errorf("stat idx file %s failed: %v", idxFileName, err)
	}
	if originFileInf.IdxFileSize != uint64(stat.Size()) {
		return fmt.Errorf("idx file %s size [%v] is not same as origin file size [%v]",
			idxFileName, stat.Size(), originFileInf.IdxFileSize)
	}

	if hasRemoteDatFile {
		return nil
	}

	stat, err = os.Stat(datFileName)
	if err != nil {
		return fmt.Errorf("get dat file info failed, %v", err)
	}
	if originFileInf.DatFileSize != uint64(stat.Size()) {
		return fmt.Errorf("the dat file size [%v] is not same as origin file size [%v]",
			stat.Size(), originFileInf.DatFileSize)
	}
	return nil
}

func writeToFile(client volume_server_pb.VolumeServer_CopyFileClient, fileName string, wt *util.WriteThrottler, isAppend bool, progressFn storage.ProgressFunc) (modifiedTsNs int64, err error) {
	glog.V(4).Infof("writing to %s", fileName)
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if isAppend {
		flags = os.O_WRONLY | os.O_CREATE
	}
	dst, err := os.OpenFile(fileName, flags, 0644)
	if err != nil {
		return modifiedTsNs, nil
	}
	defer dst.Close()

	var progressedBytes int64
	for {
		resp, receiveErr := client.Recv()
		if receiveErr == io.EOF {
			break
		}
		if resp != nil && resp.ModifiedTsNs != 0 {
			modifiedTsNs = resp.ModifiedTsNs
		}
		if receiveErr != nil {
			return modifiedTsNs, fmt.Errorf("receiving %s: %v", fileName, receiveErr)
		}
		dst.Write(resp.FileContent)
		progressedBytes += int64(len(resp.FileContent))
		if progressFn != nil {
			if !progressFn(progressedBytes) {
				return modifiedTsNs, fmt.Errorf("interrupted copy operation")
			}
		}
		wt.MaybeSlowdown(int64(len(resp.FileContent)))
	}
	return modifiedTsNs, nil
}

func (vs *VolumeServer) ReadVolumeFileStatus(ctx context.Context, req *volume_server_pb.ReadVolumeFileStatusRequest) (*volume_server_pb.ReadVolumeFileStatusResponse, error) {
	resp := &volume_server_pb.ReadVolumeFileStatusResponse{}
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	resp.VolumeId = req.VolumeId
	datSize, idxSize, modTime := v.FileStat()
	resp.DatFileSize = datSize
	resp.IdxFileSize = idxSize
	resp.DatFileTimestampSeconds = uint64(modTime.Unix())
	resp.IdxFileTimestampSeconds = uint64(modTime.Unix())
	resp.FileCount = v.FileCount()
	resp.CompactionRevision = uint32(v.CompactionRevision)
	resp.Collection = v.Collection
	resp.DiskType = string(v.DiskType())
	resp.VolumeInfo = v.GetVolumeInfo()
	return resp, nil
}

// CopyFile client pulls the volume related file from the source server.
// if req.CompactionRevision != math.MaxUint32, it ensures the compact revision is as expected
// The copying still stop at req.StopOffset, but you can set it to math.MaxUint64 in order to read all data.
func (vs *VolumeServer) CopyFile(req *volume_server_pb.CopyFileRequest, stream volume_server_pb.VolumeServer_CopyFileServer) error {

	var fileName string
	if !req.IsEcVolume {
		v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
		if v == nil {
			return fmt.Errorf("not found volume id %d", req.VolumeId)
		}

		if uint32(v.CompactionRevision) != req.CompactionRevision && req.CompactionRevision != math.MaxUint32 {
			return fmt.Errorf("volume %d is compacted", req.VolumeId)
		}
		v.SyncToDisk()
		fileName = v.FileName(req.Ext)
	} else {
		baseFileName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId)) + req.Ext
		for _, location := range vs.store.Locations {
			tName := util.Join(location.Directory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
			tName = util.Join(location.IdxDirectory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
		}
		if fileName == "" {
			if req.IgnoreSourceFileNotFound {
				return nil
			}
			return fmt.Errorf("CopyFile not found ec volume id %d", req.VolumeId)
		}
	}

	bytesToRead := int64(req.StopOffset)

	file, err := os.Open(fileName)
	if err != nil {
		if req.IgnoreSourceFileNotFound && err == os.ErrNotExist {
			return nil
		}
		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	fileModTsNs := fileInfo.ModTime().UnixNano()

	buffer := make([]byte, BufferSizeLimit)

	for bytesToRead > 0 {
		bytesread, err := file.Read(buffer)

		// println(fileName, "read", bytesread, "bytes, with target", bytesToRead)

		if err != nil {
			if err != io.EOF {
				return err
			}
			// println(fileName, "read", bytesread, "bytes, with target", bytesToRead, "err", err.Error())
			break
		}

		if int64(bytesread) > bytesToRead {
			bytesread = int(bytesToRead)
		}
		err = stream.Send(&volume_server_pb.CopyFileResponse{
			FileContent:  buffer[:bytesread],
			ModifiedTsNs: fileModTsNs,
		})
		if err != nil {
			// println("sending", bytesread, "bytes err", err.Error())
			return err
		}
		fileModTsNs = 0 // only send once

		bytesToRead -= int64(bytesread)

	}

	return nil
}
