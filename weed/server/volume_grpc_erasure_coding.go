package weed_server

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

/*

Steps to apply erasure coding to .dat .idx files
0. ensure the volume is readonly
1. client call VolumeEcShardsGenerate to generate the .ecx and .ec01~.ec14 files
2. client ask master for possible servers to hold the ec files, at least 4 servers
3. client call VolumeEcShardsCopy on above target servers to copy ec files from the source server
4. target servers report the new ec files to the master
5.   master stores vid -> [14]*DataNode
6. client checks master. If all 14 slices are ready, delete the original .idx, .idx files

*/

// VolumeEcShardsGenerate generates the .ecx and .ec01 ~ .ec14 files
func (vs *VolumeServer) VolumeEcShardsGenerate(ctx context.Context, req *volume_server_pb.VolumeEcShardsGenerateRequest) (*volume_server_pb.VolumeEcShardsGenerateResponse, error) {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("volume %d not found", req.VolumeId)
	}
	baseFileName := v.FileName()

	if v.Collection != req.Collection {
		return nil, fmt.Errorf("existing collection:%v unexpected input: %v", v.Collection, req.Collection)
	}

	// write .ecx file
	if err := erasure_coding.WriteSortedEcxFile(baseFileName); err != nil {
		return nil, fmt.Errorf("WriteSortedEcxFile %s: %v", baseFileName, err)
	}

	// write .ec01 ~ .ec14 files
	if err := erasure_coding.WriteEcFiles(baseFileName); err != nil {
		return nil, fmt.Errorf("WriteEcFiles %s: %v", baseFileName, err)
	}

	return &volume_server_pb.VolumeEcShardsGenerateResponse{}, nil
}

// VolumeEcShardsRebuild generates the any of the missing .ec01 ~ .ec14 files
func (vs *VolumeServer) VolumeEcShardsRebuild(ctx context.Context, req *volume_server_pb.VolumeEcShardsRebuildRequest) (*volume_server_pb.VolumeEcShardsRebuildResponse, error) {

	baseFileName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId))

	var rebuiltShardIds []uint32

	for _, location := range vs.store.Locations {
		if util.FileExists(path.Join(location.Directory, baseFileName+".ecx")) {
			// write .ec01 ~ .ec14 files
			baseFileName = path.Join(location.Directory, baseFileName)
			if generatedShardIds, err := erasure_coding.RebuildEcFiles(baseFileName); err != nil {
				return nil, fmt.Errorf("RebuildEcFiles %s: %v", baseFileName, err)
			} else {
				rebuiltShardIds = generatedShardIds
			}

			if err := erasure_coding.RebuildEcxFile(baseFileName); err != nil {
				return nil, fmt.Errorf("RebuildEcxFile %s: %v", baseFileName, err)
			}

			break
		}
	}

	return &volume_server_pb.VolumeEcShardsRebuildResponse{
		RebuiltShardIds: rebuiltShardIds,
	}, nil
}

// VolumeEcShardsCopy copy the .ecx and some ec data slices
func (vs *VolumeServer) VolumeEcShardsCopy(ctx context.Context, req *volume_server_pb.VolumeEcShardsCopyRequest) (*volume_server_pb.VolumeEcShardsCopyResponse, error) {

	location := vs.store.FindFreeLocation()
	if location == nil {
		return nil, fmt.Errorf("no space left")
	}

	baseFileName := storage.VolumeFileName(location.Directory, req.Collection, int(req.VolumeId))

	err := operation.WithVolumeServerClient(req.SourceDataNode, vs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// copy ec data slices
		for _, shardId := range req.ShardIds {
			if err := vs.doCopyFile(ctx, client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, baseFileName, erasure_coding.ToExt(int(shardId)), false); err != nil {
				return err
			}
		}

		if !req.CopyEcxFile {
			return nil
		}

		// copy ecx file
		if err := vs.doCopyFile(ctx, client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, baseFileName, ".ecx", false); err != nil {
			return err
		}

		// copy ecj file
		if err := vs.doCopyFile(ctx, client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, baseFileName, ".ecj", true); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("VolumeEcShardsCopy volume %d: %v", req.VolumeId, err)
	}

	return &volume_server_pb.VolumeEcShardsCopyResponse{}, nil
}

// VolumeEcShardsDelete local delete the .ecx and some ec data slices if not needed
// the shard should not be mounted before calling this.
func (vs *VolumeServer) VolumeEcShardsDelete(ctx context.Context, req *volume_server_pb.VolumeEcShardsDeleteRequest) (*volume_server_pb.VolumeEcShardsDeleteResponse, error) {

	baseFilename := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId))

	found := false
	for _, location := range vs.store.Locations {
		if util.FileExists(path.Join(location.Directory, baseFilename+".ecx")) {
			found = true
			baseFilename = path.Join(location.Directory, baseFilename)
			for _, shardId := range req.ShardIds {
				os.Remove(baseFilename + erasure_coding.ToExt(int(shardId)))
			}
			break
		}
	}

	if !found {
		return nil, nil
	}

	// check whether to delete the ecx file also
	hasEcxFile := false
	existingShardCount := 0

	for _, location := range vs.store.Locations {
		fileInfos, err := ioutil.ReadDir(location.Directory)
		if err != nil {
			continue
		}
		for _, fileInfo := range fileInfos {
			if fileInfo.Name() == baseFilename+".ecx" {
				hasEcxFile = true
				continue
			}
			if strings.HasPrefix(fileInfo.Name(), baseFilename+".ec") {
				existingShardCount++
			}
		}
	}

	if hasEcxFile && existingShardCount == 0 {
		if err := os.Remove(baseFilename + ".ecx"); err != nil {
			return nil, err
		}
		if err := os.Remove(baseFilename + ".ecj"); err != nil {
			return nil, err
		}
	}

	return &volume_server_pb.VolumeEcShardsDeleteResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardsMount(ctx context.Context, req *volume_server_pb.VolumeEcShardsMountRequest) (*volume_server_pb.VolumeEcShardsMountResponse, error) {

	for _, shardId := range req.ShardIds {
		err := vs.store.MountEcShards(req.Collection, needle.VolumeId(req.VolumeId), erasure_coding.ShardId(shardId))

		if err != nil {
			glog.Errorf("ec shard mount %v: %v", req, err)
		} else {
			glog.V(2).Infof("ec shard mount %v", req)
		}

		if err != nil {
			return nil, fmt.Errorf("mount %d.%d: %v", req.VolumeId, shardId, err)
		}
	}

	return &volume_server_pb.VolumeEcShardsMountResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardsUnmount(ctx context.Context, req *volume_server_pb.VolumeEcShardsUnmountRequest) (*volume_server_pb.VolumeEcShardsUnmountResponse, error) {

	for _, shardId := range req.ShardIds {
		err := vs.store.UnmountEcShards(needle.VolumeId(req.VolumeId), erasure_coding.ShardId(shardId))

		if err != nil {
			glog.Errorf("ec shard unmount %v: %v", req, err)
		} else {
			glog.V(2).Infof("ec shard unmount %v", req)
		}

		if err != nil {
			return nil, fmt.Errorf("unmount %d.%d: %v", req.VolumeId, shardId, err)
		}
	}

	return &volume_server_pb.VolumeEcShardsUnmountResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardRead(req *volume_server_pb.VolumeEcShardReadRequest, stream volume_server_pb.VolumeServer_VolumeEcShardReadServer) error {

	ecVolume, found := vs.store.FindEcVolume(needle.VolumeId(req.VolumeId))
	if !found {
		return fmt.Errorf("VolumeEcShardRead not found ec volume id %d", req.VolumeId)
	}
	ecShard, found := ecVolume.FindEcVolumeShard(erasure_coding.ShardId(req.ShardId))
	if !found {
		return fmt.Errorf("not found ec shard %d.%d", req.VolumeId, req.ShardId)
	}

	if req.FileKey != 0 {
		_, size, _ := ecVolume.FindNeedleFromEcx(types.Uint64ToNeedleId(req.FileKey))
		if size == types.TombstoneFileSize {
			return stream.Send(&volume_server_pb.VolumeEcShardReadResponse{
				IsDeleted: true,
			})
		}
	}

	bufSize := req.Size
	if bufSize > BufferSizeLimit {
		bufSize = BufferSizeLimit
	}
	buffer := make([]byte, bufSize)

	startOffset, bytesToRead := req.Offset, req.Size

	for bytesToRead > 0 {
		// min of bytesToRead and bufSize
		bufferSize := bufSize
		if bufferSize > bytesToRead {
			bufferSize = bytesToRead
		}
		bytesread, err := ecShard.ReadAt(buffer[0:bufferSize], startOffset)

		// println("read", ecShard.FileName(), "startOffset", startOffset, bytesread, "bytes, with target", bufferSize)
		if bytesread > 0 {

			if int64(bytesread) > bytesToRead {
				bytesread = int(bytesToRead)
			}
			err = stream.Send(&volume_server_pb.VolumeEcShardReadResponse{
				Data: buffer[:bytesread],
			})
			if err != nil {
				// println("sending", bytesread, "bytes err", err.Error())
				return err
			}

			startOffset += int64(bytesread)
			bytesToRead -= int64(bytesread)

		}

		if err != nil {
			if err != io.EOF {
				return err
			}
			return nil
		}

	}

	return nil

}

func (vs *VolumeServer) VolumeEcBlobDelete(ctx context.Context, req *volume_server_pb.VolumeEcBlobDeleteRequest) (*volume_server_pb.VolumeEcBlobDeleteResponse, error) {

	resp := &volume_server_pb.VolumeEcBlobDeleteResponse{}

	for _, location := range vs.store.Locations {
		if localEcVolume, found := location.FindEcVolume(needle.VolumeId(req.VolumeId)); found {

			_, size, _, err := localEcVolume.LocateEcShardNeedle(types.NeedleId(req.FileKey), needle.Version(req.Version))
			if err != nil {
				return nil, fmt.Errorf("locate in local ec volume: %v", err)
			}
			if size == types.TombstoneFileSize {
				return resp, nil
			}

			err = localEcVolume.DeleteNeedleFromEcx(types.NeedleId(req.FileKey))
			if err != nil {
				return nil, err
			}

			break
		}
	}

	return resp, nil
}
