package weed_server

import (
	"context"
	"fmt"
	"math"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

/*

Steps to apply erasure coding to .dat .idx files
0. ensure the volume is readonly
1. client call VolumeEcGenerateSlices to generate the .ecx and .ec01~.ec14 files
2. client ask master for possible servers to hold the ec files, at least 4 servers
3. client call VolumeEcCopy on above target servers to copy ec files from the source server
4. target servers report the new ec files to the master
5.   master stores vid -> [14]*DataNode
6. client checks master. If all 14 slices are ready, delete the original .idx, .idx files

 */

// VolumeEcGenerateSlices generates the .ecx and .ec01 ~ .ec14 files
func (vs *VolumeServer) VolumeEcGenerateSlices(ctx context.Context, req *volume_server_pb.VolumeEcGenerateSlicesRequest) (*volume_server_pb.VolumeEcGenerateSlicesResponse, error) {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("volume %d not found", req.VolumeId)
	}
	baseFileName := v.FileName()

	// write .ecx file
	if err := erasure_coding.WriteSortedEcxFile(baseFileName); err != nil {
		return nil, fmt.Errorf("WriteSortedEcxFile %s: %v", baseFileName, err)
	}

	// write .ec01 ~ .ec14 files
	if err := erasure_coding.WriteEcFiles(baseFileName); err != nil {
		return nil, fmt.Errorf("WriteEcFiles %s: %v", baseFileName, err)
	}


	return &volume_server_pb.VolumeEcGenerateSlicesResponse{}, nil
}

// VolumeEcCopy copy the .ecx and some ec data slices
func (vs *VolumeServer) VolumeEcCopy(ctx context.Context, req *volume_server_pb.VolumeEcCopyRequest) (*volume_server_pb.VolumeEcCopyResponse, error) {

	location := vs.store.FindFreeLocation()
	if location == nil {
		return nil, fmt.Errorf("no space left")
	}

	baseFileName := storage.VolumeFileName(req.Collection, location.Directory, int(req.VolumeId))

	err := operation.WithVolumeServerClient(req.SourceDataNode, vs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// copy ecx file
		if err:=vs.doCopyFile(ctx, client, req.VolumeId, math.MaxUint32, math.MaxUint64, baseFileName, ".ecx"); err!=nil{
			return err
		}

		// copy ec data slices
		for _, ecIndex := range req.EcIndexes {
			if err:=vs.doCopyFile(ctx, client, req.VolumeId, math.MaxUint32, math.MaxUint64, baseFileName, erasure_coding.ToExt(int(ecIndex))); err!=nil{
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("VolumeEcCopy volume %d: %v", req.VolumeId, err)
	}

	return &volume_server_pb.VolumeEcCopyResponse{}, nil
}
