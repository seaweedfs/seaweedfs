package weed_server

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"io"
	"os"
)

func (vs *VolumeServer) ReplicateVolume(ctx context.Context, req *volume_server_pb.ReplicateVolumeRequest) (*volume_server_pb.ReplicateVolumeResponse, error) {

	v := vs.store.GetVolume(storage.VolumeId(req.VolumeId))
	if v != nil {
		// unmount the volume
		err := vs.store.UnmountVolume(storage.VolumeId(req.VolumeId))
		if err != nil {
			return nil, fmt.Errorf("failed to unmount volume %d: %v", req.VolumeId, err)
		}
	}

	location := vs.store.FindFreeLocation()
	if location == nil {
		return nil, fmt.Errorf("no space left")
	}

	volumeFileName := storage.VolumeFileName(req.Collection, location.Directory, int(req.VolumeId))

	// the master will not start compaction for read-only volumes, so it is safe to just copy files directly
	// copy .dat and .idx files
	//   read .idx .dat file size and timestamp
	//   send .idx file
	//   send .dat file
	//   confirm size and timestamp

	err := operation.WithVolumeServerClient(req.SourceDataNode, vs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// TODO read file sizes before copying
		client.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{})

		copyFileClient, err := client.CopyFile(ctx, &volume_server_pb.CopyFileRequest{
			VolumeId:  req.VolumeId,
			IsIdxFile: true,
		})

		if err != nil {
			return fmt.Errorf("failed to start copying volume %d idx file: %v", req.VolumeId, err)
		}

		err = writeToFile(copyFileClient, volumeFileName+".idx")
		if err != nil {
			return fmt.Errorf("failed to copy volume %d idx file: %v", req.VolumeId, err)
		}

		copyFileClient, err = client.CopyFile(ctx, &volume_server_pb.CopyFileRequest{
			VolumeId:  req.VolumeId,
			IsDatFile: true,
		})

		if err != nil {
			return fmt.Errorf("failed to start copying volume %d dat file: %v", req.VolumeId, err)
		}

		err = writeToFile(copyFileClient, volumeFileName+".dat")
		if err != nil {
			return fmt.Errorf("failed to copy volume %d dat file: %v", req.VolumeId, err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// TODO: check the timestamp and size

	// mount the volume
	err = vs.store.MountVolume(storage.VolumeId(req.VolumeId))
	if err != nil {
		return nil, fmt.Errorf("failed to mount volume %d: %v", req.VolumeId, err)
	}

	return &volume_server_pb.ReplicateVolumeResponse{}, err

}

func writeToFile(client volume_server_pb.VolumeServer_CopyFileClient, fileName string) error {
	println("writing to ", fileName)
	dst, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil
	}
	defer dst.Close()

	for {
		resp, receiveErr := client.Recv()
		if receiveErr == io.EOF {
			break
		}
		if receiveErr != nil {
			return fmt.Errorf("receiving %s: %v", fileName, receiveErr)
		}
		dst.Write(resp.FileContent)
	}
	return nil
}

func (vs *VolumeServer) ReadVolumeFileStatus(ctx context.Context, req *volume_server_pb.ReadVolumeFileStatusRequest) (*volume_server_pb.ReadVolumeFileStatusResponse, error) {
	resp := &volume_server_pb.ReadVolumeFileStatusResponse{}
	return resp, nil
}

func (vs *VolumeServer) CopyFile(req *volume_server_pb.CopyFileRequest, stream volume_server_pb.VolumeServer_CopyFileServer) (error) {

	v := vs.store.GetVolume(storage.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	const BufferSize = 1024 * 16
	var fileName = v.FileName()
	if req.IsDatFile {
		fileName += ".dat"
	} else if req.IsIdxFile {
		fileName += ".idx"
	}
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	buffer := make([]byte, BufferSize)

	for {
		bytesread, err := file.Read(buffer)

		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}

		stream.Send(&volume_server_pb.CopyFileResponse{
			FileContent: buffer[:bytesread],
		})

	}

	return nil
}
