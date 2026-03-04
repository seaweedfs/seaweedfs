package csi

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
)

// VolumeInfo holds volume metadata returned by the backend.
type VolumeInfo struct {
	VolumeID      string
	ISCSIAddr     string // iSCSI target address (ip:port)
	IQN           string // iSCSI target IQN
	CapacityBytes uint64
}

// VolumeBackend abstracts volume lifecycle for the CSI controller.
type VolumeBackend interface {
	CreateVolume(ctx context.Context, name string, sizeBytes uint64) (*VolumeInfo, error)
	DeleteVolume(ctx context.Context, name string) error
	LookupVolume(ctx context.Context, name string) (*VolumeInfo, error)
}

// LocalVolumeBackend wraps VolumeManager for standalone/local mode (CP6-1).
type LocalVolumeBackend struct {
	mgr *VolumeManager
}

// NewLocalVolumeBackend creates a backend backed by the local VolumeManager.
func NewLocalVolumeBackend(mgr *VolumeManager) *LocalVolumeBackend {
	return &LocalVolumeBackend{mgr: mgr}
}

func (b *LocalVolumeBackend) CreateVolume(ctx context.Context, name string, sizeBytes uint64) (*VolumeInfo, error) {
	if err := b.mgr.CreateVolume(name, sizeBytes); err != nil {
		return nil, err
	}
	actualSize := b.mgr.VolumeSizeBytes(name)
	if actualSize == 0 {
		actualSize = sizeBytes
	}
	return &VolumeInfo{
		VolumeID:      name,
		ISCSIAddr:     b.mgr.ListenAddr(),
		IQN:           b.mgr.VolumeIQN(name),
		CapacityBytes: actualSize,
	}, nil
}

func (b *LocalVolumeBackend) DeleteVolume(ctx context.Context, name string) error {
	return b.mgr.DeleteVolume(name)
}

func (b *LocalVolumeBackend) LookupVolume(ctx context.Context, name string) (*VolumeInfo, error) {
	if !b.mgr.VolumeExists(name) {
		return nil, fmt.Errorf("volume %q not found", name)
	}
	return &VolumeInfo{
		VolumeID:      name,
		ISCSIAddr:     b.mgr.ListenAddr(),
		IQN:           b.mgr.VolumeIQN(name),
		CapacityBytes: b.mgr.VolumeSizeBytes(name),
	}, nil
}

// MasterVolumeClient calls master gRPC for volume operations.
type MasterVolumeClient struct {
	masterAddr string
	dialOpt    grpc.DialOption
}

// NewMasterVolumeClient creates a client that calls the master for volume operations.
func NewMasterVolumeClient(masterAddr string, dialOpt grpc.DialOption) *MasterVolumeClient {
	return &MasterVolumeClient{
		masterAddr: masterAddr,
		dialOpt:    dialOpt,
	}
}

func (c *MasterVolumeClient) CreateVolume(ctx context.Context, name string, sizeBytes uint64) (*VolumeInfo, error) {
	var info *VolumeInfo
	err := pb.WithMasterClient(false, pb.ServerAddress(c.masterAddr), c.dialOpt, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
			Name:      name,
			SizeBytes: sizeBytes,
		})
		if err != nil {
			return err
		}
		info = &VolumeInfo{
			VolumeID:      resp.VolumeId,
			ISCSIAddr:     resp.IscsiAddr,
			IQN:           resp.Iqn,
			CapacityBytes: resp.CapacityBytes,
		}
		return nil
	})
	return info, err
}

func (c *MasterVolumeClient) DeleteVolume(ctx context.Context, name string) error {
	return pb.WithMasterClient(false, pb.ServerAddress(c.masterAddr), c.dialOpt, false, func(client master_pb.SeaweedClient) error {
		_, err := client.DeleteBlockVolume(ctx, &master_pb.DeleteBlockVolumeRequest{
			Name: name,
		})
		return err
	})
}

func (c *MasterVolumeClient) LookupVolume(ctx context.Context, name string) (*VolumeInfo, error) {
	var info *VolumeInfo
	err := pb.WithMasterClient(false, pb.ServerAddress(c.masterAddr), c.dialOpt, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{
			Name: name,
		})
		if err != nil {
			return err
		}
		info = &VolumeInfo{
			VolumeID:      name,
			ISCSIAddr:     resp.IscsiAddr,
			IQN:           resp.Iqn,
			CapacityBytes: resp.CapacityBytes,
		}
		return nil
	})
	return info, err
}
