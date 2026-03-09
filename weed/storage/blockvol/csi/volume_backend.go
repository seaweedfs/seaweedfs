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
	NvmeAddr      string // NVMe/TCP target address (ip:port), empty if NVMe disabled
	NQN           string // NVMe subsystem NQN, empty if NVMe disabled
	CapacityBytes uint64
}

// SnapshotInfo holds snapshot metadata returned by the backend.
type SnapshotInfo struct {
	SnapshotID uint32
	VolumeID   string
	CreatedAt  int64  // Unix timestamp in seconds
	SizeBytes  uint64
}

// VolumeBackend abstracts volume lifecycle for the CSI controller.
type VolumeBackend interface {
	CreateVolume(ctx context.Context, name string, sizeBytes uint64) (*VolumeInfo, error)
	DeleteVolume(ctx context.Context, name string) error
	LookupVolume(ctx context.Context, name string) (*VolumeInfo, error)

	CreateSnapshot(ctx context.Context, volumeID string, snapID uint32) (*SnapshotInfo, error)
	DeleteSnapshot(ctx context.Context, volumeID string, snapID uint32) error
	ListSnapshots(ctx context.Context, volumeID string) ([]*SnapshotInfo, error)
	ExpandVolume(ctx context.Context, volumeID string, newSizeBytes uint64) (uint64, error)
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
		NvmeAddr:      b.mgr.NvmeAddr(),
		NQN:           b.mgr.VolumeNQN(name),
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
		NvmeAddr:      b.mgr.NvmeAddr(),
		NQN:           b.mgr.VolumeNQN(name),
		CapacityBytes: b.mgr.VolumeSizeBytes(name),
	}, nil
}

func (b *LocalVolumeBackend) CreateSnapshot(ctx context.Context, volumeID string, snapID uint32) (*SnapshotInfo, error) {
	info, err := b.mgr.CreateSnapshot(volumeID, snapID)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (b *LocalVolumeBackend) DeleteSnapshot(ctx context.Context, volumeID string, snapID uint32) error {
	return b.mgr.DeleteSnapshot(volumeID, snapID)
}

func (b *LocalVolumeBackend) ListSnapshots(ctx context.Context, volumeID string) ([]*SnapshotInfo, error) {
	return b.mgr.ListSnapshots(volumeID)
}

func (b *LocalVolumeBackend) ExpandVolume(ctx context.Context, volumeID string, newSizeBytes uint64) (uint64, error) {
	return b.mgr.ExpandVolume(volumeID, newSizeBytes)
}

// MasterVolumeClient calls master gRPC for volume operations.
// NOTE: NvmeAddr/NQN fields in VolumeInfo are NOT populated by MasterVolumeClient
// because the master proto (CreateBlockVolumeResponse, LookupBlockVolumeResponse)
// does not yet have nvme_addr/nqn fields. This is deferred until proto is updated
// in a future CP. NVMe support via master-backend path is therefore iSCSI-only
// until that proto change lands.
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

func (c *MasterVolumeClient) CreateSnapshot(ctx context.Context, volumeID string, snapID uint32) (*SnapshotInfo, error) {
	var info *SnapshotInfo
	err := pb.WithMasterClient(false, pb.ServerAddress(c.masterAddr), c.dialOpt, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.CreateBlockSnapshot(ctx, &master_pb.CreateBlockSnapshotRequest{
			VolumeName: volumeID,
			SnapshotId: snapID,
		})
		if err != nil {
			return err
		}
		info = &SnapshotInfo{
			SnapshotID: resp.SnapshotId,
			VolumeID:   volumeID,
			CreatedAt:  resp.CreatedAt,
			SizeBytes:  resp.SizeBytes,
		}
		return nil
	})
	return info, err
}

func (c *MasterVolumeClient) DeleteSnapshot(ctx context.Context, volumeID string, snapID uint32) error {
	return pb.WithMasterClient(false, pb.ServerAddress(c.masterAddr), c.dialOpt, false, func(client master_pb.SeaweedClient) error {
		_, err := client.DeleteBlockSnapshot(ctx, &master_pb.DeleteBlockSnapshotRequest{
			VolumeName: volumeID,
			SnapshotId: snapID,
		})
		return err
	})
}

func (c *MasterVolumeClient) ListSnapshots(ctx context.Context, volumeID string) ([]*SnapshotInfo, error) {
	var infos []*SnapshotInfo
	err := pb.WithMasterClient(false, pb.ServerAddress(c.masterAddr), c.dialOpt, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListBlockSnapshots(ctx, &master_pb.ListBlockSnapshotsRequest{
			VolumeName: volumeID,
		})
		if err != nil {
			return err
		}
		for _, s := range resp.Snapshots {
			infos = append(infos, &SnapshotInfo{
				SnapshotID: s.SnapshotId,
				VolumeID:   volumeID,
				CreatedAt:  s.CreatedAt,
				SizeBytes:  s.VolumeSizeBytes,
			})
		}
		return nil
	})
	return infos, err
}

func (c *MasterVolumeClient) ExpandVolume(ctx context.Context, volumeID string, newSizeBytes uint64) (uint64, error) {
	var capacity uint64
	err := pb.WithMasterClient(false, pb.ServerAddress(c.masterAddr), c.dialOpt, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ExpandBlockVolume(ctx, &master_pb.ExpandBlockVolumeRequest{
			Name:         volumeID,
			NewSizeBytes: newSizeBytes,
		})
		if err != nil {
			return err
		}
		capacity = resp.CapacityBytes
		return nil
	})
	return capacity, err
}
