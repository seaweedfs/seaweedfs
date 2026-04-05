package csi

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/volumev2"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// V2RuntimeBackend is a bounded CSI backend adapter over the new runtime-owned
// RF2 path. It intentionally starts narrow: create/bootstrap plus lookup/publish
// from runtime-owned exports and surfaces.
type V2RuntimeBackend struct {
	manager       *volumev2.InProcessRuntimeManager
	primaryNodeID string
	dataDir       string
	iqnPrefix     string
}

// NewV2RuntimeBackend creates one bounded CSI backend over the V2 runtime path.
func NewV2RuntimeBackend(manager *volumev2.InProcessRuntimeManager, primaryNodeID, dataDir, iqnPrefix string) *V2RuntimeBackend {
	if iqnPrefix == "" {
		iqnPrefix = "iqn.2026-04.com.seaweedfs:v2.csi."
	}
	return &V2RuntimeBackend{
		manager:       manager,
		primaryNodeID: primaryNodeID,
		dataDir:       dataDir,
		iqnPrefix:     iqnPrefix,
	}
}

func (b *V2RuntimeBackend) CreateVolume(ctx context.Context, name string, sizeBytes uint64) (*VolumeInfo, error) {
	if b == nil || b.manager == nil {
		return nil, fmt.Errorf("csi: v2 runtime backend is nil")
	}
	if name == "" {
		return nil, fmt.Errorf("csi: volume name is required")
	}
	if existing, ok := b.manager.ISCSIExport(name); ok {
		return &VolumeInfo{
			VolumeID:      name,
			ISCSIAddr:     existing.Address,
			IQN:           existing.IQN,
			CapacityBytes: sizeBytes,
		}, nil
	}
	path := filepath.Join(b.dataDir, name+".blk")
	if err := b.manager.BootstrapPrimaryVolume(name, b.primaryNodeID, path, 1, blockvol.CreateOptions{
		VolumeSize: sizeBytes,
		BlockSize:  4096,
		WALSize:    64 * 1024 * 1024,
	}); err != nil {
		return nil, err
	}
	export, err := b.manager.ExportVolumeISCSI(name, b.primaryNodeID, "127.0.0.1:0", b.iqnPrefix+name)
	if err != nil {
		return nil, err
	}
	return &VolumeInfo{
		VolumeID:      name,
		ISCSIAddr:     export.Address,
		IQN:           export.IQN,
		CapacityBytes: sizeBytes,
	}, nil
}

func (b *V2RuntimeBackend) DeleteVolume(ctx context.Context, name string) error {
	return fmt.Errorf("csi: v2 runtime backend delete volume %q not yet implemented", name)
}

func (b *V2RuntimeBackend) LookupVolume(ctx context.Context, name string) (*VolumeInfo, error) {
	if b == nil || b.manager == nil {
		return nil, fmt.Errorf("csi: v2 runtime backend is nil")
	}
	export, ok := b.manager.ISCSIExport(name)
	if !ok {
		return nil, fmt.Errorf("volume %q not found", name)
	}
	return &VolumeInfo{
		VolumeID:  name,
		ISCSIAddr: export.Address,
		IQN:       export.IQN,
	}, nil
}

func (b *V2RuntimeBackend) CreateSnapshot(ctx context.Context, volumeID string, snapID uint32) (*SnapshotInfo, error) {
	return nil, fmt.Errorf("csi: v2 runtime backend snapshots are not yet implemented")
}

func (b *V2RuntimeBackend) DeleteSnapshot(ctx context.Context, volumeID string, snapID uint32) error {
	return fmt.Errorf("csi: v2 runtime backend snapshots are not yet implemented")
}

func (b *V2RuntimeBackend) ListSnapshots(ctx context.Context, volumeID string) ([]*SnapshotInfo, error) {
	return nil, fmt.Errorf("csi: v2 runtime backend snapshots are not yet implemented")
}

func (b *V2RuntimeBackend) ExpandVolume(ctx context.Context, volumeID string, newSizeBytes uint64) (uint64, error) {
	return 0, fmt.Errorf("csi: v2 runtime backend expand volume is not yet implemented")
}
