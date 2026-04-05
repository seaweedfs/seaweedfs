package volumev2

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/purev2"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// DataPlane is the minimal single-node execution contract for volumev2.
// It keeps backend mechanics replaceable while volumev2 owns the control shell.
type DataPlane interface {
	BootstrapPrimary(path string, opts blockvol.CreateOptions, epoch uint64, leaseTTL time.Duration) error
	WriteLBA(path string, lba uint64, data []byte) error
	ReadLBA(path string, lba uint64, length uint32) ([]byte, error)
	SyncCache(path string) error
	Snapshot(path string) (purev2.VolumeDebugSnapshot, error)
	WithVolume(path string, fn func(*blockvol.BlockVol) error) error
	Close()
}

// PureRuntimeDataPlane adapts the current purev2 runtime as a volumev2 data plane.
type PureRuntimeDataPlane struct {
	runtime *purev2.Runtime
}

// NewPureRuntimeDataPlane wraps one purev2 runtime behind the DataPlane contract.
func NewPureRuntimeDataPlane(runtime *purev2.Runtime) (*PureRuntimeDataPlane, error) {
	if runtime == nil {
		return nil, fmt.Errorf("volumev2: pure runtime is nil")
	}
	return &PureRuntimeDataPlane{runtime: runtime}, nil
}

func (dp *PureRuntimeDataPlane) BootstrapPrimary(path string, opts blockvol.CreateOptions, epoch uint64, leaseTTL time.Duration) error {
	if dp == nil || dp.runtime == nil {
		return fmt.Errorf("volumev2: data plane is nil")
	}
	return dp.runtime.BootstrapPrimary(path, opts, epoch, leaseTTL)
}

func (dp *PureRuntimeDataPlane) WriteLBA(path string, lba uint64, data []byte) error {
	if dp == nil || dp.runtime == nil {
		return fmt.Errorf("volumev2: data plane is nil")
	}
	return dp.runtime.WriteLBA(path, lba, data)
}

func (dp *PureRuntimeDataPlane) ReadLBA(path string, lba uint64, length uint32) ([]byte, error) {
	if dp == nil || dp.runtime == nil {
		return nil, fmt.Errorf("volumev2: data plane is nil")
	}
	return dp.runtime.ReadLBA(path, lba, length)
}

func (dp *PureRuntimeDataPlane) SyncCache(path string) error {
	if dp == nil || dp.runtime == nil {
		return fmt.Errorf("volumev2: data plane is nil")
	}
	return dp.runtime.SyncCache(path)
}

func (dp *PureRuntimeDataPlane) Snapshot(path string) (purev2.VolumeDebugSnapshot, error) {
	if dp == nil || dp.runtime == nil {
		return purev2.VolumeDebugSnapshot{}, fmt.Errorf("volumev2: data plane is nil")
	}
	return dp.runtime.Snapshot(path)
}

func (dp *PureRuntimeDataPlane) WithVolume(path string, fn func(*blockvol.BlockVol) error) error {
	if dp == nil || dp.runtime == nil {
		return fmt.Errorf("volumev2: data plane is nil")
	}
	return dp.runtime.WithVolume(path, fn)
}

func (dp *PureRuntimeDataPlane) Close() {
	if dp == nil || dp.runtime == nil {
		return
	}
	dp.runtime.Close()
}
