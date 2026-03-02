package blockvol

import (
	"fmt"
	"os"
)

// Epoch returns the current epoch of this volume.
func (v *BlockVol) Epoch() uint64 {
	return v.epoch.Load()
}

// SetEpoch persists a new epoch to the superblock and fsyncs.
// Must be durable before writes are accepted at the new epoch.
func (v *BlockVol) SetEpoch(epoch uint64) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.super.Epoch = epoch
	v.epoch.Store(epoch)

	if _, err := v.fd.Seek(0, os.SEEK_SET); err != nil {
		return fmt.Errorf("blockvol: seek superblock: %w", err)
	}
	if _, err := v.super.WriteTo(v.fd); err != nil {
		return fmt.Errorf("blockvol: write superblock: %w", err)
	}
	if err := v.fd.Sync(); err != nil {
		return fmt.Errorf("blockvol: sync superblock: %w", err)
	}
	return nil
}

// SetMasterEpoch sets the expected epoch from the master.
// Writes are rejected if v.epoch != v.masterEpoch (when role != RoleNone).
func (v *BlockVol) SetMasterEpoch(epoch uint64) {
	v.masterEpoch.Store(epoch)
}
