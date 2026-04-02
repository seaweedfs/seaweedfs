package csi

import (
	"context"
	"log"
)

// ExportedControllerServer wraps controllerServer for cross-package testing.
type ExportedControllerServer struct {
	*controllerServer
}

// NewExportedControllerServer creates a controller server with the given backend.
func NewExportedControllerServer(backend VolumeBackend) *ExportedControllerServer {
	return &ExportedControllerServer{&controllerServer{backend: backend}}
}

// StagedInfo holds exported staged volume info for cross-package test assertions.
type StagedInfo struct {
	IsLocal   bool
	ISCSIAddr string
	IQN       string
	Transport string
}

// ExportedNodeServer wraps nodeServer for cross-package testing.
type ExportedNodeServer struct {
	*nodeServer
}

// NewExportedNodeServer creates a node server with the given parameters.
// Pass mgr=nil to prove publish_context consumption (no local fallback).
// Uses NoopISCSI and NoopMount for cross-package test use.
func NewExportedNodeServer(nodeID string, mgr *VolumeManager, logger *log.Logger) *ExportedNodeServer {
	return &ExportedNodeServer{&nodeServer{
		mgr:       mgr,
		nodeID:    nodeID,
		iscsiUtil: &noopISCSI{},
		mountUtil: &noopMount{},
		logger:    logger,
		staged:    map[string]*stagedVolumeInfo{},
	}}
}

// GetStagedInfo returns exported staging info for a volume.
func (e *ExportedNodeServer) GetStagedInfo(volumeID string) *StagedInfo {
	e.stagedMu.Lock()
	defer e.stagedMu.Unlock()
	info, ok := e.staged[volumeID]
	if !ok {
		return nil
	}
	return &StagedInfo{
		IsLocal:   info.isLocal,
		ISCSIAddr: info.iscsiAddr,
		IQN:       info.iqn,
		Transport: info.transport,
	}
}

// noopISCSI implements ISCSIUtil with no real iSCSI operations.
type noopISCSI struct{}

func (n *noopISCSI) Discovery(ctx context.Context, portal string) error         { return nil }
func (n *noopISCSI) Login(ctx context.Context, iqn, portal string) error        { return nil }
func (n *noopISCSI) Logout(ctx context.Context, iqn string) error               { return nil }
func (n *noopISCSI) GetDeviceByIQN(ctx context.Context, iqn string) (string, error) {
	return "/dev/sda-noop", nil
}
func (n *noopISCSI) IsLoggedIn(ctx context.Context, iqn string) (bool, error) { return false, nil }
func (n *noopISCSI) RescanDevice(ctx context.Context, iqn string) error       { return nil }

// noopMount implements MountUtil with no real mount operations.
type noopMount struct{}

func (n *noopMount) FormatAndMount(ctx context.Context, device, target, fsType string) error { return nil }
func (n *noopMount) Mount(ctx context.Context, source, target, fsType string, readOnly bool) error {
	return nil
}
func (n *noopMount) BindMount(ctx context.Context, source, target string, readOnly bool) error {
	return nil
}
func (n *noopMount) Unmount(ctx context.Context, target string) error                        { return nil }
func (n *noopMount) IsFormatted(ctx context.Context, device string) (bool, error)            { return true, nil }
func (n *noopMount) IsMounted(ctx context.Context, target string) (bool, error)              { return false, nil }
func (n *noopMount) ResizeFS(ctx context.Context, devicePath, mountPath, fsType string) error { return nil }
