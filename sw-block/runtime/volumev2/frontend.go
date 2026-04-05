package volumev2

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

// ISCSITargetExport is a small handle for one running iSCSI frontend export.
type ISCSITargetExport struct {
	iqn    string
	addr   string
	server *iscsi.TargetServer
}

// IQN returns the exported target name.
func (e *ISCSITargetExport) IQN() string {
	if e == nil {
		return ""
	}
	return e.iqn
}

// Address returns the current listen address.
func (e *ISCSITargetExport) Address() string {
	if e == nil {
		return ""
	}
	return e.addr
}

// Close stops the iSCSI target server.
func (e *ISCSITargetExport) Close() error {
	if e == nil || e.server == nil {
		return nil
	}
	return e.server.Close()
}

// ExportISCSI starts a small iSCSI target server for one named volume.
// This gives the single-node MVP a real block frontend without depending on weed/server.
func (n *Node) ExportISCSI(name, listenAddr, iqn string) (*ISCSITargetExport, error) {
	if n == nil {
		return nil, fmt.Errorf("volumev2: node is nil")
	}
	if listenAddr == "" {
		listenAddr = "127.0.0.1:0"
	}
	if iqn == "" {
		iqn = "iqn.2026-04.com.seaweedfs:v2." + name
	}
	path, err := n.pathFor(name)
	if err != nil {
		return nil, err
	}

	var dev iscsi.BlockDevice
	if err := n.dataPlane.WithVolume(path, func(vol *blockvol.BlockVol) error {
		dev = blockvol.NewBlockVolAdapter(vol)
		return nil
	}); err != nil {
		return nil, err
	}
	if dev == nil {
		return nil, fmt.Errorf("volumev2: no block device for %q", name)
	}

	cfg := iscsi.DefaultTargetConfig()
	cfg.TargetName = iqn
	logger := log.New(io.Discard, "", 0)
	server := iscsi.NewTargetServer(listenAddr, cfg, logger)
	server.AddVolume(iqn, dev)

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("volumev2: iscsi listen %s: %w", listenAddr, err)
	}
	server.SetPortalAddr(ln.Addr().String() + ",1")
	go func() {
		_ = server.Serve(ln)
	}()

	return &ISCSITargetExport{
		iqn:    iqn,
		addr:   ln.Addr().String(),
		server: server,
	}, nil
}
