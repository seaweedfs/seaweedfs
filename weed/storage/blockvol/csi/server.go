package csi

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
)

// DriverConfig holds configuration for the CSI driver.
type DriverConfig struct {
	Endpoint  string // CSI endpoint (unix:///csi/csi.sock)
	DataDir   string // volume data directory
	ISCSIAddr string // local iSCSI target listen address
	IQNPrefix string // IQN prefix for volumes
	NodeID    string // node identifier
	Logger    *log.Logger

	// Control-plane mode fields.
	MasterAddr string // master address for control-plane mode (empty = local/standalone)
	Mode       string // "controller", "node", "all" (default "all")
}

// CSIDriver manages the gRPC server and CSI services.
type CSIDriver struct {
	identity   *identityServer
	controller *controllerServer
	node       *nodeServer
	mgr        *VolumeManager
	server     *grpc.Server
	endpoint   string
	logger     *log.Logger
}

// NewCSIDriver creates a new CSI driver from the given configuration.
func NewCSIDriver(cfg DriverConfig) (*CSIDriver, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("csi: node ID is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	if cfg.Mode == "" {
		cfg.Mode = "all"
	}
	switch cfg.Mode {
	case "controller", "node", "all":
		// valid
	default:
		return nil, fmt.Errorf("csi: invalid mode %q, must be controller/node/all", cfg.Mode)
	}

	d := &CSIDriver{
		identity: &identityServer{},
		endpoint: cfg.Endpoint,
		logger:   cfg.Logger,
	}

	// Create VolumeManager for modes that need local volume management.
	var mgr *VolumeManager
	needsLocalMgr := cfg.Mode == "all" && cfg.MasterAddr == "" || cfg.Mode == "node"
	if needsLocalMgr {
		mgr = NewVolumeManager(cfg.DataDir, cfg.ISCSIAddr, cfg.IQNPrefix, cfg.Logger)
		d.mgr = mgr
	}

	// Create backend for controller.
	var backend VolumeBackend
	if cfg.Mode == "controller" || cfg.Mode == "all" {
		if cfg.MasterAddr != "" {
			backend = NewMasterVolumeClient(cfg.MasterAddr, nil)
		} else if mgr != nil {
			backend = NewLocalVolumeBackend(mgr)
		} else {
			return nil, fmt.Errorf("csi: controller mode requires either --master or --data-dir")
		}
		d.controller = &controllerServer{backend: backend}
	}

	// Create node server.
	if cfg.Mode == "node" || cfg.Mode == "all" {
		d.node = &nodeServer{
			mgr:       mgr, // may be nil in controller-only mode
			nodeID:    cfg.NodeID,
			iqnPrefix: cfg.IQNPrefix,
			iscsiUtil: &realISCSIUtil{},
			mountUtil: &realMountUtil{},
			logger:    cfg.Logger,
			staged:    make(map[string]*stagedVolumeInfo),
		}
	}

	return d, nil
}

// Run starts the volume manager and gRPC server. Blocks until Stop is called.
func (d *CSIDriver) Run() error {
	if d.mgr != nil {
		if err := d.mgr.Start(context.Background()); err != nil {
			return fmt.Errorf("csi: start volume manager: %w", err)
		}
	}

	// Parse endpoint URL.
	proto, addr, err := parseEndpoint(d.endpoint)
	if err != nil {
		return fmt.Errorf("csi: parse endpoint: %w", err)
	}

	// Remove existing socket file if present.
	if proto == "unix" {
		os.Remove(addr)
	}

	ln, err := net.Listen(proto, addr)
	if err != nil {
		return fmt.Errorf("csi: listen %s: %w", d.endpoint, err)
	}

	d.server = grpc.NewServer()
	csi.RegisterIdentityServer(d.server, d.identity)
	if d.controller != nil {
		csi.RegisterControllerServer(d.server, d.controller)
	}
	if d.node != nil {
		csi.RegisterNodeServer(d.server, d.node)
	}

	d.logger.Printf("CSI driver serving on %s", d.endpoint)
	return d.server.Serve(ln)
}

// Stop gracefully shuts down the gRPC server and volume manager.
func (d *CSIDriver) Stop() {
	if d.server != nil {
		d.server.GracefulStop()
	}
	if d.mgr != nil {
		d.mgr.Stop()
	}
}

// parseEndpoint parses a CSI endpoint string (unix:///path or tcp://host:port).
func parseEndpoint(ep string) (string, string, error) {
	if strings.HasPrefix(ep, "unix://") {
		u, err := url.Parse(ep)
		if err != nil {
			return "", "", err
		}
		addr := u.Path
		if u.Host != "" {
			addr = u.Host + addr
		}
		return "unix", addr, nil
	}
	if strings.HasPrefix(ep, "tcp://") {
		u, err := url.Parse(ep)
		if err != nil {
			return "", "", err
		}
		return "tcp", u.Host, nil
	}
	return "", "", fmt.Errorf("unsupported endpoint scheme: %s", ep)
}
