package pluginworkers

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/plugin"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// HarnessConfig configures the shared plugin worker test harness.
type HarnessConfig struct {
	PluginOptions plugin.Options
	WorkerOptions pluginworker.WorkerOptions
	Handlers      []pluginworker.JobHandler
}

// Harness manages an in-process plugin admin server and worker.
type Harness struct {
	t *testing.T

	pluginSvc *plugin.Plugin

	adminServer   *grpc.Server
	adminListener net.Listener
	adminGrpcAddr string

	worker       *pluginworker.Worker
	workerCtx    context.Context
	workerCancel context.CancelFunc
	workerDone   chan struct{}
}

// NewHarness starts a plugin admin gRPC server and a worker connected to it.
func NewHarness(t *testing.T, cfg HarnessConfig) *Harness {
	t.Helper()

	pluginOpts := cfg.PluginOptions
	if pluginOpts.DataDir == "" {
		pluginOpts.DataDir = t.TempDir()
	}

	pluginSvc, err := plugin.New(pluginOpts)
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	adminServer := pb.NewGrpcServer()
	plugin_pb.RegisterPluginControlServiceServer(adminServer, pluginSvc)
	go func() {
		_ = adminServer.Serve(listener)
	}()

	adminGrpcAddr := listener.Addr().String()
	adminPort := listener.Addr().(*net.TCPAddr).Port
	adminAddr := fmt.Sprintf("127.0.0.1:0.%d", adminPort)

	workerOpts := cfg.WorkerOptions
	if workerOpts.AdminServer == "" {
		workerOpts.AdminServer = adminAddr
	}
	if workerOpts.GrpcDialOption == nil {
		workerOpts.GrpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	if workerOpts.WorkerID == "" {
		workerOpts.WorkerID = "plugin-worker-test"
	}
	if workerOpts.WorkerVersion == "" {
		workerOpts.WorkerVersion = "test"
	}
	if workerOpts.WorkerAddress == "" {
		workerOpts.WorkerAddress = "127.0.0.1"
	}
	if len(cfg.Handlers) > 0 {
		workerOpts.Handlers = cfg.Handlers
	}
	if workerOpts.Handler == nil && len(workerOpts.Handlers) == 0 {
		workerOpts.Handlers = cfg.Handlers
	}

	worker, err := pluginworker.NewWorker(workerOpts)
	require.NoError(t, err)

	workerCtx, workerCancel := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		_ = worker.Run(workerCtx)
	}()

	harness := &Harness{
		t:             t,
		pluginSvc:     pluginSvc,
		adminServer:   adminServer,
		adminListener: listener,
		adminGrpcAddr: adminGrpcAddr,
		worker:        worker,
		workerCtx:     workerCtx,
		workerCancel:  workerCancel,
		workerDone:    workerDone,
	}

	require.Eventually(t, func() bool {
		return len(pluginSvc.ListWorkers()) > 0
	}, 5*time.Second, 50*time.Millisecond)

	t.Cleanup(func() {
		harness.Shutdown()
	})

	return harness
}

// Plugin exposes the underlying admin plugin service.
func (h *Harness) Plugin() *plugin.Plugin {
	return h.pluginSvc
}

// AdminGrpcAddress returns the gRPC address for the admin server.
func (h *Harness) AdminGrpcAddress() string {
	return h.adminGrpcAddr
}

// WaitForJobType waits until a worker with the given capability is registered.
func (h *Harness) WaitForJobType(jobType string) {
	h.t.Helper()
	require.Eventually(h.t, func() bool {
		workers := h.pluginSvc.ListWorkers()
		for _, worker := range workers {
			if worker == nil || worker.Capabilities == nil {
				continue
			}
			if _, ok := worker.Capabilities[jobType]; ok {
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond)
}

// Shutdown stops the worker and admin server.
func (h *Harness) Shutdown() {
	if h.workerCancel != nil {
		h.workerCancel()
	}

	if h.workerDone != nil {
		select {
		case <-h.workerDone:
		case <-time.After(2 * time.Second):
		}
	}

	if h.adminServer != nil {
		h.adminServer.GracefulStop()
	}

	if h.adminListener != nil {
		_ = h.adminListener.Close()
	}

	if h.pluginSvc != nil {
		h.pluginSvc.Shutdown()
	}
}
