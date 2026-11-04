package pluginworker

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type authCaptureAdmin struct {
	plugin_pb.UnimplementedPluginControlServiceServer
	signingKey security.SigningKey
	streamErr  chan error
}

func (s *authCaptureAdmin) WorkerStream(stream plugin_pb.PluginControlService_WorkerStreamServer) error {
	err := s.serve(stream)
	s.streamErr <- err
	return err
}

func (s *authCaptureAdmin) serve(stream plugin_pb.PluginControlService_WorkerStreamServer) error {
	if err := security.CheckGrpcAdminPeer(stream.Context(), s.signingKey); err != nil {
		return err
	}
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(&plugin_pb.AdminToWorkerMessage{
		RequestId: "exec-auth-1",
		Body: &plugin_pb.AdminToWorkerMessage_ExecuteJobRequest{
			ExecuteJobRequest: &plugin_pb.ExecuteJobRequest{
				Job: &plugin_pb.JobSpec{JobId: "job-auth-1", JobType: "vacuum"},
			},
		},
	}); err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		if msg.GetJobCompleted() != nil {
			return nil
		}
	}
}

type outgoingAuthCaptureHandler struct {
	testJobHandler
	authorization chan []string
}

func (h *outgoingAuthCaptureHandler) Execute(ctx context.Context, _ *plugin_pb.ExecuteJobRequest, sender ExecutionSender) error {
	md, _ := metadata.FromOutgoingContext(ctx)
	h.authorization <- md.Get("authorization")
	return sender.SendCompleted(&plugin_pb.JobCompleted{Success: true})
}

func startAuthCaptureWorker(t *testing.T, ctx context.Context, handler JobHandler) *authCaptureAdmin {
	t.Helper()
	const signingKey = "plugin-worker-stream-auth-key"
	util.GetViper().Set("jwt.filer_signing.key", signingKey)
	t.Cleanup(func() { util.GetViper().Set("jwt.filer_signing.key", "") })
	util.GetViper().Set("jwt.filer_signing.expires_after_seconds", 10)
	t.Cleanup(func() { util.GetViper().Set("jwt.filer_signing.expires_after_seconds", 0) })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	admin := &authCaptureAdmin{signingKey: security.SigningKey(signingKey), streamErr: make(chan error, 1)}
	server := grpc.NewServer()
	plugin_pb.RegisterPluginControlServiceServer(server, admin)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	worker, err := NewWorker(WorkerOptions{
		AdminServer:             listener.Addr().String(),
		GrpcDialOption:          grpc.WithTransportCredentials(insecure.NewCredentials()),
		Handler:                 handler,
		MaxExecutionConcurrency: 1,
	})
	if err != nil {
		t.Fatalf("NewWorker error = %v", err)
	}
	go worker.runOnce(ctx, listener.Addr().String())
	return admin
}

func vacuumTestJobHandler() testJobHandler {
	return testJobHandler{
		capability: &plugin_pb.JobTypeCapability{JobType: "vacuum", CanExecute: true},
		descriptor: &plugin_pb.JobTypeDescriptor{JobType: "vacuum"},
	}
}

func TestWorkerJobContextDoesNotCarryStreamToken(t *testing.T) {
	handler := &outgoingAuthCaptureHandler{
		testJobHandler: vacuumTestJobHandler(),
		authorization:  make(chan []string, 1),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin := startAuthCaptureWorker(t, ctx, handler)

	select {
	case err := <-admin.streamErr:
		if err != nil {
			t.Fatalf("admin stream: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for the job to complete")
	}
	select {
	case got := <-handler.authorization:
		if len(got) != 0 {
			t.Fatalf("job context forwards the stream's admin token: %v", got)
		}
	default:
		t.Fatal("job completed without running the handler")
	}
}

type blockingCancelHandler struct {
	testJobHandler
	started  chan struct{}
	canceled chan struct{}
}

func (h *blockingCancelHandler) Execute(ctx context.Context, _ *plugin_pb.ExecuteJobRequest, _ ExecutionSender) error {
	close(h.started)
	<-ctx.Done()
	close(h.canceled)
	return ctx.Err()
}

func TestWorkerConnectionTeardownCancelsJobContext(t *testing.T) {
	handler := &blockingCancelHandler{
		testJobHandler: vacuumTestJobHandler(),
		started:        make(chan struct{}),
		canceled:       make(chan struct{}),
	}
	connCtx, cancelConn := context.WithCancel(context.Background())
	defer cancelConn()
	startAuthCaptureWorker(t, connCtx, handler)

	timeout := time.After(10 * time.Second)
	select {
	case <-handler.started:
	case <-timeout:
		t.Fatal("timed out waiting for the job to start")
	}
	cancelConn()
	select {
	case <-handler.canceled:
	case <-timeout:
		t.Fatal("job context outlived the worker connection")
	}
}
