package pluginworker

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestWorkerBuildHelloUsesConfiguredConcurrency(t *testing.T) {
	handler := &testJobHandler{
		capability: &plugin_pb.JobTypeCapability{
			JobType:                 "vacuum",
			CanDetect:               true,
			CanExecute:              true,
			MaxDetectionConcurrency: 99,
			MaxExecutionConcurrency: 88,
		},
		descriptor: &plugin_pb.JobTypeDescriptor{JobType: "vacuum"},
	}

	worker, err := NewWorker(WorkerOptions{
		AdminServer:             "localhost:23646",
		GrpcDialOption:          grpc.WithTransportCredentials(insecure.NewCredentials()),
		Handler:                 handler,
		MaxDetectionConcurrency: 3,
		MaxExecutionConcurrency: 4,
	})
	if err != nil {
		t.Fatalf("NewWorker error = %v", err)
	}

	hello := worker.buildHello()
	if hello == nil || len(hello.Capabilities) != 1 {
		t.Fatalf("expected one capability in hello")
	}
	capability := hello.Capabilities[0]
	if capability.MaxDetectionConcurrency != 3 {
		t.Fatalf("expected max_detection_concurrency=3, got=%d", capability.MaxDetectionConcurrency)
	}
	if capability.MaxExecutionConcurrency != 4 {
		t.Fatalf("expected max_execution_concurrency=4, got=%d", capability.MaxExecutionConcurrency)
	}
	if capability.JobType != "vacuum" {
		t.Fatalf("expected job type vacuum, got=%q", capability.JobType)
	}
}

type testJobHandler struct {
	capability *plugin_pb.JobTypeCapability
	descriptor *plugin_pb.JobTypeDescriptor
}

func (h *testJobHandler) Capability() *plugin_pb.JobTypeCapability {
	return h.capability
}

func (h *testJobHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return h.descriptor
}

func (h *testJobHandler) Detect(context.Context, *plugin_pb.RunDetectionRequest, DetectionSender) error {
	return nil
}

func (h *testJobHandler) Execute(context.Context, *plugin_pb.ExecuteJobRequest, ExecutionSender) error {
	return nil
}
