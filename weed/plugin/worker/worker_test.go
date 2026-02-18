package pluginworker

import (
	"context"
	"testing"
	"time"

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

func TestWorkerBuildHelloIncludesMultipleCapabilities(t *testing.T) {
	worker, err := NewWorker(WorkerOptions{
		AdminServer:    "localhost:23646",
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		Handlers: []JobHandler{
			&testJobHandler{
				capability: &plugin_pb.JobTypeCapability{JobType: "vacuum", CanDetect: true, CanExecute: true},
				descriptor: &plugin_pb.JobTypeDescriptor{JobType: "vacuum"},
			},
			&testJobHandler{
				capability: &plugin_pb.JobTypeCapability{JobType: "volume_balance", CanDetect: true, CanExecute: true},
				descriptor: &plugin_pb.JobTypeDescriptor{JobType: "volume_balance"},
			},
		},
		MaxDetectionConcurrency: 2,
		MaxExecutionConcurrency: 3,
	})
	if err != nil {
		t.Fatalf("NewWorker error = %v", err)
	}

	hello := worker.buildHello()
	if hello == nil || len(hello.Capabilities) != 2 {
		t.Fatalf("expected two capabilities in hello")
	}

	found := map[string]bool{}
	for _, capability := range hello.Capabilities {
		found[capability.JobType] = true
		if capability.MaxDetectionConcurrency != 2 {
			t.Fatalf("expected max_detection_concurrency=2, got=%d", capability.MaxDetectionConcurrency)
		}
		if capability.MaxExecutionConcurrency != 3 {
			t.Fatalf("expected max_execution_concurrency=3, got=%d", capability.MaxExecutionConcurrency)
		}
	}
	if !found["vacuum"] || !found["volume_balance"] {
		t.Fatalf("expected capabilities for vacuum and volume_balance, got=%v", found)
	}
}

func TestWorkerCancelWorkByTargetID(t *testing.T) {
	worker, err := NewWorker(WorkerOptions{
		AdminServer:    "localhost:23646",
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		Handler: &testJobHandler{
			capability: &plugin_pb.JobTypeCapability{JobType: "vacuum"},
			descriptor: &plugin_pb.JobTypeDescriptor{JobType: "vacuum"},
		},
	})
	if err != nil {
		t.Fatalf("NewWorker error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	worker.setWorkCancel(cancel, "request-1", "job-1")

	if !worker.cancelWork("request-1") {
		t.Fatalf("expected cancel by request id to succeed")
	}
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected context to be canceled")
	}

	if !worker.cancelWork("job-1") {
		t.Fatalf("expected cancel by job id to succeed")
	}
	if worker.cancelWork("unknown-target") {
		t.Fatalf("expected cancel unknown target to fail")
	}
}

func TestWorkerHandleCancelRequestAck(t *testing.T) {
	worker, err := NewWorker(WorkerOptions{
		AdminServer:    "localhost:23646",
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		Handler: &testJobHandler{
			capability: &plugin_pb.JobTypeCapability{JobType: "vacuum"},
			descriptor: &plugin_pb.JobTypeDescriptor{JobType: "vacuum"},
		},
	})
	if err != nil {
		t.Fatalf("NewWorker error = %v", err)
	}

	canceled := false
	worker.setWorkCancel(func() { canceled = true }, "job-42")

	var response *plugin_pb.WorkerToAdminMessage
	ok := worker.handleAdminMessageForTest(&plugin_pb.AdminToWorkerMessage{
		RequestId: "cancel-req-1",
		Body: &plugin_pb.AdminToWorkerMessage_CancelRequest{
			CancelRequest: &plugin_pb.CancelRequest{TargetId: "job-42"},
		},
	}, func(msg *plugin_pb.WorkerToAdminMessage) bool {
		response = msg
		return true
	})
	if !ok {
		t.Fatalf("expected send callback to be invoked")
	}
	if !canceled {
		t.Fatalf("expected registered work cancel function to be called")
	}
	if response == nil || response.GetAcknowledge() == nil || !response.GetAcknowledge().Accepted {
		t.Fatalf("expected accepted acknowledge response, got=%+v", response)
	}

	response = nil
	ok = worker.handleAdminMessageForTest(&plugin_pb.AdminToWorkerMessage{
		RequestId: "cancel-req-2",
		Body: &plugin_pb.AdminToWorkerMessage_CancelRequest{
			CancelRequest: &plugin_pb.CancelRequest{TargetId: "missing"},
		},
	}, func(msg *plugin_pb.WorkerToAdminMessage) bool {
		response = msg
		return true
	})
	if !ok {
		t.Fatalf("expected send callback to be invoked")
	}
	if response == nil || response.GetAcknowledge() == nil || response.GetAcknowledge().Accepted {
		t.Fatalf("expected rejected acknowledge for missing target, got=%+v", response)
	}
}

func TestWorkerSchemaRequestRequiresJobTypeWhenMultipleHandlers(t *testing.T) {
	worker, err := NewWorker(WorkerOptions{
		AdminServer:    "localhost:23646",
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		Handlers: []JobHandler{
			&testJobHandler{
				capability: &plugin_pb.JobTypeCapability{JobType: "vacuum"},
				descriptor: &plugin_pb.JobTypeDescriptor{JobType: "vacuum"},
			},
			&testJobHandler{
				capability: &plugin_pb.JobTypeCapability{JobType: "erasure_coding"},
				descriptor: &plugin_pb.JobTypeDescriptor{JobType: "erasure_coding"},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewWorker error = %v", err)
	}

	var response *plugin_pb.WorkerToAdminMessage
	ok := worker.handleAdminMessageForTest(&plugin_pb.AdminToWorkerMessage{
		RequestId: "schema-req-1",
		Body: &plugin_pb.AdminToWorkerMessage_RequestConfigSchema{
			RequestConfigSchema: &plugin_pb.RequestConfigSchema{},
		},
	}, func(msg *plugin_pb.WorkerToAdminMessage) bool {
		response = msg
		return true
	})
	if !ok {
		t.Fatalf("expected send callback to be invoked")
	}
	schema := response.GetConfigSchemaResponse()
	if schema == nil || schema.Success {
		t.Fatalf("expected schema error response, got=%+v", response)
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

func (w *Worker) handleAdminMessageForTest(
	message *plugin_pb.AdminToWorkerMessage,
	send func(*plugin_pb.WorkerToAdminMessage) bool,
) bool {
	called := false
	w.handleAdminMessage(context.Background(), message, func(msg *plugin_pb.WorkerToAdminMessage) bool {
		called = true
		return send(msg)
	})
	return called
}
