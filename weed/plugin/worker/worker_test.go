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

func TestWorkerHandleDetectionQueuesWhenAtCapacity(t *testing.T) {
	handler := &detectionQueueTestHandler{
		capability: &plugin_pb.JobTypeCapability{
			JobType:    "vacuum",
			CanDetect:  true,
			CanExecute: false,
		},
		descriptor:     &plugin_pb.JobTypeDescriptor{JobType: "vacuum"},
		detectEntered:  make(chan struct{}, 2),
		detectContinue: make(chan struct{}, 2),
	}

	worker, err := NewWorker(WorkerOptions{
		AdminServer:             "localhost:23646",
		GrpcDialOption:          grpc.WithTransportCredentials(insecure.NewCredentials()),
		Handler:                 handler,
		MaxDetectionConcurrency: 1,
	})
	if err != nil {
		t.Fatalf("NewWorker error = %v", err)
	}

	msgCh := make(chan *plugin_pb.WorkerToAdminMessage, 8)
	send := func(msg *plugin_pb.WorkerToAdminMessage) bool {
		msgCh <- msg
		return true
	}

	sendDetection := func(requestID string) {
		worker.handleAdminMessage(context.Background(), &plugin_pb.AdminToWorkerMessage{
			RequestId: requestID,
			Body: &plugin_pb.AdminToWorkerMessage_RunDetectionRequest{
				RunDetectionRequest: &plugin_pb.RunDetectionRequest{
					JobType: "vacuum",
				},
			},
		}, send)
	}

	sendDetection("detect-1")
	expectDetectionAck(t, recvWorkerMessage(t, msgCh), "detect-1")
	<-handler.detectEntered

	sendDetection("detect-2")
	expectDetectionAck(t, recvWorkerMessage(t, msgCh), "detect-2")

	select {
	case unexpected := <-msgCh:
		t.Fatalf("did not expect detection completion before slot is available, got=%+v", unexpected)
	case <-time.After(100 * time.Millisecond):
	}

	handler.detectContinue <- struct{}{}
	expectDetectionCompleteSuccess(t, recvWorkerMessage(t, msgCh), "detect-1")

	<-handler.detectEntered
	handler.detectContinue <- struct{}{}
	expectDetectionCompleteSuccess(t, recvWorkerMessage(t, msgCh), "detect-2")
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

type detectionQueueTestHandler struct {
	capability *plugin_pb.JobTypeCapability
	descriptor *plugin_pb.JobTypeDescriptor

	detectEntered  chan struct{}
	detectContinue chan struct{}
}

func (h *detectionQueueTestHandler) Capability() *plugin_pb.JobTypeCapability {
	return h.capability
}

func (h *detectionQueueTestHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return h.descriptor
}

func (h *detectionQueueTestHandler) Detect(ctx context.Context, _ *plugin_pb.RunDetectionRequest, sender DetectionSender) error {
	select {
	case h.detectEntered <- struct{}{}:
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.detectContinue:
	}

	return sender.SendComplete(&plugin_pb.DetectionComplete{
		Success: true,
	})
}

func (h *detectionQueueTestHandler) Execute(context.Context, *plugin_pb.ExecuteJobRequest, ExecutionSender) error {
	return nil
}

func recvWorkerMessage(t *testing.T, msgCh <-chan *plugin_pb.WorkerToAdminMessage) *plugin_pb.WorkerToAdminMessage {
	t.Helper()
	select {
	case msg := <-msgCh:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker message")
		return nil
	}
}

func expectDetectionAck(t *testing.T, message *plugin_pb.WorkerToAdminMessage, requestID string) {
	t.Helper()
	ack := message.GetAcknowledge()
	if ack == nil {
		t.Fatalf("expected acknowledge for request %q, got=%+v", requestID, message)
	}
	if ack.RequestId != requestID {
		t.Fatalf("expected acknowledge request_id=%q, got=%q", requestID, ack.RequestId)
	}
	if !ack.Accepted {
		t.Fatalf("expected acknowledge accepted for request %q, got=%+v", requestID, ack)
	}
}

func expectDetectionCompleteSuccess(t *testing.T, message *plugin_pb.WorkerToAdminMessage, requestID string) {
	t.Helper()
	complete := message.GetDetectionComplete()
	if complete == nil {
		t.Fatalf("expected detection complete for request %q, got=%+v", requestID, message)
	}
	if complete.RequestId != requestID {
		t.Fatalf("expected detection complete request_id=%q, got=%q", requestID, complete.RequestId)
	}
	if !complete.Success {
		t.Fatalf("expected successful detection complete for request %q, got=%+v", requestID, complete)
	}
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
