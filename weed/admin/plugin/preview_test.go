package plugin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func previewPlugin(t *testing.T, workerID string) (*Plugin, *streamSession) {
	t.Helper()
	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New plugin error: %v", err)
	}
	t.Cleanup(pluginSvc.Shutdown)

	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{WorkerId: workerID})
	session := &streamSession{workerID: workerID, outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 4), done: make(chan struct{})}
	pluginSvc.putSession(session)
	return pluginSvc, session
}

func observeObject(p *Plugin, workerID string, objectID []string) {
	p.observations.Record(workerID, &plugin_pb.WorkerObservations{
		JobType: "lance_compact",
		Observations: []*plugin_pb.ObjectObservation{{
			ObjectId:   objectID,
			Format:     "LANCE",
			ObservedAt: timestamppb.Now(),
		}},
	})
}

// The round trip the details page makes: ask the worker that described this
// object, and render what it sends back.
func TestRequestObjectPreviewReturnsTheWorkersRows(t *testing.T) {
	t.Parallel()
	const workerID = "lance-worker-1"
	objectID := []string{"vectors", "ml", "embeddings"}
	pluginSvc, session := previewPlugin(t, workerID)
	observeObject(pluginSvc, workerID, objectID)

	type result struct {
		response *plugin_pb.ObjectPreviewResponse
		err      error
	}
	results := make(chan result, 1)
	go func() {
		response, err := pluginSvc.RequestObjectPreview(context.Background(), objectID, "LANCE", 2)
		results <- result{response, err}
	}()

	var request *plugin_pb.AdminToWorkerMessage
	select {
	case request = <-session.outgoing:
	case <-time.After(2 * time.Second):
		t.Fatal("no preview request reached the worker")
	}
	asked := request.GetRequestObjectPreview()
	if asked == nil {
		t.Fatalf("expected a preview request, got %T", request.Body)
	}
	if asked.RowLimit != 2 || asked.Format != "LANCE" {
		t.Fatalf("request lost its parameters: %+v", asked)
	}

	pluginSvc.handleWorkerMessage(workerID, &plugin_pb.WorkerToAdminMessage{
		WorkerId: workerID,
		Body: &plugin_pb.WorkerToAdminMessage_ObjectPreviewResponse{
			ObjectPreviewResponse: &plugin_pb.ObjectPreviewResponse{
				RequestId: request.RequestId,
				Success:   true,
				Columns:   []string{"id", "vec"},
				Rows:      []*plugin_pb.PreviewRow{{Values: []string{"1", "[0.1, 0.2]"}}},
				TotalRows: 1024,
			},
		},
	})

	select {
	case got := <-results:
		if got.err != nil {
			t.Fatalf("preview failed: %v", got.err)
		}
		if len(got.response.Rows) != 1 || got.response.Rows[0].Values[1] != "[0.1, 0.2]" {
			t.Fatalf("rows did not survive the round trip: %+v", got.response.Rows)
		}
		if got.response.TotalRows != 1024 {
			t.Fatalf("total_rows = %d, want 1024", got.response.TotalRows)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("preview never returned")
	}
}

// A worker that cannot read the object says so, and the page shows its reason
// rather than an empty table.
func TestRequestObjectPreviewSurfacesTheWorkersError(t *testing.T) {
	t.Parallel()
	const workerID = "lance-worker-1"
	objectID := []string{"vectors", "ml", "embeddings"}
	pluginSvc, session := previewPlugin(t, workerID)
	observeObject(pluginSvc, workerID, objectID)

	errs := make(chan error, 1)
	go func() {
		_, err := pluginSvc.RequestObjectPreview(context.Background(), objectID, "LANCE", 10)
		errs <- err
	}()

	request := <-session.outgoing
	pluginSvc.handleWorkerMessage(workerID, &plugin_pb.WorkerToAdminMessage{
		WorkerId: workerID,
		Body: &plugin_pb.WorkerToAdminMessage_ObjectPreviewResponse{
			ObjectPreviewResponse: &plugin_pb.ObjectPreviewResponse{
				RequestId:    request.RequestId,
				Success:      false,
				ErrorMessage: "open lance dataset: access denied",
			},
		},
	})

	select {
	case err := <-errs:
		if err == nil || !strings.Contains(err.Error(), "access denied") {
			t.Fatalf("error = %v, want the worker's reason", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("preview never returned")
	}
}

// Nothing has looked at the object, so there is nobody to ask. Worth its own
// message: it means detection has not run, not that the table is unreadable.
func TestRequestObjectPreviewNeedsAnObservation(t *testing.T) {
	t.Parallel()
	pluginSvc, _ := previewPlugin(t, "lance-worker-1")

	_, err := pluginSvc.RequestObjectPreview(context.Background(), []string{"vectors", "ml", "nothing"}, "LANCE", 10)
	if err == nil || !strings.Contains(err.Error(), "described") {
		t.Fatalf("error = %v, want one naming the missing description", err)
	}
}

// The worker that described the object has since gone away.
func TestRequestObjectPreviewNeedsAConnectedWorker(t *testing.T) {
	t.Parallel()
	objectID := []string{"vectors", "ml", "embeddings"}
	pluginSvc, _ := previewPlugin(t, "lance-worker-1")
	observeObject(pluginSvc, "lance-worker-gone", objectID)

	_, err := pluginSvc.RequestObjectPreview(context.Background(), objectID, "LANCE", 10)
	if err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Fatalf("error = %v, want one naming the absent worker", err)
	}
}
