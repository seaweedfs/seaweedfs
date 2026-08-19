package plugin

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// maxPreviewRows caps what admin will ask for. A preview is a look at the
// object, not an export, and the rows cross the control stream.
const maxPreviewRows = 200

// RequestObjectPreview asks the worker that last described an object for sample
// rows of it, and waits for the answer.
//
// The worker is chosen from the observation store rather than by job type: the
// one that last looked at this object is the one that can read it, and it says
// so by having reported. That also means a preview is only available once
// detection has run, which the caller should surface rather than hide.
func (r *Plugin) RequestObjectPreview(ctx context.Context, objectID []string, format string, rowLimit int) (*plugin_pb.ObjectPreviewResponse, error) {
	if len(objectID) == 0 {
		return nil, fmt.Errorf("preview needs an object id")
	}
	if rowLimit < 1 || rowLimit > maxPreviewRows {
		rowLimit = maxPreviewRows
	}

	observed, ok := r.observations.Get(objectID)
	if !ok {
		return nil, fmt.Errorf("no worker has described this object yet")
	}
	if _, connected := r.registry.Get(observed.WorkerID); !connected {
		return nil, fmt.Errorf("worker %s is not connected", observed.WorkerID)
	}

	requestID, err := newRequestID("preview")
	if err != nil {
		return nil, err
	}

	responseCh := make(chan *plugin_pb.ObjectPreviewResponse, 1)
	r.pendingPreviewMu.Lock()
	r.pendingPreview[requestID] = responseCh
	r.pendingPreviewMu.Unlock()
	defer func() {
		r.pendingPreviewMu.Lock()
		delete(r.pendingPreview, requestID)
		r.pendingPreviewMu.Unlock()
	}()

	request := &plugin_pb.AdminToWorkerMessage{
		RequestId: requestID,
		SentAt:    timestamppb.Now(),
		Body: &plugin_pb.AdminToWorkerMessage_RequestObjectPreview{
			RequestObjectPreview: &plugin_pb.RequestObjectPreview{
				ObjectId: objectID,
				Format:   format,
				RowLimit: int32(rowLimit),
			},
		},
	}
	if err := r.sendToWorker(observed.WorkerID, request); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case response, ok := <-responseCh:
		if !ok {
			return nil, fmt.Errorf("preview request %s interrupted", requestID)
		}
		if response == nil {
			return nil, fmt.Errorf("preview request %s returned nothing", requestID)
		}
		if !response.Success {
			return nil, fmt.Errorf("worker %s could not preview the object: %s", observed.WorkerID, response.ErrorMessage)
		}
		return response, nil
	}
}

// handleObjectPreviewResponse routes one reply back to whoever is waiting.
func (r *Plugin) handleObjectPreviewResponse(response *plugin_pb.ObjectPreviewResponse) {
	if response == nil {
		return
	}
	r.pendingPreviewMu.Lock()
	ch := r.pendingPreview[response.RequestId]
	r.pendingPreviewMu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- response:
	default:
	}
}
