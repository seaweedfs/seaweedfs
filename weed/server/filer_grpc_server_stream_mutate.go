package weed_server

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"syscall"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// streamMutateConcurrency bounds the number of in-flight mutations processed
// concurrently per client stream. Set to the typical filer-store sweet spot
// so one noisy mount cannot exhaust filer resources.
const streamMutateConcurrency = 64

// streamMutatePendingLimit caps total outstanding requests per stream
// (admitted + waiting-for-admission). Prevents goroutine explosion when a
// client floods requests against a conflicted path while leaving enough
// headroom that cross-path requests never block Recv in practice.
const streamMutatePendingLimit = 1024

// syncStream wraps a bidi stream so that concurrent goroutines can Send
// without interleaving frames. gRPC requires that Send not be called
// concurrently; this mutex is the serialization point.
type syncStream struct {
	stream grpc.BidiStreamingServer[filer_pb.StreamMutateEntryRequest, filer_pb.StreamMutateEntryResponse]
	mu     sync.Mutex
}

func (s *syncStream) Send(r *filer_pb.StreamMutateEntryResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream.Send(r)
}

func (fs *FilerServer) StreamMutateEntry(stream grpc.BidiStreamingServer[filer_pb.StreamMutateEntryRequest, filer_pb.StreamMutateEntryResponse]) error {
	ss := &syncStream{stream: stream}
	// Path-keyed admission + subtree barriers, adapted from filer.sync's
	// MetadataProcessor (weed/command/filer_sync_jobs.go). Admit blocks when
	// a new request conflicts with an in-flight one on the same path or with
	// a barrier directory at the same path / an ancestor.
	sched := newMutateScheduler(streamMutateConcurrency)
	// pendingSem caps goroutines-per-stream. The receive loop only blocks on
	// this sem (not on Admit), so one conflicted path cannot head-of-line
	// block receipt of unrelated paths — later distinct-path requests can be
	// spawned, admitted, and processed while the conflicted request waits.
	pendingSem := make(chan struct{}, streamMutatePendingLimit)
	var wg sync.WaitGroup
	// Track the first fatal send error across worker goroutines.
	var sendErrMu sync.Mutex
	var sendErr error
	setSendErr := func(e error) {
		sendErrMu.Lock()
		if sendErr == nil {
			sendErr = e
		}
		sendErrMu.Unlock()
	}
	getSendErr := func() error {
		sendErrMu.Lock()
		defer sendErrMu.Unlock()
		return sendErr
	}

	for {
		if e := getSendErr(); e != nil {
			wg.Wait()
			return e
		}
		req, err := stream.Recv()
		if err == io.EOF {
			wg.Wait()
			return getSendErr()
		}
		if err != nil {
			wg.Wait()
			return err
		}

		primary, secondary, kind := classifyMutation(req)
		pendingSem <- struct{}{}
		wg.Add(1)
		go func(req *filer_pb.StreamMutateEntryRequest, p, s util.FullPath, k mutateJobKind) {
			defer wg.Done()
			defer func() { <-pendingSem }()
			// Admission happens off the Recv loop so a conflicted path never
			// blocks receipt of unrelated requests.
			sched.Admit(p, s, k)
			defer sched.Done(p, s, k)
			if e := fs.handleStreamMutateRequest(ss, req); e != nil {
				setSendErr(e)
			}
		}(req, primary, secondary, kind)
	}
}

// handleStreamMutateRequest processes one request and sends exactly one
// (possibly multi-message) response via the shared sync stream. It returns
// a non-nil error only if Send fails — in which case the caller should
// tear down the stream.
func (fs *FilerServer) handleStreamMutateRequest(ss *syncStream, req *filer_pb.StreamMutateEntryRequest) error {
	switch r := req.Request.(type) {

	case *filer_pb.StreamMutateEntryRequest_CreateRequest:
		resp, createErr := fs.CreateEntry(ss.stream.Context(), r.CreateRequest)
		if createErr != nil {
			resp = &filer_pb.CreateEntryResponse{Error: createErr.Error()}
		}
		out := &filer_pb.StreamMutateEntryResponse{
			RequestId: req.RequestId,
			IsLast:    true,
			Response:  &filer_pb.StreamMutateEntryResponse_CreateResponse{CreateResponse: resp},
		}
		if resp.Error != "" {
			out.Error = resp.Error
			out.Errno = int32(syscall.EIO)
		}
		return ss.Send(out)

	case *filer_pb.StreamMutateEntryRequest_UpdateRequest:
		resp, updateErr := fs.UpdateEntry(ss.stream.Context(), r.UpdateRequest)
		if updateErr != nil {
			resp = &filer_pb.UpdateEntryResponse{}
		}
		out := &filer_pb.StreamMutateEntryResponse{
			RequestId: req.RequestId,
			IsLast:    true,
			Response:  &filer_pb.StreamMutateEntryResponse_UpdateResponse{UpdateResponse: resp},
		}
		if updateErr != nil {
			out.Error = updateErr.Error()
			out.Errno = int32(syscall.EIO)
		}
		return ss.Send(out)

	case *filer_pb.StreamMutateEntryRequest_DeleteRequest:
		resp, deleteErr := fs.DeleteEntry(ss.stream.Context(), r.DeleteRequest)
		if deleteErr != nil {
			resp = &filer_pb.DeleteEntryResponse{Error: deleteErr.Error()}
		}
		out := &filer_pb.StreamMutateEntryResponse{
			RequestId: req.RequestId,
			IsLast:    true,
			Response:  &filer_pb.StreamMutateEntryResponse_DeleteResponse{DeleteResponse: resp},
		}
		if resp.Error != "" {
			out.Error = resp.Error
			out.Errno = int32(syscall.EIO)
		}
		return ss.Send(out)

	case *filer_pb.StreamMutateEntryRequest_RenameRequest:
		return fs.handleStreamMutateRename(ss, req.RequestId, r.RenameRequest)

	default:
		// Send a terminal error response so the client's per-RequestId waiter
		// is released; returning nil here would leak the client-side waiter
		// forever when a future oneof variant or a malformed request arrives.
		glog.Warningf("StreamMutateEntry: unknown request type %T", req.Request)
		return ss.Send(&filer_pb.StreamMutateEntryResponse{
			RequestId: req.RequestId,
			IsLast:    true,
			Error:     "unknown request type",
			Errno:     int32(syscall.EINVAL),
		})
	}
}

// handleStreamMutateRename delegates to the existing StreamRenameEntry logic
// using a proxy stream that converts StreamRenameEntryResponse events into
// StreamMutateEntryResponse messages on the parent bidi stream.
func (fs *FilerServer) handleStreamMutateRename(
	parent *syncStream,
	requestId uint64,
	req *filer_pb.StreamRenameEntryRequest,
) error {
	proxy := &renameStreamProxy{parent: parent, requestId: requestId}
	renameErr := fs.StreamRenameEntry(req, proxy)
	// Always send a final is_last=true to signal rename completion.
	finalResp := &filer_pb.StreamMutateEntryResponse{
		RequestId: requestId,
		IsLast:    true,
		Response: &filer_pb.StreamMutateEntryResponse_RenameResponse{
			RenameResponse: &filer_pb.StreamRenameEntryResponse{},
		},
	}
	if renameErr != nil {
		finalResp.Error = renameErr.Error()
		finalResp.Errno = renameErrno(renameErr)
		glog.V(0).Infof("StreamMutateEntry rename: %v", renameErr)
	}
	return parent.Send(finalResp)
}

// renameStreamProxy adapts the bidi StreamMutateEntry stream to look like a
// SeaweedFiler_StreamRenameEntryServer, which is what StreamRenameEntry and
// moveEntry expect. Each Send() call forwards the response as a non-final
// StreamMutateEntryResponse.
type renameStreamProxy struct {
	parent    *syncStream
	requestId uint64
}

func (p *renameStreamProxy) Send(resp *filer_pb.StreamRenameEntryResponse) error {
	return p.parent.Send(&filer_pb.StreamMutateEntryResponse{
		RequestId: p.requestId,
		IsLast:    false,
		Response:  &filer_pb.StreamMutateEntryResponse_RenameResponse{RenameResponse: resp},
	})
}

func (p *renameStreamProxy) Context() context.Context {
	return p.parent.stream.Context()
}

// SendMsg routes through Send so the payload is wrapped into a
// StreamMutateEntryResponse and goes through the syncStream mutex. Calling
// SendMsg with anything other than *filer_pb.StreamRenameEntryResponse would
// emit the wrong protobuf type on this RPC, so reject other shapes.
func (p *renameStreamProxy) SendMsg(m any) error {
	resp, ok := m.(*filer_pb.StreamRenameEntryResponse)
	if !ok {
		return fmt.Errorf("renameStreamProxy.SendMsg: unexpected type %T", m)
	}
	return p.Send(resp)
}

// RecvMsg on the proxy would race with the outer StreamMutateEntry recv
// loop and could steal unrelated mutation requests. The rename logic never
// calls RecvMsg (it is strictly a server-push stream), so fail loudly if it
// ever does.
func (p *renameStreamProxy) RecvMsg(m any) error {
	return fmt.Errorf("renameStreamProxy.RecvMsg is not supported")
}

func (p *renameStreamProxy) SetHeader(md metadata.MD) error  { return p.parent.stream.SetHeader(md) }
func (p *renameStreamProxy) SendHeader(md metadata.MD) error { return p.parent.stream.SendHeader(md) }
func (p *renameStreamProxy) SetTrailer(md metadata.MD)       { p.parent.stream.SetTrailer(md) }

// renameErrno maps a rename error to a POSIX errno for the client.
func renameErrno(err error) int32 {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"):
		return int32(syscall.ENOENT)
	case strings.Contains(msg, "not empty"):
		return int32(syscall.ENOTEMPTY)
	case strings.Contains(msg, "not directory"):
		return int32(syscall.ENOTDIR)
	default:
		return int32(syscall.EIO)
	}
}
