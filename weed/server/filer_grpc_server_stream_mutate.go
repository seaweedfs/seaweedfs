package weed_server

import (
	"context"
	"io"
	"strings"
	"syscall"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func (fs *FilerServer) StreamMutateEntry(stream grpc.BidiStreamingServer[filer_pb.StreamMutateEntryRequest, filer_pb.StreamMutateEntryResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch r := req.Request.(type) {

		case *filer_pb.StreamMutateEntryRequest_CreateRequest:
			resp, createErr := fs.CreateEntry(stream.Context(), r.CreateRequest)
			if createErr != nil {
				resp = &filer_pb.CreateEntryResponse{Error: createErr.Error()}
			}
			streamResp := &filer_pb.StreamMutateEntryResponse{
				RequestId: req.RequestId,
				IsLast:    true,
				Response:  &filer_pb.StreamMutateEntryResponse_CreateResponse{CreateResponse: resp},
			}
			if resp.Error != "" {
				streamResp.Error = resp.Error
				streamResp.Errno = int32(syscall.EIO)
			}
			if sendErr := stream.Send(streamResp); sendErr != nil {
				return sendErr
			}

		case *filer_pb.StreamMutateEntryRequest_UpdateRequest:
			resp, updateErr := fs.UpdateEntry(stream.Context(), r.UpdateRequest)
			if updateErr != nil {
				resp = &filer_pb.UpdateEntryResponse{}
			}
			streamResp := &filer_pb.StreamMutateEntryResponse{
				RequestId: req.RequestId,
				IsLast:    true,
				Response:  &filer_pb.StreamMutateEntryResponse_UpdateResponse{UpdateResponse: resp},
			}
			if updateErr != nil {
				streamResp.Error = updateErr.Error()
				streamResp.Errno = int32(syscall.EIO)
			}
			if sendErr := stream.Send(streamResp); sendErr != nil {
				return sendErr
			}

		case *filer_pb.StreamMutateEntryRequest_DeleteRequest:
			resp, deleteErr := fs.DeleteEntry(stream.Context(), r.DeleteRequest)
			if deleteErr != nil {
				resp = &filer_pb.DeleteEntryResponse{Error: deleteErr.Error()}
			}
			streamResp := &filer_pb.StreamMutateEntryResponse{
				RequestId: req.RequestId,
				IsLast:    true,
				Response:  &filer_pb.StreamMutateEntryResponse_DeleteResponse{DeleteResponse: resp},
			}
			if resp.Error != "" {
				streamResp.Error = resp.Error
				streamResp.Errno = int32(syscall.EIO)
			}
			if sendErr := stream.Send(streamResp); sendErr != nil {
				return sendErr
			}

		case *filer_pb.StreamMutateEntryRequest_RenameRequest:
			if err := fs.handleStreamMutateRename(stream, req.RequestId, r.RenameRequest); err != nil {
				return err
			}

		default:
			glog.Warningf("StreamMutateEntry: unknown request type %T", req.Request)
		}
	}
}

// handleStreamMutateRename delegates to the existing StreamRenameEntry logic
// using a proxy stream that converts StreamRenameEntryResponse events into
// StreamMutateEntryResponse messages on the parent bidi stream.
func (fs *FilerServer) handleStreamMutateRename(
	parent grpc.BidiStreamingServer[filer_pb.StreamMutateEntryRequest, filer_pb.StreamMutateEntryResponse],
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
	if sendErr := parent.Send(finalResp); sendErr != nil {
		return sendErr
	}
	return nil
}

// renameStreamProxy adapts the bidi StreamMutateEntry stream to look like a
// SeaweedFiler_StreamRenameEntryServer, which is what StreamRenameEntry and
// moveEntry expect. Each Send() call forwards the response as a non-final
// StreamMutateEntryResponse.
type renameStreamProxy struct {
	parent    grpc.BidiStreamingServer[filer_pb.StreamMutateEntryRequest, filer_pb.StreamMutateEntryResponse]
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
	return p.parent.Context()
}

func (p *renameStreamProxy) SendMsg(m any) error             { return p.parent.SendMsg(m) }
func (p *renameStreamProxy) RecvMsg(m any) error             { return p.parent.RecvMsg(m) }
func (p *renameStreamProxy) SetHeader(md metadata.MD) error  { return p.parent.SetHeader(md) }
func (p *renameStreamProxy) SendHeader(md metadata.MD) error { return p.parent.SendHeader(md) }
func (p *renameStreamProxy) SetTrailer(md metadata.MD)       { p.parent.SetTrailer(md) }

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
