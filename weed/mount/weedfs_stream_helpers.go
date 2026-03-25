package mount

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// streamCreateEntry routes a CreateEntryRequest through the streaming mux
// if available, falling back to a unary gRPC call.
func (wfs *WFS) streamCreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if wfs.streamMutate != nil && wfs.streamMutate.IsAvailable() {
		return wfs.streamMutate.CreateEntry(ctx, req)
	}
	var resp *filer_pb.CreateEntryResponse
	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var err error
		resp, err = filer_pb.CreateEntryWithResponse(ctx, client, req)
		return err
	})
	return resp, err
}

// streamUpdateEntry routes an UpdateEntryRequest through the streaming mux
// if available, falling back to a unary gRPC call.
func (wfs *WFS) streamUpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	if wfs.streamMutate != nil && wfs.streamMutate.IsAvailable() {
		return wfs.streamMutate.UpdateEntry(ctx, req)
	}
	var resp *filer_pb.UpdateEntryResponse
	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var err error
		resp, err = client.UpdateEntry(ctx, req)
		return err
	})
	return resp, err
}

// streamDeleteEntry routes a DeleteEntryRequest through the streaming mux
// if available, falling back to a unary gRPC call.
func (wfs *WFS) streamDeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	if wfs.streamMutate != nil && wfs.streamMutate.IsAvailable() {
		return wfs.streamMutate.DeleteEntry(ctx, req)
	}
	var resp *filer_pb.DeleteEntryResponse
	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var err error
		resp, err = client.DeleteEntry(ctx, req)
		return err
	})
	return resp, err
}
