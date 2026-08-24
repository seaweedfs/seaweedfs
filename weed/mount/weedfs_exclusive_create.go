package mount

import (
	"context"
	"hash/fnv"
	"sort"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ownerFilerAddress deterministically maps a path to one filer so that every
// mount sends the same path's exclusive create to the same filer. The filer's
// per-path exclusive lock (filer_grpc_server CreateEntry) can then arbitrate
// concurrent creators cluster-wide. Without this, each mount streams to its
// own randomly chosen filer, and two filers accept the same create
// concurrently: the existence check is a read-then-insert against the shared
// store, whose insert has upsert semantics.
//
// The address list is sorted so mounts started with the filers listed in a
// different order still agree on the owner.
func (wfs *WFS) ownerFilerAddress(fullpath util.FullPath) pb.ServerAddress {
	addresses := wfs.option.FilerAddresses
	if len(addresses) == 1 {
		return addresses[0]
	}
	sorted := make([]pb.ServerAddress, len(addresses))
	copy(sorted, addresses)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	h := fnv.New32a()
	h.Write([]byte(fullpath))
	return sorted[h.Sum32()%uint32(len(sorted))]
}

// exclusiveCreateEntry sends an OExcl CreateEntryRequest to the path's owner
// filer, falling back to the ordered mutation stream when the owner already
// is this mount's stream target (same arbitration, better transport) or is
// unreachable (degrades to filer-local exclusivity rather than failing the
// create outright).
func (wfs *WFS) exclusiveCreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest, fullpath util.FullPath) (*filer_pb.CreateEntryResponse, error) {
	owner := wfs.ownerFilerAddress(fullpath)
	if owner == wfs.getCurrentFiler() {
		return wfs.streamCreateEntry(ctx, req)
	}

	var resp *filer_pb.CreateEntryResponse
	err := pb.WithGrpcClient(ctx, false, wfs.signature, func(grpcConnection *grpc.ClientConn) error {
		var err error
		resp, err = filer_pb.CreateEntryWithResponse(ctx, filer_pb.NewSeaweedFilerClient(grpcConnection), req)
		return err
	}, owner.ToGrpcAddress(), false, wfs.option.GrpcDialOption)

	if err == nil {
		return resp, nil
	}
	// Application-level failures (EEXIST and friends) come back as plain
	// errors reconstructed from the response; only RPC-level failures carry
	// a gRPC status. Those mean the owner is unreachable — retry on the
	// mount's own stream filer instead of failing the create.
	if s, ok := status.FromError(err); ok && s.Code() != codes.OK {
		glog.V(0).Infof("exclusive create %s/%s: owner filer %s unreachable (%v), falling back to mutation stream", req.Directory, req.Entry.Name, owner, err)
		return wfs.streamCreateEntry(ctx, req)
	}
	return resp, err
}
