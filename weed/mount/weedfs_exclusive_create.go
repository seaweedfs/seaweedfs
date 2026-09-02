package mount

import (
	"context"
	"errors"
	"hash/fnv"

	"google.golang.org/grpc"

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
// Rendezvous (highest-random-weight) hashing keeps the choice independent of
// the order the filers were listed in, and mounts configured with different
// but overlapping lists still agree wherever the winning filer appears in
// both; a membership change only moves the paths the changed filer owned.
func (wfs *WFS) ownerFilerAddress(fullpath util.FullPath) pb.ServerAddress {
	addresses := wfs.option.FilerAddresses
	owner := addresses[0]
	if len(addresses) == 1 {
		return owner
	}
	var bestScore uint32
	for i, addr := range addresses {
		h := fnv.New32a()
		h.Write([]byte(fullpath))
		h.Write([]byte{0})
		h.Write([]byte(addr))
		score := h.Sum32()
		if i == 0 || score > bestScore || (score == bestScore && addr < owner) {
			bestScore = score
			owner = addr
		}
	}
	return owner
}

// exclusiveCreateEntry sends an OExcl CreateEntryRequest to the path's owner
// filer and nowhere else: a retry on a different filer would race the owner's
// possibly still-in-flight create through a separate per-path lock — the very
// hole this routing closes. The ordered mutation stream is used when it
// already targets the owner; if the stream transport breaks mid-request, the
// retry goes to the same owner over unary, where OExcl answers EEXIST in case
// the first attempt had landed. An unreachable owner fails the create: mkdir
// is exclusive by contract, so unavailability beats a silent duplicate.
func (wfs *WFS) exclusiveCreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest, fullpath util.FullPath) (*filer_pb.CreateEntryResponse, error) {
	owner := wfs.ownerFilerAddress(fullpath)
	if owner == wfs.getCurrentFiler() && wfs.streamMutate != nil && wfs.streamMutate.IsAvailable() {
		resp, err := wfs.streamMutate.CreateEntry(ctx, req)
		if err == nil || !errors.Is(err, ErrStreamTransport) {
			return resp, err // success or application error
		}
		glog.V(1).Infof("exclusive create %s/%s: stream failed, retrying on owner %s: %v", req.Directory, req.Entry.Name, owner, err)
	}

	var resp *filer_pb.CreateEntryResponse
	err := pb.WithGrpcClient(ctx, false, wfs.signature, func(grpcConnection *grpc.ClientConn) error {
		var err error
		resp, err = filer_pb.CreateEntryWithResponse(ctx, filer_pb.NewSeaweedFilerClient(grpcConnection), req)
		return err
	}, owner.ToGrpcAddress(), false, wfs.option.GrpcDialOption)
	return resp, err
}
