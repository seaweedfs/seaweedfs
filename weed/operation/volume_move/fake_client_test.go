package volume_move

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeCluster fakes the volume servers a move touches. It records every RPC as
// "<server> <method>" so tests can assert the exact sequence, and lets tests
// inject per-RPC errors and per-server state.
type fakeCluster struct {
	mu    sync.Mutex
	calls []string

	// errs fails an RPC, keyed "<server> <method>".
	errs map[string]error
	// readonly is each server's VolumeStatus.IsReadOnly answer.
	readonly map[string]bool
	// status is each server's ReadVolumeFileStatus answer.
	status map[string]*volume_server_pb.ReadVolumeFileStatusResponse
	// statusFailures fails a server's next N ReadVolumeFileStatus calls, e.g.
	// to model a target that has no volume before the copy but one after. The
	// failures use statusFailureErr, defaulting to a server-side (gRPC code
	// Unknown) volume-not-found error.
	statusFailures   map[string]int
	statusFailureErr error
	// statusSeq overrides a server's next ReadVolumeFileStatus answers in
	// order, e.g. to model a source that changes between two reads.
	statusSeq map[string][]*volume_server_pb.ReadVolumeFileStatusResponse
	// ecShards is each server's VolumeEcShardsInfo answer.
	ecShards map[string][]*volume_server_pb.EcShardInfo

	lastAppendAtNs uint64

	copyReqs   []*volume_server_pb.VolumeCopyRequest
	deleteReqs []*volume_server_pb.VolumeDeleteRequest
	ecCopyReqs []*volume_server_pb.VolumeEcShardsCopyRequest
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{
		errs:           make(map[string]error),
		readonly:       make(map[string]bool),
		status:         make(map[string]*volume_server_pb.ReadVolumeFileStatusResponse),
		statusFailures: make(map[string]int),
		statusSeq:      make(map[string][]*volume_server_pb.ReadVolumeFileStatusResponse),
		ecShards:       make(map[string][]*volume_server_pb.EcShardInfo),
	}
}

func (c *fakeCluster) mover() *Mover {
	return NewMoverWithClientFunc(func(streamingMode bool, addr pb.ServerAddress, fn func(client volume_server_pb.VolumeServerClient) error) error {
		return fn(&fakeClient{cluster: c, addr: string(addr)})
	})
}

func (c *fakeCluster) record(addr, method string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, addr+" "+method)
	return c.errs[addr+" "+method]
}

func (c *fakeCluster) callList() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

// fakeClient answers as the volume server at addr. Unused VolumeServerClient
// methods panic via the embedded nil interface.
type fakeClient struct {
	volume_server_pb.VolumeServerClient
	cluster *fakeCluster
	addr    string
}

func (f *fakeClient) VolumeStatus(ctx context.Context, req *volume_server_pb.VolumeStatusRequest, opts ...grpc.CallOption) (*volume_server_pb.VolumeStatusResponse, error) {
	if err := f.cluster.record(f.addr, "VolumeStatus"); err != nil {
		return nil, err
	}
	return &volume_server_pb.VolumeStatusResponse{IsReadOnly: f.cluster.readonly[f.addr]}, nil
}

func (f *fakeClient) VolumeMarkReadonly(ctx context.Context, req *volume_server_pb.VolumeMarkReadonlyRequest, opts ...grpc.CallOption) (*volume_server_pb.VolumeMarkReadonlyResponse, error) {
	if err := f.cluster.record(f.addr, "VolumeMarkReadonly"); err != nil {
		return nil, err
	}
	return &volume_server_pb.VolumeMarkReadonlyResponse{}, nil
}

func (f *fakeClient) VolumeMarkWritable(ctx context.Context, req *volume_server_pb.VolumeMarkWritableRequest, opts ...grpc.CallOption) (*volume_server_pb.VolumeMarkWritableResponse, error) {
	if err := f.cluster.record(f.addr, "VolumeMarkWritable"); err != nil {
		return nil, err
	}
	return &volume_server_pb.VolumeMarkWritableResponse{}, nil
}

func (f *fakeClient) ReadVolumeFileStatus(ctx context.Context, req *volume_server_pb.ReadVolumeFileStatusRequest, opts ...grpc.CallOption) (*volume_server_pb.ReadVolumeFileStatusResponse, error) {
	if err := f.cluster.record(f.addr, "ReadVolumeFileStatus"); err != nil {
		return nil, err
	}
	f.cluster.mu.Lock()
	failures := f.cluster.statusFailures[f.addr]
	if failures > 0 {
		f.cluster.statusFailures[f.addr] = failures - 1
	}
	failureErr := f.cluster.statusFailureErr
	var seqResp *volume_server_pb.ReadVolumeFileStatusResponse
	if failures == 0 {
		if seq := f.cluster.statusSeq[f.addr]; len(seq) > 0 {
			seqResp = seq[0]
			f.cluster.statusSeq[f.addr] = seq[1:]
		}
	}
	resp := f.cluster.status[f.addr]
	f.cluster.mu.Unlock()
	if failures > 0 {
		if failureErr != nil {
			return nil, failureErr
		}
		return nil, status.Error(codes.Unknown, fmt.Sprintf("not found volume id %d", req.VolumeId))
	}
	if seqResp != nil {
		return seqResp, nil
	}
	if resp == nil {
		return nil, status.Error(codes.Unknown, fmt.Sprintf("not found volume id %d", req.VolumeId))
	}
	return resp, nil
}

func (f *fakeClient) VolumeCopy(ctx context.Context, req *volume_server_pb.VolumeCopyRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[volume_server_pb.VolumeCopyResponse], error) {
	if err := f.cluster.record(f.addr, "VolumeCopy"); err != nil {
		return nil, err
	}
	f.cluster.mu.Lock()
	f.cluster.copyReqs = append(f.cluster.copyReqs, req)
	lastAppendAtNs := f.cluster.lastAppendAtNs
	f.cluster.mu.Unlock()
	return &fakeCopyStream{resps: []*volume_server_pb.VolumeCopyResponse{
		{ProcessedBytes: 1024},
		{LastAppendAtNs: lastAppendAtNs},
	}}, nil
}

func (f *fakeClient) VolumeTailReceiver(ctx context.Context, req *volume_server_pb.VolumeTailReceiverRequest, opts ...grpc.CallOption) (*volume_server_pb.VolumeTailReceiverResponse, error) {
	if err := f.cluster.record(f.addr, "VolumeTailReceiver"); err != nil {
		return nil, err
	}
	return &volume_server_pb.VolumeTailReceiverResponse{}, nil
}

func (f *fakeClient) VolumeDelete(ctx context.Context, req *volume_server_pb.VolumeDeleteRequest, opts ...grpc.CallOption) (*volume_server_pb.VolumeDeleteResponse, error) {
	if err := f.cluster.record(f.addr, "VolumeDelete"); err != nil {
		return nil, err
	}
	f.cluster.mu.Lock()
	f.cluster.deleteReqs = append(f.cluster.deleteReqs, req)
	f.cluster.mu.Unlock()
	return &volume_server_pb.VolumeDeleteResponse{}, nil
}

func (f *fakeClient) VolumeConfigure(ctx context.Context, req *volume_server_pb.VolumeConfigureRequest, opts ...grpc.CallOption) (*volume_server_pb.VolumeConfigureResponse, error) {
	if err := f.cluster.record(f.addr, "VolumeConfigure"); err != nil {
		return nil, err
	}
	return &volume_server_pb.VolumeConfigureResponse{}, nil
}

func (f *fakeClient) VolumeEcShardsCopy(ctx context.Context, req *volume_server_pb.VolumeEcShardsCopyRequest, opts ...grpc.CallOption) (*volume_server_pb.VolumeEcShardsCopyResponse, error) {
	if err := f.cluster.record(f.addr, "VolumeEcShardsCopy"); err != nil {
		return nil, err
	}
	f.cluster.mu.Lock()
	f.cluster.ecCopyReqs = append(f.cluster.ecCopyReqs, req)
	f.cluster.mu.Unlock()
	return &volume_server_pb.VolumeEcShardsCopyResponse{}, nil
}

func (f *fakeClient) VolumeEcShardsMount(ctx context.Context, req *volume_server_pb.VolumeEcShardsMountRequest, opts ...grpc.CallOption) (*volume_server_pb.VolumeEcShardsMountResponse, error) {
	if err := f.cluster.record(f.addr, "VolumeEcShardsMount"); err != nil {
		return nil, err
	}
	return &volume_server_pb.VolumeEcShardsMountResponse{}, nil
}

func (f *fakeClient) VolumeEcShardsUnmount(ctx context.Context, req *volume_server_pb.VolumeEcShardsUnmountRequest, opts ...grpc.CallOption) (*volume_server_pb.VolumeEcShardsUnmountResponse, error) {
	if err := f.cluster.record(f.addr, "VolumeEcShardsUnmount"); err != nil {
		return nil, err
	}
	return &volume_server_pb.VolumeEcShardsUnmountResponse{}, nil
}

func (f *fakeClient) VolumeEcShardsDelete(ctx context.Context, req *volume_server_pb.VolumeEcShardsDeleteRequest, opts ...grpc.CallOption) (*volume_server_pb.VolumeEcShardsDeleteResponse, error) {
	if err := f.cluster.record(f.addr, "VolumeEcShardsDelete"); err != nil {
		return nil, err
	}
	return &volume_server_pb.VolumeEcShardsDeleteResponse{}, nil
}

func (f *fakeClient) VolumeEcShardsInfo(ctx context.Context, req *volume_server_pb.VolumeEcShardsInfoRequest, opts ...grpc.CallOption) (*volume_server_pb.VolumeEcShardsInfoResponse, error) {
	if err := f.cluster.record(f.addr, "VolumeEcShardsInfo"); err != nil {
		return nil, err
	}
	return &volume_server_pb.VolumeEcShardsInfoResponse{EcShardInfos: f.cluster.ecShards[f.addr]}, nil
}

// fakeCopyStream feeds the canned VolumeCopy responses, then io.EOF.
type fakeCopyStream struct {
	grpc.ClientStream
	resps []*volume_server_pb.VolumeCopyResponse
}

func (s *fakeCopyStream) Recv() (*volume_server_pb.VolumeCopyResponse, error) {
	if len(s.resps) == 0 {
		return nil, io.EOF
	}
	resp := s.resps[0]
	s.resps = s.resps[1:]
	return resp, nil
}
