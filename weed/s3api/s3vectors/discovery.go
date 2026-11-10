package s3vectors

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
	"time"
)

func NewVectorDiscovery(masters func() []pb.ServerAddress, dialOption grpc.DialOption, filerGroup string) *vectorDiscovery {
	return &vectorDiscovery{masters: masters, grpcDialOption: dialOption, discover: func(m []pb.ServerAddress, opt grpc.DialOption) []string {
		return discoverVectorWorkers(m, opt, filerGroup)
	}}
}

type vectorDiscovery struct {
	masters        func() []pb.ServerAddress
	grpcDialOption grpc.DialOption
	discover       func([]pb.ServerAddress, grpc.DialOption) []string
	mu             sync.Mutex
	closed         bool
	worker         string
	failed         string
	active         map[vector_pb.VectorServiceClient]int
	retired        map[vector_pb.VectorServiceClient]*grpc.ClientConn
	conn           *grpc.ClientConn
	client         vector_pb.VectorServiceClient
	nextResolve    time.Time
	resolving      chan struct{} // closed when the in-flight resolve lands
}

func (d *vectorDiscovery) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	d.retire()
	d.worker, d.conn, d.client = "", nil, nil
	return nil
}

func (d *vectorDiscovery) reset(c vector_pb.VectorServiceClient) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if c != d.client {
		return
	}
	d.failed = d.worker
	d.retire()
	d.worker, d.conn, d.client = "", nil, nil
	d.nextResolve = time.Time{}
}

func (d *vectorDiscovery) retire() {
	if d.conn == nil {
		return
	}
	if d.active[d.client] == 0 {
		_ = d.conn.Close()
		return
	}
	if d.retired == nil {
		d.retired = make(map[vector_pb.VectorServiceClient]*grpc.ClientConn)
	}
	d.retired[d.client] = d.conn
}

func (d *vectorDiscovery) release(c vector_pb.VectorServiceClient) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.active[c]--
	if d.active[c] == 0 {
		delete(d.active, c)
		if conn := d.retired[c]; conn != nil {
			_ = conn.Close()
			delete(d.retired, c)
		}
	}
}

func isTransportError(err error) bool {
	return status.Code(err) == codes.Unavailable
}

func (d *vectorDiscovery) resolve() {
	var masters []pb.ServerAddress
	if d.masters != nil {
		masters = d.masters()
	}
	workers := d.discover(masters, d.grpcDialOption)
	d.mu.Lock()
	defer d.mu.Unlock()
	// Measured from lookup end so a slow lookup cannot leave the next one already due.
	defer func() {
		interval := vectorRecheckInterval
		if d.client != nil {
			interval = vectorResolveInterval
		}
		d.nextResolve = time.Now().Add(interval)
		close(d.resolving)
		d.resolving = nil
	}()
	if d.closed || len(workers) == 0 {
		return
	}
	worker := workers[0]
	for _, w := range workers {
		if w != d.failed {
			worker = w
			break
		}
	}
	for _, w := range workers {
		if w == d.worker && w != d.failed {
			worker = w
			break
		}
	}
	if worker != d.worker {
		conn, err := pb.GrpcDial(context.Background(), worker, false, d.grpcDialOption)
		if err != nil {
			glog.Warningf("S3 Vectors: dial weed-vector %s: %v", worker, err)
			return
		}
		d.retire()
		d.worker, d.conn = worker, conn
		d.client = vector_pb.NewVectorServiceClient(conn)
	}
}

const (
	vectorResolveInterval = 5 * time.Second
	vectorRecheckInterval = 500 * time.Millisecond
)

var errNoVectorWorker = status.Error(codes.Unavailable, "no weed-vector registered on master")

func (d *vectorDiscovery) cli(ctx context.Context) (vector_pb.VectorServiceClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, status.Error(codes.Canceled, "vector discovery closed")
	}
	if d.resolving == nil && !time.Now().Before(d.nextResolve) {
		d.resolving = make(chan struct{})
		go d.resolve()
	}
	if d.client == nil && d.resolving != nil {
		resolving := d.resolving
		d.mu.Unlock()
		select {
		case <-resolving:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		d.mu.Lock()
	}
	defer d.mu.Unlock()
	if d.closed {
		return nil, status.Error(codes.Canceled, "vector discovery closed")
	}
	if d.client == nil {
		return nil, errNoVectorWorker
	}
	if d.active == nil {
		d.active = make(map[vector_pb.VectorServiceClient]int)
	}
	d.active[d.client]++
	return d.client, nil
}

func (d *vectorDiscovery) CreateIndex(ctx context.Context, in *vector_pb.CreateIndexRequest, opts ...grpc.CallOption) (resp *vector_pb.CreateIndexResponse, err error) {
	var firstErr error
	for attempt := 0; attempt < 2; attempt++ {
		var c vector_pb.VectorServiceClient
		var resolveErr error
		if c, resolveErr = d.cli(ctx); resolveErr != nil {
			if attempt > 0 {
				return nil, firstErr
			}
			return nil, resolveErr
		}
		resp, err = c.CreateIndex(ctx, in, opts...)
		if isTransportError(err) {
			d.reset(c)
		}
		d.release(c)
		if err == nil {
			return resp, nil
		}
		if firstErr == nil {
			firstErr = err
		}
		if !isTransportError(err) {
			return nil, firstErr
		}
	}
	return nil, firstErr
}
func (d *vectorDiscovery) DropIndex(ctx context.Context, in *vector_pb.DropIndexRequest, opts ...grpc.CallOption) (resp *vector_pb.DropIndexResponse, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		var c vector_pb.VectorServiceClient
		if c, err = d.cli(ctx); err != nil {
			return nil, err
		}
		resp, err = c.DropIndex(ctx, in, opts...)
		if isTransportError(err) {
			d.reset(c)
		}
		d.release(c)
		if err == nil {
			return resp, nil
		}
		if !isTransportError(err) {
			return nil, err
		}
	}
	return nil, err
}
func (d *vectorDiscovery) PutVectors(ctx context.Context, in *vector_pb.PutVectorsRequest, opts ...grpc.CallOption) (resp *vector_pb.PutVectorsResponse, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		var c vector_pb.VectorServiceClient
		if c, err = d.cli(ctx); err != nil {
			return nil, err
		}
		resp, err = c.PutVectors(ctx, in, opts...)
		if isTransportError(err) {
			d.reset(c)
		}
		d.release(c)
		if err == nil {
			return resp, nil
		}
		if !isTransportError(err) {
			return nil, err
		}
	}
	return nil, err
}
func (d *vectorDiscovery) GetVectors(ctx context.Context, in *vector_pb.GetVectorsRequest, opts ...grpc.CallOption) (resp *vector_pb.GetVectorsResponse, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		var c vector_pb.VectorServiceClient
		if c, err = d.cli(ctx); err != nil {
			return nil, err
		}
		resp, err = c.GetVectors(ctx, in, opts...)
		if isTransportError(err) {
			d.reset(c)
		}
		d.release(c)
		if err == nil {
			return resp, nil
		}
		if !isTransportError(err) {
			return nil, err
		}
	}
	return nil, err
}
func (d *vectorDiscovery) DeleteVectors(ctx context.Context, in *vector_pb.DeleteVectorsRequest, opts ...grpc.CallOption) (resp *vector_pb.DeleteVectorsResponse, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		var c vector_pb.VectorServiceClient
		if c, err = d.cli(ctx); err != nil {
			return nil, err
		}
		resp, err = c.DeleteVectors(ctx, in, opts...)
		if isTransportError(err) {
			d.reset(c)
		}
		d.release(c)
		if err == nil {
			return resp, nil
		}
		if !isTransportError(err) {
			return nil, err
		}
	}
	return nil, err
}
func (d *vectorDiscovery) ListVectors(ctx context.Context, in *vector_pb.ListVectorsRequest, opts ...grpc.CallOption) (resp *vector_pb.ListVectorsResponse, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		var c vector_pb.VectorServiceClient
		if c, err = d.cli(ctx); err != nil {
			return nil, err
		}
		resp, err = c.ListVectors(ctx, in, opts...)
		if isTransportError(err) {
			d.reset(c)
		}
		d.release(c)
		if err == nil {
			return resp, nil
		}
		if !isTransportError(err) {
			return nil, err
		}
	}
	return nil, err
}
func (d *vectorDiscovery) QueryVectors(ctx context.Context, in *vector_pb.QueryVectorsRequest, opts ...grpc.CallOption) (resp *vector_pb.QueryVectorsResponse, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		var c vector_pb.VectorServiceClient
		if c, err = d.cli(ctx); err != nil {
			return nil, err
		}
		resp, err = c.QueryVectors(ctx, in, opts...)
		if isTransportError(err) {
			d.reset(c)
		}
		d.release(c)
		if err == nil {
			return resp, nil
		}
		if !isTransportError(err) {
			return nil, err
		}
	}
	return nil, err
}

func discoverVectorWorkers(masters []pb.ServerAddress, grpcDialOption grpc.DialOption, filerGroup string) []string {
	for _, master := range masters {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := pb.GrpcDial(ctx, master.ToGrpcAddress(), false, grpcDialOption)
		if err != nil {
			cancel()
			continue
		}
		resp, err := master_pb.NewSeaweedClient(conn).ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{
			ClientType: "vector",
			FilerGroup: filerGroup,
		})
		conn.Close()
		cancel()
		if err != nil {
			continue
		}
		workers := make([]string, 0, len(resp.ClusterNodes))
		for _, node := range resp.ClusterNodes {
			glog.V(0).Infof("S3 Vectors: discovered weed-vector at %s via master %s", node.Address, master)
			workers = append(workers, node.Address)
		}
		// A follower can answer with an empty list — workers register with
		// the leader, so keep looking rather than accept "none."
		if len(workers) > 0 {
			return workers
		}
	}
	return nil
}
