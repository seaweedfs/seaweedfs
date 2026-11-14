package s3_lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// failoverClient sends LifecycleDelete to one S3 front and, when that front
// becomes unreachable, moves the pass to the next front that passes
// checkLifecycleReady. A front that failed, including one selectS3Front
// skipped before addr, is not selected again in the pass.
type failoverClient struct {
	ctx       context.Context
	h         *Handler
	endpoints []string
	report    func(string)

	mu        sync.Mutex
	conn      *grpc.ClientConn
	addr      string
	client    lifecycleRPCAdapter
	failed    map[string]bool
	exhausted error
}

func (h *Handler) newFailoverClient(ctx context.Context, conn *grpc.ClientConn, addr string, endpoints []string, report func(string)) *failoverClient {
	f := &failoverClient{ctx: ctx, h: h, endpoints: endpoints, report: report, failed: map[string]bool{}}
	for _, e := range endpoints {
		if e == addr {
			break
		}
		f.failed[e] = true
	}
	f.use(conn, addr)
	return f
}

func (f *failoverClient) use(conn *grpc.ClientConn, addr string) {
	f.conn, f.addr = conn, addr
	f.client = lifecycleRPCAdapter{c: s3_lifecycle_pb.NewSeaweedS3LifecycleInternalClient(conn)}
}

// LifecycleDelete resends the same request after each switch: the server's
// identity check resolves a delete that landed before the reply was lost
// instead of applying it twice. Every switch retires a front, which bounds
// the loop.
func (f *failoverClient) LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	for {
		f.mu.Lock()
		conn, client, exhausted := f.conn, f.client, f.exhausted
		f.mu.Unlock()
		if exhausted != nil {
			return nil, exhausted
		}
		resp, err := client.LifecycleDelete(ctx, req)
		if code := status.Code(err); err == nil || ctx.Err() != nil || (code != codes.Unavailable && code != codes.Canceled) {
			return resp, err
		}
		if err := f.failover(conn, err); err != nil {
			return nil, err
		}
	}
}

// failover replaces the front behind failing when it is unavailable; a call
// cut off by an earlier switch closing its connection just moves on.
func (f *failoverClient) failover(failing *grpc.ClientConn, cause error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.exhausted != nil {
		return f.exhausted
	}
	if f.conn != failing {
		return nil
	}
	code := status.Code(cause)
	if code != codes.Unavailable {
		return cause
	}
	from := f.addr
	f.failed[from] = true
	f.conn.Close()
	f.conn = nil

	var remaining []string
	for _, addr := range f.endpoints {
		if !f.failed[addr] {
			remaining = append(remaining, addr)
		}
	}
	conn, addr, skipped := f.h.selectS3Front(f.ctx, remaining)
	if f.ctx.Err() == nil {
		for _, s := range remaining {
			if s == addr {
				break
			}
			f.failed[s] = true
		}
	}
	lost := fmt.Errorf("s3 %s: %s", from, code)
	if conn == nil {
		f.exhausted = noReadyS3Error(append([]error{lost}, skipped...))
		glog.Warningf("s3 lifecycle: %v", f.exhausted)
		f.report(f.exhausted.Error())
		return f.exhausted
	}
	f.use(conn, addr)
	msg := fmt.Sprintf("switched to s3 %s, %v", addr, lost)
	if len(skipped) > 0 {
		msg += fmt.Sprintf(", skipped: %v", errors.Join(skipped...))
	}
	glog.Warningf("s3 lifecycle: %s", msg)
	f.report(msg)
	return nil
}

func (f *failoverClient) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn != nil {
		f.conn.Close()
		f.conn = nil
	}
}
