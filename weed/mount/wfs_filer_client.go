package mount

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var _ = filer_pb.FilerClient(&WFS{})

func (wfs *WFS) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) (err error) {
	return wfs.withFilerClient(context.Background(), streamingMode, wfs.filerRPCWait(), func(_ context.Context, client filer_pb.SeaweedFilerClient) error {
		return fn(client)
	})
}

// withFilerClient walks the filer addresses until one answers fn. callWait
// bounds one attempt; callWait <= 0 leaves the attempt to ctx alone, for
// calls whose duration the metadata bound cannot anticipate (a whole-file
// remote download). Streaming calls ignore callWait — their callbacks bound
// themselves (the rename fallback arms its own silence timer). The callback
// must run its RPCs under the supplied context: that is what lets a
// timed-out attempt be cancelled without disturbing the shared cached
// channel's other callers.
func (wfs *WFS) withFilerClient(ctx context.Context, streamingMode bool, callWait time.Duration, fn func(ctx context.Context, client filer_pb.SeaweedFilerClient) error) (err error) {

	return util.Retry("filer grpc", func() error {

		i := atomic.LoadInt32(&wfs.option.filerIndex)
		n := len(wfs.option.FilerAddresses)
		for x := 0; x < n; x++ {

			filerGrpcAddress := wfs.option.FilerAddresses[i].ToGrpcAddress()
			err = wfs.callFiler(ctx, streamingMode, callWait, filerGrpcAddress, fn)

			if err != nil {
				// glog.V(0).Infof("WithFilerClient %d %v: %v", x, filerGrpcAddress, err)
			} else {
				atomic.StoreInt32(&wfs.option.filerIndex, i)
				return nil
			}

			i++
			if i >= int32(n) {
				i = 0
			}

		}
		return err
	})

}

// filerRPCWait bounds one filer call when the callers' RPCs carry no deadline
// of their own. filerCallTimeout is only settable on the WFS for tests.
func (wfs *WFS) filerRPCWait() time.Duration {
	if wfs.filerCallTimeout > 0 {
		return wfs.filerCallTimeout
	}
	return filerRPCTimeout
}

// callFiler runs fn against one filer under a per-attempt context derived
// from ctx, so a filer whose transport silently stalls (stopped peer,
// black-holed connection) cannot block forever and keep the walk from
// reaching the remaining filer addresses. Cancelling the attempt aborts its
// RPCs on the shared cached channel without tearing that channel down.
//
// On timeout the call waits for the attempt's callback to unwind before
// returning: a slow attempt must not overlap the next filer's run of the
// same closure and clobber the caller's shared result variables. A callback
// that ignored the attempt context is forced out by dropping the channel.
func (wfs *WFS) callFiler(ctx context.Context, streamingMode bool, callWait time.Duration, filerGrpcAddress string, fn func(ctx context.Context, client filer_pb.SeaweedFilerClient) error) error {
	if streamingMode {
		return pb.WithGrpcClient(ctx, streamingMode, wfs.signature, func(grpcConnection *grpc.ClientConn) error {
			return fn(ctx, filer_pb.NewSeaweedFilerClient(grpcConnection))
		}, filerGrpcAddress, false, wfs.option.GrpcDialOption)
	}

	attemptCtx, cancel := context.WithCancel(ctx)
	if callWait > 0 {
		attemptCtx, cancel = context.WithTimeout(ctx, callWait)
	}
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- pb.WithGrpcClient(attemptCtx, streamingMode, wfs.signature, func(grpcConnection *grpc.ClientConn) error {
			return fn(attemptCtx, filer_pb.NewSeaweedFilerClient(grpcConnection))
		}, filerGrpcAddress, false, wfs.option.GrpcDialOption)
	}()

	select {
	case err := <-done:
		if err != nil && ctx.Err() == nil && attemptCtx.Err() == context.DeadlineExceeded {
			// fn surfaced the attempt deadline itself; report it like the
			// timer path so the failure reads as a filer timeout, not as a
			// caller cancellation (which IsTransientError would not retry).
			return status.Error(codes.Unavailable, fmt.Sprintf("filer %s: call timed out", filerGrpcAddress))
		}
		return err
	case <-attemptCtx.Done():
		// Give the callback one RPC's grace to unwind on its own; only if it
		// ignored the attempt context does the cached channel get dropped —
		// the collateral is borne by calls to the same unresponsive filer,
		// which the walk is abandoning anyway.
		drain := time.NewTimer(wfs.filerRPCWait())
		defer drain.Stop()
		select {
		case <-done:
		case <-drain.C:
			pb.InvalidateGrpcConnection(filerGrpcAddress)
			<-done
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return status.Error(codes.Unavailable, fmt.Sprintf("filer %s: call timed out", filerGrpcAddress))
	}
}

func (wfs *WFS) AdjustedUrl(location *filer_pb.Location) string {
	if wfs.option.VolumeServerAccess == "publicUrl" {
		return location.PublicUrl
	}
	return location.Url
}

func (wfs *WFS) GetDataCenter() string {
	return wfs.option.DataCenter
}
