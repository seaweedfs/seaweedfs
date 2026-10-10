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

	return util.Retry("filer grpc", func() error {

		i := atomic.LoadInt32(&wfs.option.filerIndex)
		n := len(wfs.option.FilerAddresses)
		for x := 0; x < n; x++ {

			filerGrpcAddress := wfs.option.FilerAddresses[i].ToGrpcAddress()
			err = wfs.callFiler(streamingMode, filerGrpcAddress, fn)

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

// callFiler runs fn against one filer. Callers issue their RPCs on
// context.Background(), so a filer whose transport silently stalls (stopped
// peer, black-holed connection) would otherwise block forever and keep the
// walk from reaching the remaining filer addresses. Bound the call; on
// timeout drop the cached channel so the abandoned attempt is unblocked and
// the next attempt redials.
func (wfs *WFS) callFiler(streamingMode bool, filerGrpcAddress string, fn func(filer_pb.SeaweedFilerClient) error) error {
	if streamingMode {
		return pb.WithGrpcClient(context.Background(), streamingMode, wfs.signature, func(grpcConnection *grpc.ClientConn) error {
			return fn(filer_pb.NewSeaweedFilerClient(grpcConnection))
		}, filerGrpcAddress, false, wfs.option.GrpcDialOption)
	}

	done := make(chan error, 1)
	go func() {
		done <- pb.WithGrpcClient(context.Background(), streamingMode, wfs.signature, func(grpcConnection *grpc.ClientConn) error {
			return fn(filer_pb.NewSeaweedFilerClient(grpcConnection))
		}, filerGrpcAddress, false, wfs.option.GrpcDialOption)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(wfs.filerRPCWait()):
		pb.InvalidateGrpcConnection(filerGrpcAddress)
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
