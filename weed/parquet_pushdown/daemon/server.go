package daemon

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/parquet_pushdown"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	pbpushdown "github.com/seaweedfs/seaweedfs/weed/pb/parquet_pushdown_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
)

// Run starts the pushdown daemon. It blocks until the gRPC server
// returns (graceful shutdown on SIGTERM, or error from Serve). Always
// validates the config first.
func Run(cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("pushdown config: %w", err)
	}

	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	// Probe the filer until reachable so we fail fast on bad config
	// and emit a clear "waiting for filers" log otherwise. Bound the
	// probe so the daemon doesn't sit forever if the operator hands
	// in an unreachable filer.
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer probeCancel()
	fc := newFilerClient(cfg.FilerAddresses, grpcDialOption)
	if err := fc.waitUntilReachable(probeCtx); err != nil {
		return fmt.Errorf("filer probe failed: %w", err)
	}

	svc := parquet_pushdown.New(parquet_pushdown.Options{
		Version:   cfg.Version,
		TrustMode: cfg.TrustMode,
	})

	grpcL, localL, err := util.NewIpAndLocalListeners(cfg.IP, cfg.Port, 0)
	if err != nil {
		return fmt.Errorf("listen on grpc port %d: %w", cfg.Port, err)
	}

	grpcS := pb.NewGrpcServer()
	pbpushdown.RegisterSeaweedParquetPushdownServer(grpcS, svc)
	reflection.Register(grpcS)

	grace.OnInterrupt(grpcS.GracefulStop)

	if localL != nil {
		localGrpcS := pb.NewGrpcServer()
		pbpushdown.RegisterSeaweedParquetPushdownServer(localGrpcS, svc)
		reflection.Register(localGrpcS)
		grace.OnInterrupt(localGrpcS.GracefulStop)
		go serve(localGrpcS, localL, "pushdown localhost")
	}

	glog.V(0).Infof("pushdown daemon listening on %s:%d (trust=%s)", cfg.IP, cfg.Port, cfg.TrustMode)
	return serve(grpcS, grpcL, "pushdown")
}

func serve(s *grpc.Server, l net.Listener, label string) error {
	if err := s.Serve(l); err != nil && err != grpc.ErrServerStopped {
		glog.Errorf("%s server serve error: %v", label, err)
		return err
	}
	return nil
}
