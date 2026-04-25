package command

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	pb "github.com/seaweedfs/seaweedfs/weed/pb/parquet_pushdown_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	pushdownPingOptions PushdownPingOptions
)

type PushdownPingOptions struct {
	target  *string
	timeout *time.Duration
	useTLS  *bool
}

func init() {
	cmdPushdownPing.Run = runPushdownPing
	pushdownPingOptions.target = cmdPushdownPing.Flag.String("server", "localhost:18888", "pushdown daemon gRPC address")
	pushdownPingOptions.timeout = cmdPushdownPing.Flag.Duration("timeout", 5*time.Second, "RPC deadline")
	pushdownPingOptions.useTLS = cmdPushdownPing.Flag.Bool("tls", false, "dial with TLS using grpc.client config")
}

var cmdPushdownPing = &Command{
	UsageLine: "pushdown.ping [-server=<ip:port>]",
	Short:     "<WIP> ping a running pushdown daemon (M0 smoke test)",
	Long: `ping a running pushdown daemon.

	Calls the SeaweedParquetPushdown.Ping RPC and prints the daemon's
	reported version and trust mode. Used as a deployment smoke test
	and by the M0 integration test.`,
}

func runPushdownPing(cmd *Command, args []string) bool {
	util.LoadSecurityConfiguration()

	dialOpt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if *pushdownPingOptions.useTLS {
		dialOpt = security.LoadClientTLS(util.GetViper(), "grpc.client")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *pushdownPingOptions.timeout)
	defer cancel()

	conn, err := grpc.NewClient(*pushdownPingOptions.target, dialOpt)
	if err != nil {
		glog.Errorf("dial %s: %v", *pushdownPingOptions.target, err)
		return false
	}
	defer conn.Close()

	resp, err := pb.NewSeaweedParquetPushdownClient(conn).Ping(ctx, &pb.PingRequest{})
	if err != nil {
		glog.Errorf("ping %s: %v", *pushdownPingOptions.target, err)
		return false
	}

	fmt.Fprintf(os.Stdout, "ok: server=%s version=%q trust=%q\n", *pushdownPingOptions.target, resp.Version, resp.TrustMode)
	return true
}
