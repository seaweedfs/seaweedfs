package command

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/parquet_pushdown"
	"github.com/seaweedfs/seaweedfs/weed/parquet_pushdown/daemon"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

var (
	pushdownOptions PushdownOptions
)

type PushdownOptions struct {
	filer               *string
	ip                  *string
	port                *int
	trustMode           *string
	connectorTrustedAck *bool
}

func init() {
	cmdPushdown.Run = runPushdown // break init cycle
	pushdownOptions.filer = cmdPushdown.Flag.String("filer", "localhost:8888", "comma-separated filer server addresses for high availability")
	pushdownOptions.ip = cmdPushdown.Flag.String("ip", util.DetectedHostAddress(), "pushdown daemon gRPC listen ip address")
	pushdownOptions.port = cmdPushdown.Flag.Int("port", 18888, "pushdown daemon gRPC listen port")
	pushdownOptions.trustMode = cmdPushdown.Flag.String("trust", string(parquet_pushdown.TrustModeCatalogValidated), "trust mode: catalog-validated (default) or connector-trusted (dev only)")
	pushdownOptions.connectorTrustedAck = cmdPushdown.Flag.Bool("dev.connector_trusted_ack", false, "explicitly acknowledge running with -trust=connector-trusted; required for that mode")
}

var cmdPushdown = &Command{
	UsageLine: "pushdown [-port=18888] [-filer=<ip:port>[,<ip:port>]...] [-trust=catalog-validated]",
	Short:     "<WIP> start the SeaweedFS Parquet pushdown daemon (M0 skeleton)",
	Long: `start the SeaweedFS Parquet pushdown daemon.

	The pushdown daemon is a standalone gRPC service that accelerates Parquet/
	Iceberg scans by pruning files, row groups, and pages before the engine
	reads data. See PARQUET_PUSHDOWN_DESIGN.md and PARQUET_PUSHDOWN_DEV_PLAN.md
	at the repo root for the full design and milestone plan.

	M0 ships request validation and the gRPC surface only; pruning logic
	lands in M1+.

	Trust modes:
	  catalog-validated (default)
	      Server validates each request's DataFiles and Deletes against the
	      Iceberg catalog at the requested snapshot before serving.
	  connector-trusted
	      Skip catalog validation. Developer-only; production builds must
	      not enable. Requires the explicit -dev.connector_trusted_ack flag.

	Multiple filer addresses can be specified for high availability,
	separated by commas.`,
}

func runPushdown(cmd *Command, args []string) bool {
	cfg := daemon.Config{
		IP:                  *pushdownOptions.ip,
		Port:                *pushdownOptions.port,
		FilerAddresses:      pb.ServerAddresses(*pushdownOptions.filer).ToAddresses(),
		TrustMode:           parquet_pushdown.TrustMode(*pushdownOptions.trustMode),
		ConnectorTrustedAck: *pushdownOptions.connectorTrustedAck,
		Version:             version.Version(),
	}
	if err := daemon.Run(cfg); err != nil {
		glog.Fatalf("pushdown daemon: %v", err)
	}
	return true
}
