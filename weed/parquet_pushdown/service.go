// Package parquet_pushdown implements the SeaweedFS-aware Parquet
// pushdown service. See PARQUET_PUSHDOWN_DESIGN.md and
// PARQUET_PUSHDOWN_DEV_PLAN.md at the repo root for the surrounding
// design and the milestone plan.
//
// M0 wires up the gRPC service and request validation; the actual
// pruning logic (parsed-footer cache, row-group pruning, scalar/page
// indexes, deletes, vectors) lands in M1+.
package parquet_pushdown

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/seaweedfs/seaweedfs/weed/pb/parquet_pushdown_pb"
)

// TrustMode controls how the server validates the request's
// DataFiles and Deletes against the Iceberg catalog. See the design's
// "Trust Model and Catalog Validation" section.
type TrustMode string

const (
	// TrustModeCatalogValidated is the default. The server reads the
	// Iceberg snapshot and verifies every DataFileDescriptor and
	// DeleteFileRef against the manifest before serving.
	TrustModeCatalogValidated TrustMode = "catalog-validated"

	// TrustModeConnectorTrusted is a developer-only mode that skips
	// catalog validation. Production builds must reject this mode at
	// configuration time.
	TrustModeConnectorTrusted TrustMode = "connector-trusted"
)

// Options configures a Service. Fields the M0 skeleton does not yet
// consume (catalog client, filer client, predicate engine) will be
// added in their respective milestones.
type Options struct {
	// Version is reported in PingResponse and stamped on PushdownStats.
	Version string

	// TrustMode determines request-validation strictness. M0 only
	// stamps the value into the response; the actual validation
	// against the catalog lands in M3.
	TrustMode TrustMode
}

// Service implements parquet_pushdown_pb.SeaweedParquetPushdownServer.
// One Service instance per daemon process.
type Service struct {
	pb.UnimplementedSeaweedParquetPushdownServer

	version   string
	trustMode TrustMode
}

// New constructs a Service from Options. The caller is responsible
// for registering it on a gRPC server (see weed/parquet_pushdown/daemon).
func New(opts Options) *Service {
	mode := opts.TrustMode
	if mode == "" {
		mode = TrustModeCatalogValidated
	}
	return &Service{
		version:   opts.Version,
		trustMode: mode,
	}
}

// Ping returns daemon liveness information. Used by smoke tests and
// connector health checks. Cheap; never reads files.
func (s *Service) Ping(_ context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		Version:   s.version,
		TrustMode: string(s.trustMode),
	}, nil
}

// Pushdown evaluates a request and returns surviving file/row-group/
// page byte ranges. M0 returns Unimplemented after request-shape
// validation; later milestones replace the body.
//
// Unimplemented is attached with a PushdownStats detail so the wire
// format for stats is exercised end-to-end on M0.
func (s *Service) Pushdown(_ context.Context, req *pb.ParquetPushdownRequest) (*pb.ParquetPushdownResponse, error) {
	stats := newStats(s.trustMode)

	if err := validateRequest(req); err != nil {
		return nil, err
	}

	st := status.New(codes.Unimplemented, "parquet pushdown evaluation lands in M1; M0 only validates request shape")
	if withDetails, err := st.WithDetails(stats.toProto()); err == nil {
		return nil, withDetails.Err()
	}
	return nil, st.Err()
}
