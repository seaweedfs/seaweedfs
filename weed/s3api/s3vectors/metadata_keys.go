package s3vectors

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// MetadataColumnsCapability is the gRPC health service an engine reports
// serving when its writes fill indexed metadata key columns.
const MetadataColumnsCapability = "weed.vector.capability.metadata_columns"

// MetadataKeys lists the indexed metadata keys of an index, as its table
// records them. The caller is trusted: the shell and the admin server.
func (s *Server) MetadataKeys(ctx context.Context, bucket, index string) (*vector_pb.MetadataKeysResponse, error) {
	if _, err := s.trustedIndex(ctx, bucket, index); err != nil {
		return nil, err
	}
	if !s.hasEngine() {
		return nil, errNoEngine
	}
	return s.vectors.MetadataKeys(ctx, &vector_pb.MetadataKeysRequest{Table: tableID(bucket, index)})
}

// UpdateMetadataKey asks for a metadata key of an index to be indexed, or no
// longer. Adding refuses a non-filterable key, and refuses while a registered
// engine does not fill indexed key columns on its writes.
func (s *Server) UpdateMetadataKey(ctx context.Context, bucket, index, key string, remove bool) (*vector_pb.MetadataKeysResponse, error) {
	idx, err := s.trustedIndex(ctx, bucket, index)
	if err != nil {
		return nil, err
	}
	if !s.hasEngine() {
		return nil, errNoEngine
	}
	if !remove {
		if err := metadataKeyIndexable(idx, key); err != nil {
			return nil, err
		}
		if err := s.checkMetadataColumns(ctx); err != nil {
			return nil, err
		}
	}
	return s.vectors.UpdateMetadataKey(ctx, &vector_pb.UpdateMetadataKeyRequest{
		Table:  tableID(bucket, index),
		Key:    key,
		Remove: remove,
	})
}

var errNoEngine = fmt.Errorf("no vector engine is configured")

// hasEngine reports whether calls can reach an engine client.
func (s *Server) hasEngine() bool {
	backend := s.vectors
	if described, ok := backend.(*describedClient); ok {
		backend = described.VectorServiceClient
	}
	return backend != nil
}

func (s *Server) trustedIndex(ctx context.Context, bucket, index string) (*Index, error) {
	var record map[string]interface{}
	if err := s.internalCatalog(ctx, "GetTable", &s3tables.GetTableRequest{
		TableBucketARN: s.s3tablesArn(bucket),
		Namespace:      []string{indexNamespace},
		Name:           index,
	}, &record); err != nil {
		return nil, err
	}
	return indexFromRecord(s, bucket, index, record), nil
}

func metadataKeyIndexable(idx *Index, key string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("the metadata key is empty")
	}
	if idx.MetadataConfiguration != nil && slices.Contains(idx.MetadataConfiguration.NonFilterableMetadataKeys, key) {
		return fmt.Errorf("%s is a non-filterable metadata key of index %s; only filterable keys can be indexed", key, idx.IndexName)
	}
	return nil
}

// checkMetadataColumns refuses unless every registered engine reports the
// capability, when the engines come from master discovery.
func (s *Server) checkMetadataColumns(ctx context.Context) error {
	backend := s.vectors
	if described, ok := backend.(*describedClient); ok {
		backend = described.VectorServiceClient
	}
	if d, ok := backend.(*vectorDiscovery); ok {
		return d.CheckMetadataColumns(ctx)
	}
	return nil
}

// CheckMetadataColumns returns an error unless engines are registered and
// each reports MetadataColumnsCapability.
func (d *vectorDiscovery) CheckMetadataColumns(ctx context.Context) error {
	return checkMetadataColumns(ctx, d.discover(d.masters(), d.grpcDialOption), d.grpcDialOption)
}

func checkMetadataColumns(ctx context.Context, engines []string, dialOption grpc.DialOption) error {
	if len(engines) == 0 {
		return fmt.Errorf("no vector engine is registered")
	}
	var lacking []string
	for _, address := range engines {
		if err := engineReports(ctx, address, dialOption, MetadataColumnsCapability); err != nil {
			lacking = append(lacking, fmt.Sprintf("%s (%v)", address, err))
		}
	}
	if len(lacking) > 0 {
		slices.Sort(lacking)
		return fmt.Errorf("vector engines that do not fill indexed metadata key columns are registered; upgrade them first: %s", strings.Join(lacking, ", "))
	}
	return nil
}

func engineReports(ctx context.Context, address string, dialOption grpc.DialOption, service string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := pb.GrpcDial(ctx, address, false, dialOption)
	if err != nil {
		return err
	}
	defer conn.Close()
	resp, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: service})
	if err != nil {
		return err
	}
	if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("%s is %s", service, resp.Status)
	}
	return nil
}
