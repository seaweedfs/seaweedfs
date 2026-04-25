// Package parquet_pushdown implements the SeaweedFS-aware Parquet
// pushdown service. See PARQUET_PUSHDOWN_DESIGN.md and
// PARQUET_PUSHDOWN_DEV_PLAN.md at the repo root for the surrounding
// design and the milestone plan.
//
// M1 wires up the parsed-footer cache and file/range-level pushdown:
// for predicate-less, no-vector requests the service returns the
// column-chunk byte ranges of the projected columns. Predicate /
// vector evaluation lands in later milestones.
package parquet_pushdown

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/parquet_pushdown/footer"
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

// defaultFooterCacheSize is the parsed-footer LRU's default capacity
// when Options.Cache is unset. Footer entries are small (a few KiB
// typically; the heaviest field is the per-row-group ColumnChunks
// slice that scales with column count and row group count). 4096
// covers a few thousand active Parquet files comfortably; can be
// overridden via Options.CacheSize.
const defaultFooterCacheSize = 4096

// Options configures a Service. Loader and Cache are pluggable so
// integration tests can wire deterministic local files; the daemon
// supplies a filer-backed Loader and a sized Cache.
type Options struct {
	// Version is reported in PingResponse and stamped on PushdownStats.
	Version string

	// TrustMode determines request-validation strictness. The actual
	// validation against the catalog lands in M3.
	TrustMode TrustMode

	// Loader resolves DataFile paths to FileHandles. Defaults to
	// LocalLoader{} (filesystem-backed).
	Loader Loader

	// Cache is the parsed-footer LRU. When nil, a cache of CacheSize
	// (or defaultFooterCacheSize) entries is created.
	Cache *footer.Cache

	// CacheSize is the entry cap when Cache is nil.
	CacheSize int
}

// Service implements parquet_pushdown_pb.SeaweedParquetPushdownServer.
// One Service instance per daemon process.
type Service struct {
	pb.UnimplementedSeaweedParquetPushdownServer

	version   string
	trustMode TrustMode
	loader    Loader
	cache     *footer.Cache
}

// New constructs a Service from Options, applying defaults. Returns
// an error if the cache cannot be constructed (e.g. nonsense size).
func New(opts Options) *Service {
	mode := opts.TrustMode
	if mode == "" {
		mode = TrustModeCatalogValidated
	}
	loader := opts.Loader
	if loader == nil {
		loader = LocalLoader{}
	}
	cache := opts.Cache
	if cache == nil {
		size := opts.CacheSize
		if size <= 0 {
			size = defaultFooterCacheSize
		}
		c, err := footer.NewCache(size)
		if err != nil {
			// Size is validated above to be > 0; this is a
			// programmer-error path the cache implementation should
			// never hit. Promoting to panic keeps New non-error and
			// callers oblivious to a configuration knob they did not
			// touch.
			panic(fmt.Errorf("parquet_pushdown: build footer cache: %w", err))
		}
		cache = c
	}
	return &Service{
		version:   opts.Version,
		trustMode: mode,
		loader:    loader,
		cache:     cache,
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
// page byte ranges.
//
// M1 handles only the predicate-less, no-vector path: parse each
// data file's footer (cached by Identity) and return one FileRange
// per (data file, row group, projected column) triple covering that
// column chunk's byte range. Predicate / vector clauses fall through
// to Unimplemented until later milestones.
func (s *Service) Pushdown(ctx context.Context, req *pb.ParquetPushdownRequest) (*pb.ParquetPushdownResponse, error) {
	stats := newStats(s.trustMode)

	if err := validateRequest(req); err != nil {
		return nil, err
	}

	if len(req.Predicate) > 0 || req.VectorQuery != nil {
		st := status.New(codes.Unimplemented, "predicate / vector evaluation lands in a later milestone; M1 only handles file/range-level pushdown")
		if d, err := st.WithDetails(stats.toProto()); err == nil {
			return nil, d.Err()
		}
		return nil, st.Err()
	}

	resp := &pb.ParquetPushdownResponse{}
	for _, df := range req.DataFiles {
		pf, err := s.loadFooter(ctx, df, stats)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "load footer for %q: %v", df.Path, err)
		}
		ranges, err := projectColumnRanges(df.Path, pf, req.Columns)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "project columns for %q: %v", df.Path, err)
		}
		resp.FileRanges = append(resp.FileRanges, ranges...)
	}
	resp.Stats = stats.toProto()
	return resp, nil
}

func (s *Service) loadFooter(ctx context.Context, df *pb.DataFileDescriptor, stats *statsRecorder) (*footer.ParsedFooter, error) {
	id := footer.Identity{
		Path:        df.Path,
		SizeBytes:   df.SizeBytes,
		RecordCount: df.RecordCount,
		ETag:        df.Etag,
	}
	if pf, ok := s.cache.Get(id); ok {
		stats.recordFooterCacheHit()
		return pf, nil
	}
	stats.recordFooterCacheMiss()

	h, err := s.loader.Open(ctx, df.Path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer h.Close()

	pf, err := footer.ParseFromReader(h, h.Size())
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	s.cache.Add(id, pf)
	return pf, nil
}

// projectColumnRanges expands the requested column projection into
// per-row-group byte ranges over the data file. An empty Columns
// list means "every column"; when columns are listed they are
// matched by ColumnRef.Path against the file's leaf-column paths.
//
// Field-id-only projection (Path == "") needs the catalog schema to
// resolve to a leaf path and is therefore unsupported until M3
// (catalog-validated mode) plumbs the schema through.
func projectColumnRanges(filePath string, pf *footer.ParsedFooter, cols []*pb.ColumnRef) ([]*pb.FileRange, error) {
	pathIdx := make(map[string]int, len(pf.ColumnPaths))
	for i, p := range pf.ColumnPaths {
		pathIdx[p] = i
	}

	indices, err := resolveColumnIndices(pathIdx, pf.NumCols, cols)
	if err != nil {
		return nil, err
	}

	out := make([]*pb.FileRange, 0, len(pf.RowGroups)*len(indices))
	for _, rg := range pf.RowGroups {
		for _, idx := range indices {
			cc := rg.ColumnChunks[idx]
			out = append(out, &pb.FileRange{
				File:   filePath,
				Offset: cc.Offset,
				Length: cc.Length,
			})
		}
	}
	return out, nil
}

func resolveColumnIndices(pathIdx map[string]int, numCols int, cols []*pb.ColumnRef) ([]int, error) {
	if len(cols) == 0 {
		out := make([]int, numCols)
		for i := range out {
			out[i] = i
		}
		return out, nil
	}
	out := make([]int, 0, len(cols))
	for _, c := range cols {
		if c == nil {
			return nil, fmt.Errorf("nil column ref")
		}
		if c.Path == "" {
			return nil, fmt.Errorf("column ref field_id=%d has no path hint; field-id-only resolution requires catalog schema (M3)", c.FieldId)
		}
		idx, ok := pathIdx[c.Path]
		if !ok {
			return nil, fmt.Errorf("column %q not in file", c.Path)
		}
		out = append(out, idx)
	}
	return out, nil
}
