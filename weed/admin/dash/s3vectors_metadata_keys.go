package dash

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3vectors"
	"golang.org/x/sync/errgroup"
)

// metadataIndexJobType is the worker job that adds, maintains and removes
// indexed metadata keys.
const metadataIndexJobType = "lance_metadata_index"

// frequentFilterKeys caps the "frequently filtered, not indexed" list.
const frequentFilterKeys = 10

// S3VectorsIndexPanel is one index's card on the bucket page: its vector
// count and vector index, its indexed metadata keys, and the keys its
// queries filter on most that are not indexed.
type S3VectorsIndexPanel struct {
	Index       *s3vectors.Index          `json:"index"`
	Vectors     uint64                    `json:"vectors"`
	VectorIndex string                    `json:"vector_index"`
	Keys        []S3VectorsMetadataKeyRow `json:"keys"`
	Frequent    []S3VectorsFilterKeyRow   `json:"frequent"`
	Error       string                    `json:"error,omitempty"`
	StatsError  string                    `json:"stats_error,omitempty"`
	// StatsTruncated marks stats from an engine that stopped counting new
	// keys, so Frequent can miss keys.
	StatsTruncated bool                       `json:"stats_truncated,omitempty"`
	stats          map[string]*filterKeyTotal `json:"-"`
}

type S3VectorsMetadataKeyRow struct {
	Key            string  `json:"key"`
	State          string  `json:"state"`
	Coverage       float64 `json:"coverage"`
	QueriesPerHour float64 `json:"queries_per_hour"`
	IndexBytes     uint64  `json:"index_bytes"`
}

type S3VectorsFilterKeyRow struct {
	Key            string  `json:"key"`
	QueriesPerHour float64 `json:"queries_per_hour"`
	AvgMs          float64 `json:"avg_ms"`
	// Numeric marks a key filtered only with numbers, which phase 1 does
	// not index.
	Numeric bool `json:"numeric"`
}

type filterKeyTotal struct {
	perHour float64
	seconds float64
	queries uint64
	strings bool
	numbers bool
}

// s3VectorsIndexPanel reads one index's keys from its table through an
// engine, and its filter stats from every registered engine.
func (s *AdminServer) s3VectorsIndexPanel(ctx context.Context, bucket string, index *s3vectors.Index, engines []VectorNode) S3VectorsIndexPanel {
	panel := S3VectorsIndexPanel{Index: index, Keys: []S3VectorsMetadataKeyRow{}, Frequent: []S3VectorsFilterKeyRow{}}
	keysCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	keys, err := s.s3VectorsServer.MetadataKeys(keysCtx, bucket, index.IndexName)
	if err != nil {
		panel.Error = err.Error()
	}
	stats, truncated, err := s.s3VectorsFilterStats(ctx, bucket, index.IndexName, engines)
	panel.StatsTruncated = truncated
	if err != nil {
		panel.StatsError = err.Error()
	}
	panel.stats = stats
	indexed := map[string]bool{}
	if keys != nil {
		panel.Vectors = keys.Rows
		panel.VectorIndex = keys.VectorIndex
		for _, k := range keys.Keys {
			indexed[k.Key] = true
			row := S3VectorsMetadataKeyRow{Key: k.Key, State: k.State, IndexBytes: k.IndexBytes}
			if k.Rows > 0 {
				row.Coverage = float64(k.IndexedRows) * 100 / float64(k.Rows)
			}
			if total := stats[k.Key]; total != nil {
				row.QueriesPerHour = total.perHour
			}
			panel.Keys = append(panel.Keys, row)
		}
	}
	nonFilterable := map[string]bool{}
	if index.MetadataConfiguration != nil {
		for _, k := range index.MetadataConfiguration.NonFilterableMetadataKeys {
			nonFilterable[k] = true
		}
	}
	for key, total := range stats {
		if indexed[key] || nonFilterable[key] || (!total.strings && !total.numbers) {
			continue
		}
		row := S3VectorsFilterKeyRow{Key: key, QueriesPerHour: total.perHour, Numeric: !total.strings}
		if total.queries > 0 {
			row.AvgMs = total.seconds * 1000 / float64(total.queries)
		}
		panel.Frequent = append(panel.Frequent, row)
	}
	sort.Slice(panel.Frequent, func(i, j int) bool {
		if panel.Frequent[i].QueriesPerHour != panel.Frequent[j].QueriesPerHour {
			return panel.Frequent[i].QueriesPerHour > panel.Frequent[j].QueriesPerHour
		}
		return panel.Frequent[i].Key < panel.Frequent[j].Key
	})
	if len(panel.Frequent) > frequentFilterKeys {
		panel.Frequent = panel.Frequent[:frequentFilterKeys]
	}
	return panel
}

// s3VectorsFilterStats sums an index's filter stats over the engines, each
// counted per hour over its own window.
func (s *AdminServer) s3VectorsFilterStats(ctx context.Context, bucket, index string, engines []VectorNode) (map[string]*filterKeyTotal, bool, error) {
	responses := make([]*vector_pb.FilterStatsResponse, len(engines))
	errs := make([]error, len(engines))
	var calls errgroup.Group
	calls.SetLimit(8)
	for i, engine := range engines {
		calls.Go(func() error {
			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			conn, err := pb.GrpcDial(ctx, engine.Address, false, s.grpcDialOption)
			if err != nil {
				errs[i] = err
				return nil
			}
			defer conn.Close()
			responses[i], errs[i] = vector_pb.NewVectorServiceClient(conn).FilterStats(ctx, &vector_pb.FilterStatsRequest{
				Table: &vector_pb.Table{Id: []string{bucket, "vector", index}},
			})
			return nil
		})
	}
	_ = calls.Wait()
	stats, truncated := sumFilterStats(responses)
	return stats, truncated, firstError(engines, errs)
}

func sumFilterStats(responses []*vector_pb.FilterStatsResponse) (map[string]*filterKeyTotal, bool) {
	out := map[string]*filterKeyTotal{}
	truncated := false
	for _, resp := range responses {
		if resp == nil || resp.WindowSeconds == 0 {
			continue
		}
		truncated = truncated || resp.Truncated
		for _, k := range resp.Keys {
			total := out[k.Key]
			if total == nil {
				total = &filterKeyTotal{}
				out[k.Key] = total
			}
			total.perHour += float64(k.Queries) * 3600 / float64(resp.WindowSeconds)
			total.queries += k.Queries
			total.seconds += k.Seconds
			total.strings = total.strings || k.StringOperand
			total.numbers = total.numbers || k.NumberOperand
		}
	}
	return out, truncated
}

func firstError(engines []VectorNode, errs []error) error {
	for i, err := range errs {
		if err != nil {
			return fmt.Errorf("filter stats from %s: %w", engines[i].Address, err)
		}
	}
	return nil
}

// UpdateS3VectorsMetadataKeyAPI asks to index a metadata key (POST) or to
// stop (DELETE), then dispatches the worker job that carries it out.
func (s *AdminServer) UpdateS3VectorsMetadataKeyAPI(w http.ResponseWriter, r *http.Request) {
	if !requireSessionCSRFToken(w, r) {
		return
	}
	var req struct {
		Bucket string `json:"bucket"`
		Index  string `json:"index"`
		Key    string `json:"key"`
	}
	if r.Method == http.MethodDelete {
		q := r.URL.Query()
		req.Bucket, req.Index, req.Key = q.Get("bucket"), q.Get("index"), q.Get("key")
	} else if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.Bucket == "" || req.Index == "" || req.Key == "" {
		writeJSONError(w, http.StatusBadRequest, "bucket, index and key are required")
		return
	}
	remove := r.Method == http.MethodDelete
	keys, err := s.s3VectorsServer.UpdateMetadataKey(r.Context(), req.Bucket, req.Index, req.Key, remove)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	// A withdrawn request leaves nothing for a worker to do.
	action := "add"
	if remove {
		action = ""
		for _, k := range keys.GetKeys() {
			if k.Key == req.Key && k.State == "off" {
				action = "remove"
			}
		}
	}
	dispatched := false
	if action != "" {
		dispatched = s.dispatchMetadataIndexJob(req.Bucket, req.Index, req.Key, action)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"keys": keys.GetKeys(), "job_dispatched": dispatched})
}

// dispatchMetadataIndexJob runs the job for one key in the background when
// a worker can; otherwise the job's detection finds the work.
func (s *AdminServer) dispatchMetadataIndexJob(bucket, index, key, action string) bool {
	if s.plugin == nil || !s.hasCapableWorker(metadataIndexJobType) {
		return false
	}
	job := &plugin_pb.JobSpec{
		JobId:     uuid.NewString(),
		JobType:   metadataIndexJobType,
		DedupeKey: fmt.Sprintf("%s:%s$vector$%s:%s:%s", metadataIndexJobType, bucket, index, key, action),
		Summary:   fmt.Sprintf("%s metadata key %s of %s/%s", action, key, bucket, index),
		Parameters: map[string]*plugin_pb.ConfigValue{
			"table_id": {Kind: &plugin_pb.ConfigValue_StringList{StringList: &plugin_pb.StringList{Values: []string{bucket, "vector", index}}}},
			"key":      {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: key}},
			"action":   {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: action}},
		},
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		if _, err := s.plugin.ExecuteJob(ctx, job, s.buildDefaultPluginClusterContext(), 1); err != nil {
			glog.Warningf("%s for %s/%s key %s: %v", metadataIndexJobType, bucket, index, key, err)
		}
	}()
	return true
}

func (s *AdminServer) hasCapableWorker(jobType string) bool {
	for _, worker := range s.GetPluginWorkers() {
		if capability := worker.Capabilities[jobType]; capability != nil && capability.CanExecute {
			return true
		}
	}
	return false
}
