package stats

import (
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// SetVersionInfo sets the version information for the BuildInfo metric
// This is called by the version package during initialization.
// It uses sync.Once to ensure the build information is set only once,
// making it safe to call multiple times while ensuring immutability.
var SetVersionInfo = func() func(string, string, string) {
	var once sync.Once
	return func(version, commitHash, sizeLimit string) {
		once.Do(func() {
			BuildInfo.WithLabelValues(version, commitHash, sizeLimit, runtime.GOOS, runtime.GOARCH).Set(1)
		})
	}
}()

// Readonly volume types
const (
	Namespace        = "SeaweedFS"
	IsReadOnly       = "IsReadOnly"
	NoWriteOrDelete  = "noWriteOrDelete"
	NoWriteCanDelete = "noWriteCanDelete"
	IsDiskSpaceLow   = "isDiskSpaceLow"
	bucketAtiveTTL   = 10 * time.Minute
)

// Prometheus metric subsystems.
const (
	subsystemBuild        = "build"
	subsystemMaster       = "master"
	subsystemWDClient     = "wdclient"
	subsystemFiler        = "filer"
	subsystemFilerStore   = "filerStore"
	subsystemFilerSync    = "filerSync"
	subsystemVolumeServer = "volumeServer"
	subsystemS3           = "s3"
	subsystemS3Lifecycle  = "s3_lifecycle"
	subsystemAdmin        = "admin"
)

var bucketLastActiveTsNs map[string]int64 = map[string]int64{}
var bucketLastActiveLock sync.Mutex

var (
	Gather = prometheus.NewRegistry()

	BuildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemBuild,
			Name:      "info",
			Help:      "A metric with a constant '1' value labeled by version, commit, sizelimit, goos, and goarch from which SeaweedFS was built.",
		}, []string{"version", "commit", "sizelimit", "goos", "goarch"})

	MasterStartTimeSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemMaster,
			Name:      "start_time_seconds",
			Help:      "Start time of the master, as seconds since UNIX epoch.",
		})

	MasterClientConnectCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemWDClient,
			Name:      "connect_updates",
			Help:      "Counter of master client leader updates.",
		}, []string{"type"})

	MasterRaftIsleader = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemMaster,
			Name:      "is_leader",
			Help:      "is leader",
		})

	MasterAdminLock = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemMaster,
			Name:      "admin_lock",
			Help:      "admin lock",
		}, []string{"client"})

	MasterReceivedHeartbeatCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemMaster,
			Name:      "received_heartbeats",
			Help:      "Counter of master received heartbeat.",
		}, []string{"type"})

	MasterReplicaPlacementMismatch = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemMaster,
			Name:      "replica_placement_mismatch",
			Help:      "replica placement mismatch",
		}, []string{"collection", "id"})

	MasterVolumeLayoutWritable = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemMaster,
			Name:      "volume_layout_writable",
			Help:      "Number of writable volumes in volume layouts",
		}, []string{"collection", "disk", "rp", "ttl"})

	MasterVolumeLayoutCrowded = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemMaster,
			Name:      "volume_layout_crowded",
			Help:      "Number of crowded volumes in volume layouts",
		}, []string{"collection", "disk", "rp", "ttl"})

	// MasterUnderReplicatedVolumes tracks volumes that do not have enough replicas,
	// partitioned by collection, disk type, replication type, and TTL.
	MasterUnderReplicatedVolumes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemMaster,
			Name:      "under_replicated_volumes",
			Help:      "Current number of volumes that do not have enough replicas per collection/layout. 0 = healthy.",
		}, []string{"collection", "disk", "rp", "ttl"})

	// MasterVolumeCreationCounter counts volume creation (growth) operations by result (success, failure).
	// Volume growth is orchestrated by the master, so this metric is exported by the master process.
	MasterVolumeCreationCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemMaster,
			Name:      "volume_creation_total",
			Help:      "Counter of volume creation operations by result (success, failure).",
		}, []string{"result"})

	MasterPickForWriteErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemMaster,
			Name:      "pick_for_write_error",
			Help:      "Counter of master pick for write error",
		})

	MasterBroadcastToFullErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemMaster,
			Name:      "broadcast_to_full",
			Help:      "Counter of master broadcast send to full message channel err",
		})

	MasterLeaderChangeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemMaster,
			Name:      "leader_changes",
			Help:      "Counter of master leader changes.",
		}, []string{"type"})

	FilerRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemFiler,
			Name:      "request_total",
			Help:      "Counter of filer requests.",
		}, []string{"type", "code"})

	FilerHandlerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemFiler,
			Name:      "handler_total",
			Help:      "Counter of filer handlers.",
		}, []string{"type"})

	FilerRequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: subsystemFiler,
			Name:      "request_seconds",
			Help:      "Bucketed histogram of filer request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})

	FilerInFlightRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemFiler,
			Name:      "in_flight_requests",
			Help:      "Current number of in-flight requests being handled by filer.",
		}, []string{"type"})

	FilerInFlightUploadBytesGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemFiler,
			Name:      "in_flight_upload_bytes",
			Help:      "Current number of bytes being uploaded to filer.",
		})

	FilerInFlightUploadCountGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemFiler,
			Name:      "in_flight_upload_count",
			Help:      "Current number of uploads in progress to filer.",
		})

	FilerServerLastSendTsOfSubscribeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemFiler,
			Name:      "last_send_timestamp_of_subscribe",
			Help:      "The last send timestamp of the filer subscription.",
		}, []string{"sourceFiler", "clientName", "path"})

	// Sampled only on first creation, so counts track distinct objects.
	FilerObjectSizeBytesHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: subsystemFiler,
			Name:      "object_size_bytes",
			Help:      "Distribution of object sizes in bytes, sampled when an object is first created.",
			Buckets:   []float64{1024, 102400, 1048576, 104857600, 1073741824},
		})

	FilerStoreCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemFilerStore,
			Name:      "request_total",
			Help:      "Counter of filer store requests.",
		}, []string{"store", "type"})

	FilerStoreHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: subsystemFilerStore,
			Name:      "request_seconds",
			Help:      "Bucketed histogram of filer store request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"store", "type"})

	FilerSyncOffsetGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemFilerSync,
			Name:      "sync_offset",
			Help:      "The offset of the filer synchronization service.",
		}, []string{"sourceFiler", "targetFiler", "clientName", "path"})

	VolumeServerStartTimeSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "start_time_seconds",
			Help:      "Start time of the volume server, as seconds since UNIX epoch.",
		})

	VolumeServerRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "request_total",
			Help:      "Counter of volume server requests.",
		}, []string{"type", "code"})

	VolumeServerHandlerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "handler_total",
			Help:      "Counter of volume server handlers.",
		}, []string{"type"})

	VolumeServerVacuumingCompactCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "vacuuming_compact_count",
			Help:      "Counter of volume vacuuming Compact counter",
		}, []string{"success"})

	VolumeServerVacuumingCommitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "vacuuming_commit_count",
			Help:      "Counter of volume vacuuming commit counter",
		}, []string{"success"})

	VolumeServerVacuumingHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "vacuuming_seconds",
			Help:      "Bucketed histogram of volume server vacuuming processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})

	VolumeServerRequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "request_seconds",
			Help:      "Bucketed histogram of volume server request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})

	VolumeServerInFlightRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "in_flight_requests",
			Help:      "Current number of in-flight requests being handled by volume server.",
		}, []string{"type"})

	VolumeServerVolumeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "volumes",
			Help:      "Number of volumes or shards.",
		}, []string{"collection", "type"})

	VolumeServerReadOnlyVolumeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "read_only_volumes",
			Help:      "Number of read only volumes.",
		}, []string{"collection", "type"})

	VolumeServerMaxVolumeCounter = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "max_volumes",
			Help:      "Maximum number of volumes.",
		})

	VolumeServerDiskSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "total_disk_size",
			Help:      "Actual disk size used by volumes.",
		}, []string{"collection", "type"})

	VolumeServerResourceGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "resource",
			Help:      "Resource usage",
		}, []string{"name", "type"})

	VolumeServerDiskErrorGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "disk_error_status",
			Help:      "Disk error status",
		}, []string{"name", "type"})

	VolumeServerConcurrentDownloadLimit = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "concurrent_download_limit",
			Help:      "Limit total concurrent download size.",
		})

	VolumeServerConcurrentUploadLimit = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "concurrent_upload_limit",
			Help:      "Limit total concurrent upload size.",
		})

	VolumeServerInFlightDownloadSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "in_flight_download_size",
			Help:      "In flight total download size.",
		})

	VolumeServerInFlightUploadSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "in_flight_upload_size",
			Help:      "In flight total upload size.",
		})

	VolumeServerMasterDisconnections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "master_disconnections",
			Help:      "Number of master server disconnections.",
		}, []string{"address"})

	VolumeServerFileReadFailures = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "file_read_failures",
			Help:      "Counter of overall failed file read requests from clients.",
		})

	VolumeServerFileReadInvalidNeedles = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "file_read_invalid_needles",
			Help:      "Counter of failed file read requests due to invalid needle IDs from clients.",
		})

	VolumeServerFileWriteFailures = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "file_write_failures",
			Help:      "Counter of overall failed file write requests from clients.",
		})

	VolumeServerScrubLastTimeSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "scrub_last_time_seconds",
			Help:      "Last scrub execution time, as seconds since UNIX epoch.",
		}, []string{"mode"})

	VolumeServerScrubVolumeFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "scrub_volume_failures",
			Help:      "Counter of overall volumes with issues detected during scrubbing.",
		}, []string{"mode"})

	VolumeServerScrubShardFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "scrub_shard_failures",
			Help:      "Counter of overall EC shards with issues detected during scrubbing.",
		}, []string{"mode"})

	// VolumeServerReplicationCounter counts replication operations by operation type
	// (write, delete) and result (success, failure).
	VolumeServerReplicationCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "replication_operations_total",
			Help:      "Counter of replication operations by type (write, delete) and result (success, failure).",
		}, []string{"operation", "result"})

	// VolumeServerReplicationHistogram records replication operation duration in seconds,
	// partitioned by operation type (write, delete).
	VolumeServerReplicationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "replication_seconds",
			Help:      "Bucketed histogram of replication operation duration in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"operation"})

	// VolumeServerReplicationTargets records the number of replica targets per replication
	// operation, useful for observing fan-out width.
	VolumeServerReplicationTargets = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "replication_targets",
			Help:      "Histogram of replica targets count per replication operation.",
			Buckets:   []float64{1, 2, 3, 4, 5},
		})

	// VolumeServerReplicationFailures counts replication failures by operation type
	// and failure reason (timeout, connection_refused, context_cancelled, server_error).
	VolumeServerReplicationFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemVolumeServer,
			Name:      "replication_failures_total",
			Help:      "Counter of replication failures by operation and reason (timeout, connection_refused, context_cancelled, server_error).",
		}, []string{"operation", "reason"})

	S3RequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "request_total",
			Help:      "Counter of s3 requests.",
		}, []string{"type", "code", "bucket"})

	S3HandlerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "handler_total",
			Help:      "Counter of s3 server handlers.",
		}, []string{"type"})

	S3RequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "request_seconds",
			Help:      "Bucketed histogram of s3 request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type", "bucket"})

	S3TimeToFirstByteHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "time_to_first_byte_millisecond",
			Help:      "Bucketed histogram of s3 time to first byte request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 27),
		}, []string{"type", "bucket"})
	S3InFlightRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "in_flight_requests",
			Help:      "Current number of in-flight requests being handled by s3.",
		}, []string{"type"})

	S3InFlightUploadBytesGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "in_flight_upload_bytes",
			Help:      "Current number of bytes being uploaded to S3.",
		})

	S3InFlightUploadCountGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "in_flight_upload_count",
			Help:      "Current number of uploads in progress to S3.",
		})

	S3BucketTrafficReceivedBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "bucket_traffic_received_bytes_total",
			Help:      "Total number of bytes received by an S3 bucket from clients.",
		}, []string{"bucket"})

	S3BucketTrafficSentBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "bucket_traffic_sent_bytes_total",
			Help:      "Total number of bytes sent from an S3 bucket to clients.",
		}, []string{"bucket"})

	S3DeletedObjectsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "deleted_objects",
			Help:      "Number of objects deleted in each bucket.",
		}, []string{"bucket"})

	S3UploadedObjectsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "uploaded_objects",
			Help:      "Number of objects uploaded in each bucket.",
		}, []string{"bucket"})

	S3BucketSizeBytesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "bucket_size_bytes",
			Help:      "Current size of each S3 bucket in bytes (logical size, deduplicated across replicas).",
		}, []string{"bucket"})

	S3BucketPhysicalSizeBytesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "bucket_physical_size_bytes",
			Help:      "Current physical size of each S3 bucket in bytes (including all replicas).",
		}, []string{"bucket"})

	S3BucketObjectCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "bucket_object_count",
			Help:      "Current number of objects in each S3 bucket (logical count, deduplicated across replicas).",
		}, []string{"bucket"})

	S3BucketQuotaBytesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "bucket_quota_bytes",
			Help:      "Configured quota of each S3 bucket in bytes. Only present for buckets with an enabled quota.",
		}, []string{"bucket"})

	S3BucketReadOnlyGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3,
			Name:      "bucket_read_only",
			Help:      "Whether each S3 bucket is read-only (1) or writable (0), e.g. after exceeding its quota.",
		}, []string{"bucket"})

	UploadErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "upload_error_total",
			Help:      "Counter of upload errors by HTTP status code. Code 0 means transport error (no response received).",
		}, []string{"code"})

	S3LifecycleDispatchCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3Lifecycle,
			Name:      "dispatch_total",
			Help:      "Counter of LifecycleDelete RPC outcomes by bucket, action kind, and outcome.",
		}, []string{"bucket", "kind", "outcome"})

	S3LifecycleScheduleDepthGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3Lifecycle,
			Name:      "schedule_depth",
			Help:      "Number of pending matches in the dispatcher schedule per shard.",
		}, []string{"shard"})

	S3LifecycleCursorMinTsNs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3Lifecycle,
			Name:      "cursor_min_ts_ns",
			Help:      "Per-shard min cursor timestamp in nanoseconds since epoch (lag = now - min).",
		}, []string{"shard"})

	S3LifecycleEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3Lifecycle,
			Name:      "events_total",
			Help:      "Counter of meta-log events the reader emitted to the router, partitioned by shard.",
		}, []string{"shard"})

	S3LifecycleBootstrapDispatchCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3Lifecycle,
			Name:      "bootstrap_dispatch_total",
			Help:      "Counter of bootstrap-walk Delete dispatches by bucket and action kind.",
		}, []string{"bucket", "kind"})

	// S3LifecycleMetadataOnlyCounter counts successful LifecycleDelete
	// dispatches that took the metadata-only path — entry was removed
	// without per-chunk DeleteFile RPCs because the entry's Attributes
	// .TtlSec > 0 and the volume's natural TTL will reclaim chunks. Per-
	// rule cardinality (rule_hash hex-encoded) lets operators identify
	// which specific rule is exercising the optimization; in clusters
	// with many rules this can be reduced via Prometheus relabeling.
	S3LifecycleMetadataOnlyCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3Lifecycle,
			Name:      "metadata_only_total",
			Help:      "Counter of LifecycleDelete completions that skipped per-chunk delete (volume TTL reclaim).",
		}, []string{"bucket", "rule_hash"})

	// S3LifecycleDispatchLimiterWaitSeconds is the cluster-wide rate
	// limiter's per-dispatch wait time on the daily-replay path. The
	// limiter blocks just before each LifecycleDelete RPC; near-zero
	// observations mean the cluster cap isn't binding, a long-tail at
	// the configured 1/rate ceiling means the cluster cap is the
	// active throttle. Operators tune cluster_deletes_per_second by
	// reading p95/p99 on this histogram.
	S3LifecycleDispatchLimiterWaitSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3Lifecycle,
			Name:      "dispatch_limiter_wait_seconds",
			Help:      "Time spent waiting on the cluster rate limiter before issuing a LifecycleDelete RPC. Non-zero values indicate the cluster cap is binding.",
			Buckets:   []float64{0.0001, 0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		})

	S3LifecycleDailyRunShardDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3Lifecycle,
			Name:      "daily_run_shard_duration_seconds",
			Help:      "Wall-clock seconds spent in one shard's daily_replay pass. p95 climbing toward MaxRuntime means the shard is brushing its budget.",
			Buckets:   []float64{0.1, 0.5, 1, 5, 15, 60, 300, 900, 1800, 3600},
		}, []string{"shard"})

	S3LifecycleDailyRunEventsScanned = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3Lifecycle,
			Name:      "daily_run_events_scanned_total",
			Help:      "Counter of meta-log events drainShardEvents processed on the daily_replay path, partitioned by shard.",
		}, []string{"shard"})

	// S3LifecycleDailyRunLastWalkedNs is the per-shard wall-clock
	// timestamp (UnixNano) of the most recent successful steady-state /
	// empty-replay walker fire. Set by dailyrun.runShard after each
	// cursor save. Zero means the shard hasn't completed a walk yet
	// (either cold start, or the walker never fired because the bucket
	// has only replay-eligible rules and the throttle hasn't elapsed).
	// Operators read (now - last_walked_ns) to confirm the walker
	// cadence matches WalkerInterval; a stuck value means the
	// scheduler isn't invoking the worker, the throttle is too long,
	// or the walker is failing.
	S3LifecycleDailyRunLastWalkedNs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemS3Lifecycle,
			Name:      "daily_run_last_walked_ns",
			Help:      "Per-shard timestamp (UnixNano) of the most recent successful walker fire. 0 means the shard hasn't completed a walk yet.",
		}, []string{"shard"})

	AdminMaintenanceTasksByStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemAdmin,
			Name:      "maintenance_tasks_by_status",
			Help:      "Current number of maintenance tasks by status (pending, assigned, in_progress, completed, failed, cancelled).",
		}, []string{"status"})

	AdminMaintenanceTasksByType = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemAdmin,
			Name:      "maintenance_tasks_by_type",
			Help:      "Current number of maintenance tasks by type.",
		}, []string{"type"})

	AdminMaintenanceTasksCompletedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemAdmin,
			Name:      "maintenance_tasks_completed_total",
			Help:      "Counter of maintenance tasks that reached a terminal state, by type and outcome (completed, failed).",
		}, []string{"type", "outcome"})

	AdminMaintenanceTaskDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: subsystemAdmin,
			Name:      "maintenance_task_duration_seconds",
			Help:      "Execution time of maintenance tasks that reached a terminal state, by type.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
		}, []string{"type"})

	AdminMaintenanceLastScanTimestampSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemAdmin,
			Name:      "maintenance_last_scan_timestamp_seconds",
			Help:      "Unix timestamp of the most recent maintenance scan. 0 means no scan has run yet.",
		})

	AdminMaintenanceNextScanTimestampSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemAdmin,
			Name:      "maintenance_next_scan_timestamp_seconds",
			Help:      "Unix timestamp of the next expected maintenance scan.",
		})

	AdminWorkersConnected = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemAdmin,
			Name:      "workers_connected",
			Help:      "Current number of maintenance workers known to the admin server.",
		})

	AdminWorkerSlots = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemAdmin,
			Name:      "worker_slots",
			Help:      "Maintenance worker task slots aggregated across workers, by state (used, max).",
		}, []string{"state"})

	AdminWorkerEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemAdmin,
			Name:      "worker_events_total",
			Help:      "Counter of maintenance worker lifecycle events by type (registered, unregistered, stale_removed).",
		}, []string{"event"})
)

func init() {
	Gather.MustRegister(BuildInfo)

	Gather.MustRegister(MasterStartTimeSeconds)
	Gather.MustRegister(MasterClientConnectCounter)
	Gather.MustRegister(MasterRaftIsleader)
	Gather.MustRegister(MasterAdminLock)
	Gather.MustRegister(MasterReceivedHeartbeatCounter)
	Gather.MustRegister(MasterLeaderChangeCounter)
	Gather.MustRegister(MasterReplicaPlacementMismatch)
	Gather.MustRegister(MasterVolumeLayoutWritable)
	Gather.MustRegister(MasterVolumeLayoutCrowded)
	Gather.MustRegister(MasterPickForWriteErrorCounter)
	Gather.MustRegister(MasterBroadcastToFullErrorCounter)

	Gather.MustRegister(FilerRequestCounter)
	Gather.MustRegister(FilerHandlerCounter)
	Gather.MustRegister(FilerRequestHistogram)
	Gather.MustRegister(FilerInFlightRequestsGauge)
	Gather.MustRegister(FilerInFlightUploadBytesGauge)
	Gather.MustRegister(FilerInFlightUploadCountGauge)
	Gather.MustRegister(FilerStoreCounter)
	Gather.MustRegister(FilerStoreHistogram)
	Gather.MustRegister(FilerSyncOffsetGauge)
	Gather.MustRegister(FilerServerLastSendTsOfSubscribeGauge)
	Gather.MustRegister(FilerObjectSizeBytesHistogram)
	Gather.MustRegister(collectors.NewGoCollector())
	Gather.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	Gather.MustRegister(VolumeServerStartTimeSeconds)
	Gather.MustRegister(VolumeServerRequestCounter)
	Gather.MustRegister(VolumeServerHandlerCounter)
	Gather.MustRegister(VolumeServerRequestHistogram)
	Gather.MustRegister(VolumeServerInFlightRequestsGauge)
	Gather.MustRegister(VolumeServerVacuumingCompactCounter)
	Gather.MustRegister(VolumeServerVacuumingCommitCounter)
	Gather.MustRegister(VolumeServerVacuumingHistogram)
	Gather.MustRegister(VolumeServerVolumeGauge)
	Gather.MustRegister(VolumeServerMaxVolumeCounter)
	Gather.MustRegister(VolumeServerReadOnlyVolumeGauge)
	Gather.MustRegister(VolumeServerDiskSizeGauge)
	Gather.MustRegister(VolumeServerResourceGauge)
	Gather.MustRegister(VolumeServerDiskErrorGauge)
	Gather.MustRegister(VolumeServerConcurrentDownloadLimit)
	Gather.MustRegister(VolumeServerConcurrentUploadLimit)
	Gather.MustRegister(VolumeServerInFlightDownloadSize)
	Gather.MustRegister(VolumeServerInFlightUploadSize)
	Gather.MustRegister(VolumeServerMasterDisconnections)
	Gather.MustRegister(VolumeServerFileReadFailures)
	Gather.MustRegister(VolumeServerFileReadInvalidNeedles)
	Gather.MustRegister(VolumeServerFileWriteFailures)
	Gather.MustRegister(VolumeServerScrubLastTimeSeconds)
	Gather.MustRegister(VolumeServerScrubVolumeFailures)
	Gather.MustRegister(VolumeServerScrubShardFailures)
	Gather.MustRegister(VolumeServerReplicationCounter)
	Gather.MustRegister(VolumeServerReplicationHistogram)
	Gather.MustRegister(VolumeServerReplicationTargets)
	Gather.MustRegister(VolumeServerReplicationFailures)
	Gather.MustRegister(MasterUnderReplicatedVolumes)
	Gather.MustRegister(MasterVolumeCreationCounter)

	Gather.MustRegister(S3RequestCounter)
	Gather.MustRegister(S3HandlerCounter)
	Gather.MustRegister(S3RequestHistogram)
	Gather.MustRegister(S3InFlightRequestsGauge)
	Gather.MustRegister(S3InFlightUploadBytesGauge)
	Gather.MustRegister(S3InFlightUploadCountGauge)
	Gather.MustRegister(S3TimeToFirstByteHistogram)
	Gather.MustRegister(S3BucketTrafficReceivedBytesCounter)
	Gather.MustRegister(S3BucketTrafficSentBytesCounter)
	Gather.MustRegister(S3DeletedObjectsCounter)
	Gather.MustRegister(S3UploadedObjectsCounter)
	Gather.MustRegister(S3BucketSizeBytesGauge)
	Gather.MustRegister(S3BucketPhysicalSizeBytesGauge)
	Gather.MustRegister(S3BucketObjectCountGauge)
	Gather.MustRegister(S3BucketQuotaBytesGauge)
	Gather.MustRegister(S3BucketReadOnlyGauge)

	Gather.MustRegister(S3LifecycleDispatchCounter)
	Gather.MustRegister(S3LifecycleScheduleDepthGauge)
	Gather.MustRegister(S3LifecycleCursorMinTsNs)
	Gather.MustRegister(S3LifecycleEventCounter)
	Gather.MustRegister(S3LifecycleBootstrapDispatchCounter)
	Gather.MustRegister(S3LifecycleMetadataOnlyCounter)
	Gather.MustRegister(S3LifecycleDispatchLimiterWaitSeconds)
	Gather.MustRegister(S3LifecycleDailyRunShardDurationSeconds)
	Gather.MustRegister(S3LifecycleDailyRunEventsScanned)
	Gather.MustRegister(S3LifecycleDailyRunLastWalkedNs)

	Gather.MustRegister(UploadErrorCounter)

	Gather.MustRegister(AdminMaintenanceTasksByStatus)
	Gather.MustRegister(AdminMaintenanceTasksByType)
	Gather.MustRegister(AdminMaintenanceTasksCompletedTotal)
	Gather.MustRegister(AdminMaintenanceTaskDurationSeconds)
	Gather.MustRegister(AdminMaintenanceLastScanTimestampSeconds)
	Gather.MustRegister(AdminMaintenanceNextScanTimestampSeconds)
	Gather.MustRegister(AdminWorkersConnected)
	Gather.MustRegister(AdminWorkerSlots)
	Gather.MustRegister(AdminWorkerEventsTotal)

	go bucketMetricTTLControl()
}

func LoopPushingMetric(name, instance, addr string, intervalSeconds int) {
	if addr == "" || intervalSeconds == 0 {
		return
	}

	glog.V(0).Infof("%s server sends metrics to %s every %d seconds", name, addr, intervalSeconds)

	pusher := push.New(addr, name).Gatherer(Gather).Grouping("instance", instance)

	for {
		err := pusher.Push()
		if err != nil && !strings.HasPrefix(err.Error(), "unexpected status code 200") {
			glog.V(0).Infof("could not push metrics to prometheus push gateway %s: %v", addr, err)
		}
		if intervalSeconds <= 0 {
			intervalSeconds = 15
		}
		time.Sleep(time.Duration(intervalSeconds) * time.Second)
	}
}

func JoinHostPort(host string, port int) string {
	portStr := strconv.Itoa(port)
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		return host + ":" + portStr
	}
	return net.JoinHostPort(host, portStr)
}

func StartMetricsServer(ip string, port int) {
	if port == 0 {
		return
	}
	http.Handle("/metrics", promhttp.HandlerFor(Gather, promhttp.HandlerOpts{}))
	glog.Fatal(http.ListenAndServe(JoinHostPort(ip, port), nil))
}

func SourceName(port uint32) string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return net.JoinHostPort(hostname, strconv.Itoa(int(port)))
}

func RecordBucketActiveTime(bucket string) {
	bucketLastActiveLock.Lock()
	bucketLastActiveTsNs[bucket] = time.Now().UnixNano()
	bucketLastActiveLock.Unlock()
}

func DeleteBucketMetrics(bucket string) {
	bucketLastActiveLock.Lock()
	delete(bucketLastActiveTsNs, bucket)
	bucketLastActiveLock.Unlock()

	labels := prometheus.Labels{"bucket": bucket}
	c := S3RequestCounter.DeletePartialMatch(labels)
	c += S3RequestHistogram.DeletePartialMatch(labels)
	c += S3TimeToFirstByteHistogram.DeletePartialMatch(labels)
	c += S3BucketTrafficReceivedBytesCounter.DeletePartialMatch(labels)
	c += S3BucketTrafficSentBytesCounter.DeletePartialMatch(labels)
	c += S3DeletedObjectsCounter.DeletePartialMatch(labels)
	c += S3UploadedObjectsCounter.DeletePartialMatch(labels)
	c += S3BucketSizeBytesGauge.DeletePartialMatch(labels)
	c += S3BucketPhysicalSizeBytesGauge.DeletePartialMatch(labels)
	c += S3BucketObjectCountGauge.DeletePartialMatch(labels)
	c += S3BucketQuotaBytesGauge.DeletePartialMatch(labels)
	c += S3BucketReadOnlyGauge.DeletePartialMatch(labels)
	c += S3LifecycleDispatchCounter.DeletePartialMatch(labels)
	c += S3LifecycleBootstrapDispatchCounter.DeletePartialMatch(labels)
	c += S3LifecycleMetadataOnlyCounter.DeletePartialMatch(labels)

	glog.V(0).Infof("delete bucket metrics, %s: %d", bucket, c)
}

func DeleteCollectionMetrics(collection string) {
	labels := prometheus.Labels{"collection": collection}
	c := MasterReplicaPlacementMismatch.DeletePartialMatch(labels)
	c += MasterVolumeLayoutWritable.DeletePartialMatch(labels)
	c += MasterVolumeLayoutCrowded.DeletePartialMatch(labels)
	c += MasterUnderReplicatedVolumes.DeletePartialMatch(labels)
	c += VolumeServerDiskSizeGauge.DeletePartialMatch(labels)
	c += VolumeServerVolumeGauge.DeletePartialMatch(labels)
	c += VolumeServerReadOnlyVolumeGauge.DeletePartialMatch(labels)

	glog.V(0).Infof("delete collection metrics, %s: %d", collection, c)
}

func bucketMetricTTLControl() {
	ttlNs := bucketAtiveTTL.Nanoseconds()
	for {
		now := time.Now().UnixNano()

		// Collect expired buckets under the lock, then release before
		// doing the expensive Prometheus DeletePartialMatch calls.
		// This prevents blocking RecordBucketActiveTime during cleanup.
		bucketLastActiveLock.Lock()
		var expiredBuckets []string
		for bucket, ts := range bucketLastActiveTsNs {
			if (now - ts) > ttlNs {
				expiredBuckets = append(expiredBuckets, bucket)
				delete(bucketLastActiveTsNs, bucket)
			}
		}
		bucketLastActiveLock.Unlock()

		for _, bucket := range expiredBuckets {
			labels := prometheus.Labels{"bucket": bucket}
			// Only delete gauges and histograms, which represent current state.
			// Counters (traffic, requests, objects) must persist for the process
			// lifetime so that Prometheus rate()/increase() queries work correctly.
			c := S3RequestHistogram.DeletePartialMatch(labels)
			c += S3TimeToFirstByteHistogram.DeletePartialMatch(labels)
			c += S3BucketSizeBytesGauge.DeletePartialMatch(labels)
			c += S3BucketPhysicalSizeBytesGauge.DeletePartialMatch(labels)
			c += S3BucketObjectCountGauge.DeletePartialMatch(labels)
			c += S3BucketQuotaBytesGauge.DeletePartialMatch(labels)
			c += S3BucketReadOnlyGauge.DeletePartialMatch(labels)
			glog.V(0).Infof("delete inactive bucket metrics, %s: %d", bucket, c)
		}

		time.Sleep(bucketAtiveTTL)
	}

}

// UpdateBucketSizeMetrics updates the bucket size gauges
// logicalSize is the deduplicated size (accounting for replication)
// physicalSize is the raw size including all replicas
// objectCount is the number of objects in the bucket (deduplicated)
func UpdateBucketSizeMetrics(bucket string, logicalSize, physicalSize float64, objectCount float64) {
	S3BucketSizeBytesGauge.WithLabelValues(bucket).Set(logicalSize)
	S3BucketPhysicalSizeBytesGauge.WithLabelValues(bucket).Set(physicalSize)
	S3BucketObjectCountGauge.WithLabelValues(bucket).Set(objectCount)
	RecordBucketActiveTime(bucket)
}

// UpdateBucketQuotaMetrics updates the per-bucket quota gauges. A non-positive
// quota removes the quota series so utilization queries like
// bucket_size_bytes / bucket_quota_bytes only see enforced quotas.
func UpdateBucketQuotaMetrics(bucket string, quota float64, readOnly bool) {
	if quota > 0 {
		S3BucketQuotaBytesGauge.WithLabelValues(bucket).Set(quota)
	} else {
		S3BucketQuotaBytesGauge.DeleteLabelValues(bucket)
	}
	readOnlyValue := float64(0)
	if readOnly {
		readOnlyValue = 1
	}
	S3BucketReadOnlyGauge.WithLabelValues(bucket).Set(readOnlyValue)
}
