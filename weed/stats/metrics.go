package stats

import (
	"log"
	"net"
	"net/http"
	"os"
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

// Readonly volume types
const (
	Namespace        = "SeaweedFS"
	IsReadOnly       = "IsReadOnly"
	NoWriteOrDelete  = "noWriteOrDelete"
	NoWriteCanDelete = "noWriteCanDelete"
	IsDiskSpaceLow   = "isDiskSpaceLow"
	bucketAtiveTTL   = 10 * time.Minute
)

var readOnlyVolumeTypes = [4]string{IsReadOnly, NoWriteOrDelete, NoWriteCanDelete, IsDiskSpaceLow}

var bucketLastActiveTsNs map[string]int64 = map[string]int64{}
var bucketLastActiveLock sync.Mutex

var (
	Gather = prometheus.NewRegistry()

	MasterClientConnectCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "wdclient",
			Name:      "connect_updates",
			Help:      "Counter of master client leader updates.",
		}, []string{"type"})

	MasterRaftIsleader = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "is_leader",
			Help:      "is leader",
		})

	MasterAdminLock = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "admin_lock",
			Help:      "admin lock",
		}, []string{"client"})

	MasterReceivedHeartbeatCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "received_heartbeats",
			Help:      "Counter of master received heartbeat.",
		}, []string{"type"})

	MasterReplicaPlacementMismatch = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "replica_placement_mismatch",
			Help:      "replica placement mismatch",
		}, []string{"collection", "id"})

	MasterVolumeLayoutWritable = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "volume_layout_writable",
			Help:      "Number of writable volumes in volume layouts",
		}, []string{"collection", "disk", "rp", "ttl"})

	MasterVolumeLayoutCrowded = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "volume_layout_crowded",
			Help:      "Number of crowded volumes in volume layouts",
		}, []string{"collection", "disk", "rp", "ttl"})

	MasterPickForWriteErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "pick_for_write_error",
			Help:      "Counter of master pick for write error",
		})

	MasterBroadcastToFullErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "broadcast_to_full",
			Help:      "Counter of master broadcast send to full message channel err",
		})

	MasterLeaderChangeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "leader_changes",
			Help:      "Counter of master leader changes.",
		}, []string{"type"})

	FilerRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "filer",
			Name:      "request_total",
			Help:      "Counter of filer requests.",
		}, []string{"type", "code"})

	FilerHandlerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "filer",
			Name:      "handler_total",
			Help:      "Counter of filer handlers.",
		}, []string{"type"})

	FilerRequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "filer",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of filer request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})

	FilerInFlightRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "filer",
			Name:      "in_flight_requests",
			Help:      "Current number of in-flight requests being handled by filer.",
		}, []string{"type"})

	FilerServerLastSendTsOfSubscribeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "filer",
			Name:      "last_send_timestamp_of_subscribe",
			Help:      "The last send timestamp of the filer subscription.",
		}, []string{"sourceFiler", "clientName", "path"})

	FilerStoreCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "filerStore",
			Name:      "request_total",
			Help:      "Counter of filer store requests.",
		}, []string{"store", "type"})

	FilerStoreHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "filerStore",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of filer store request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"store", "type"})

	FilerSyncOffsetGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "filerSync",
			Name:      "sync_offset",
			Help:      "The offset of the filer synchronization service.",
		}, []string{"sourceFiler", "targetFiler", "clientName", "path"})

	VolumeServerRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "request_total",
			Help:      "Counter of volume server requests.",
		}, []string{"type", "code"})

	VolumeServerHandlerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "handler_total",
			Help:      "Counter of volume server handlers.",
		}, []string{"type"})

	VolumeServerVacuumingCompactCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "vacuuming_compact_count",
			Help:      "Counter of volume vacuuming Compact counter",
		}, []string{"success"})

	VolumeServerVacuumingCommitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "vacuuming_commit_count",
			Help:      "Counter of volume vacuuming commit counter",
		}, []string{"success"})

	VolumeServerVacuumingHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "vacuuming_seconds",
			Help:      "Bucketed histogram of volume server vacuuming processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})

	VolumeServerRequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of volume server request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})

	VolumeServerInFlightRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "in_flight_requests",
			Help:      "Current number of in-flight requests being handled by volume server.",
		}, []string{"type"})

	VolumeServerVolumeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "volumes",
			Help:      "Number of volumes or shards.",
		}, []string{"collection", "type"})

	VolumeServerReadOnlyVolumeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "read_only_volumes",
			Help:      "Number of read only volumes.",
		}, []string{"collection", "type"})

	VolumeServerMaxVolumeCounter = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "max_volumes",
			Help:      "Maximum number of volumes.",
		})

	VolumeServerDiskSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "total_disk_size",
			Help:      "Actual disk size used by volumes.",
		}, []string{"collection", "type"})

	VolumeServerResourceGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "resource",
			Help:      "Resource usage",
		}, []string{"name", "type"})

	S3RequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "request_total",
			Help:      "Counter of s3 requests.",
		}, []string{"type", "code", "bucket"})

	S3HandlerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "handler_total",
			Help:      "Counter of s3 server handlers.",
		}, []string{"type"})

	S3RequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of s3 request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type", "bucket"})

	S3TimeToFirstByteHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "time_to_first_byte_millisecond",
			Help:      "Bucketed histogram of s3 time to first byte request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 27),
		}, []string{"type", "bucket"})
	S3InFlightRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "in_flight_requests",
			Help:      "Current number of in-flight requests being handled by s3.",
		}, []string{"type"})

	S3BucketTrafficReceivedBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "bucket_traffic_received_bytes_total",
			Help:      "Total number of bytes received by an S3 bucket from clients.",
		}, []string{"bucket"})

	S3BucketTrafficSentBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "bucket_traffic_sent_bytes_total",
			Help:      "Total number of bytes sent from an S3 bucket to clients.",
		}, []string{"bucket"})

	S3DeletedObjectsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "deleted_objects",
			Help:      "Number of objects deleted in each bucket.",
		}, []string{"bucket"})

	S3UploadedObjectsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "uploaded_objects",
			Help:      "Number of objects uploaded in each bucket.",
		}, []string{"bucket"})
)

func init() {
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
	Gather.MustRegister(FilerStoreCounter)
	Gather.MustRegister(FilerStoreHistogram)
	Gather.MustRegister(FilerSyncOffsetGauge)
	Gather.MustRegister(FilerServerLastSendTsOfSubscribeGauge)
	Gather.MustRegister(collectors.NewGoCollector())
	Gather.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

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

	Gather.MustRegister(S3RequestCounter)
	Gather.MustRegister(S3HandlerCounter)
	Gather.MustRegister(S3RequestHistogram)
	Gather.MustRegister(S3InFlightRequestsGauge)
	Gather.MustRegister(S3TimeToFirstByteHistogram)
	Gather.MustRegister(S3BucketTrafficReceivedBytesCounter)
	Gather.MustRegister(S3BucketTrafficSentBytesCounter)
	Gather.MustRegister(S3DeletedObjectsCounter)
	Gather.MustRegister(S3UploadedObjectsCounter)

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
	log.Fatal(http.ListenAndServe(JoinHostPort(ip, port), nil))
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

func DeleteCollectionMetrics(collection string) {
	labels := prometheus.Labels{"collection": collection}
	c := MasterReplicaPlacementMismatch.DeletePartialMatch(labels)
	c += MasterVolumeLayoutWritable.DeletePartialMatch(labels)
	c += MasterVolumeLayoutCrowded.DeletePartialMatch(labels)
	c += VolumeServerDiskSizeGauge.DeletePartialMatch(labels)
	c += VolumeServerVolumeGauge.DeletePartialMatch(labels)
	c += VolumeServerReadOnlyVolumeGauge.DeletePartialMatch(labels)

	glog.V(0).Infof("delete collection metrics, %s: %d", collection, c)
}

func bucketMetricTTLControl() {
	ttlNs := bucketAtiveTTL.Nanoseconds()
	for {
		now := time.Now().UnixNano()

		bucketLastActiveLock.Lock()
		for bucket, ts := range bucketLastActiveTsNs {
			if (now - ts) > ttlNs {
				delete(bucketLastActiveTsNs, bucket)

				labels := prometheus.Labels{"bucket": bucket}
				c := S3RequestCounter.DeletePartialMatch(labels)
				c += S3RequestHistogram.DeletePartialMatch(labels)
				c += S3TimeToFirstByteHistogram.DeletePartialMatch(labels)
				c += S3BucketTrafficReceivedBytesCounter.DeletePartialMatch(labels)
				c += S3BucketTrafficSentBytesCounter.DeletePartialMatch(labels)
				c += S3DeletedObjectsCounter.DeletePartialMatch(labels)
				c += S3UploadedObjectsCounter.DeletePartialMatch(labels)
				glog.V(0).Infof("delete inactive bucket metrics, %s: %d", bucket, c)
			}
		}

		bucketLastActiveLock.Unlock()
		time.Sleep(bucketAtiveTTL)
	}

}
