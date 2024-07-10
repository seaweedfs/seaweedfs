package stats

import (
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	sentryhttp "github.com/getsentry/sentry-go/http"
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
)

var readOnlyVolumeTypes = [4]string{IsReadOnly, NoWriteOrDelete, NoWriteCanDelete, IsDiskSpaceLow}

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

	MasterVolumeLayout = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "volume_layout_total",
			Help:      "Number of volumes in volume layouts",
		}, []string{"collection", "replica", "type"})

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
)

func init() {
	Gather.MustRegister(MasterClientConnectCounter)
	Gather.MustRegister(MasterRaftIsleader)
	Gather.MustRegister(MasterAdminLock)
	Gather.MustRegister(MasterReceivedHeartbeatCounter)
	Gather.MustRegister(MasterLeaderChangeCounter)
	Gather.MustRegister(MasterReplicaPlacementMismatch)
	Gather.MustRegister(MasterVolumeLayout)

	Gather.MustRegister(FilerRequestCounter)
	Gather.MustRegister(FilerHandlerCounter)
	Gather.MustRegister(FilerRequestHistogram)
	Gather.MustRegister(FilerStoreCounter)
	Gather.MustRegister(FilerStoreHistogram)
	Gather.MustRegister(FilerSyncOffsetGauge)
	Gather.MustRegister(FilerServerLastSendTsOfSubscribeGauge)
	Gather.MustRegister(collectors.NewGoCollector())
	Gather.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	Gather.MustRegister(VolumeServerRequestCounter)
	Gather.MustRegister(VolumeServerHandlerCounter)
	Gather.MustRegister(VolumeServerRequestHistogram)
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
	Gather.MustRegister(S3TimeToFirstByteHistogram)
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
	handler := sentryhttp.New(sentryhttp.Options{}).Handle(http.DefaultServeMux)
	log.Fatal(http.ListenAndServe(JoinHostPort(ip, port), handler))
}

func SourceName(port uint32) string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return net.JoinHostPort(hostname, strconv.Itoa(int(port)))
}

// todo - can be changed to DeletePartialMatch when https://github.com/prometheus/client_golang/pull/1013 gets released
func DeleteCollectionMetrics(collection string) {
	VolumeServerDiskSizeGauge.DeleteLabelValues(collection, "normal")
	for _, volume_type := range readOnlyVolumeTypes {
		VolumeServerReadOnlyVolumeGauge.DeleteLabelValues(collection, volume_type)
	}
	VolumeServerVolumeGauge.DeleteLabelValues(collection, "volume")
}
