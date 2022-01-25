package stats

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	Gather = prometheus.NewRegistry()

	MasterClientConnectCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "SeaweedFS",
			Subsystem: "wdclient",
			Name:      "connect_updates",
			Help:      "Counter of master client leader updates.",
		}, []string{"type"})

	MasterRaftIsleader = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "SeaweedFS",
			Subsystem: "master",
			Name:      "is_leader",
			Help:      "is leader",
		})

	MasterReceivedHeartbeatCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "SeaweedFS",
			Subsystem: "master",
			Name:      "received_heartbeats",
			Help:      "Counter of master received heartbeat.",
		}, []string{"type"})

	MasterLeaderChangeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "SeaweedFS",
			Subsystem: "master",
			Name:      "leader_changes",
			Help:      "Counter of master leader changes.",
		}, []string{"type"})

	FilerRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "SeaweedFS",
			Subsystem: "filer",
			Name:      "request_total",
			Help:      "Counter of filer requests.",
		}, []string{"type"})

	FilerRequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "SeaweedFS",
			Subsystem: "filer",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of filer request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})

	FilerStoreCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "SeaweedFS",
			Subsystem: "filerStore",
			Name:      "request_total",
			Help:      "Counter of filer store requests.",
		}, []string{"store", "type"})

	FilerStoreHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "SeaweedFS",
			Subsystem: "filerStore",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of filer store request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"store", "type"})

	VolumeServerRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "request_total",
			Help:      "Counter of volume server requests.",
		}, []string{"type"})

	VolumeServerRequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of volume server request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})

	VolumeServerVolumeCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "volumes",
			Help:      "Number of volumes or shards.",
		}, []string{"collection", "type"})

	VolumeServerReadOnlyVolumeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "read_only_volumes",
			Help:      "Number of read only volumes.",
		}, []string{"collection", "type"})

	VolumeServerMaxVolumeCounter = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "max_volumes",
			Help:      "Maximum number of volumes.",
		})

	VolumeServerDiskSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "total_disk_size",
			Help:      "Actual disk size used by volumes.",
		}, []string{"collection", "type"})

	VolumeServerResourceGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "resource",
			Help:      "Resource usage",
		}, []string{"name", "type"})

	S3RequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "SeaweedFS",
			Subsystem: "s3",
			Name:      "request_total",
			Help:      "Counter of s3 requests.",
		}, []string{"type", "code"})
	S3RequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "SeaweedFS",
			Subsystem: "s3",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of s3 request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})
)

func init() {
	Gather.MustRegister(MasterClientConnectCounter)
	Gather.MustRegister(MasterRaftIsleader)
	Gather.MustRegister(MasterReceivedHeartbeatCounter)
	Gather.MustRegister(MasterLeaderChangeCounter)

	Gather.MustRegister(FilerRequestCounter)
	Gather.MustRegister(FilerRequestHistogram)
	Gather.MustRegister(FilerStoreCounter)
	Gather.MustRegister(FilerStoreHistogram)
	Gather.MustRegister(collectors.NewGoCollector())
	Gather.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	Gather.MustRegister(VolumeServerRequestCounter)
	Gather.MustRegister(VolumeServerRequestHistogram)
	Gather.MustRegister(VolumeServerVolumeCounter)
	Gather.MustRegister(VolumeServerMaxVolumeCounter)
	Gather.MustRegister(VolumeServerReadOnlyVolumeGauge)
	Gather.MustRegister(VolumeServerDiskSizeGauge)
	Gather.MustRegister(VolumeServerResourceGauge)

	Gather.MustRegister(S3RequestCounter)
	Gather.MustRegister(S3RequestHistogram)
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

func StartMetricsServer(port int) {
	if port == 0 {
		return
	}
	http.Handle("/metrics", promhttp.HandlerFor(Gather, promhttp.HandlerOpts{}))
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

func SourceName(port uint32) string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return net.JoinHostPort(hostname, strconv.Itoa(int(port)))
}
