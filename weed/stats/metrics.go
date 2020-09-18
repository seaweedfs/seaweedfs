package stats

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

var (
	FilerGather        = prometheus.NewRegistry()
	VolumeServerGather = prometheus.NewRegistry()
	S3Gather           = prometheus.NewRegistry()

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

	S3RequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "SeaweedFS",
			Subsystem: "s3",
			Name:      "request_total",
			Help:      "Counter of s3 requests.",
		}, []string{"type"})
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

	FilerGather.MustRegister(FilerRequestCounter)
	FilerGather.MustRegister(FilerRequestHistogram)
	FilerGather.MustRegister(FilerStoreCounter)
	FilerGather.MustRegister(FilerStoreHistogram)
	FilerGather.MustRegister(prometheus.NewGoCollector())

	VolumeServerGather.MustRegister(VolumeServerRequestCounter)
	VolumeServerGather.MustRegister(VolumeServerRequestHistogram)
	VolumeServerGather.MustRegister(VolumeServerVolumeCounter)
	VolumeServerGather.MustRegister(VolumeServerMaxVolumeCounter)
	VolumeServerGather.MustRegister(VolumeServerDiskSizeGauge)

	S3Gather.MustRegister(S3RequestCounter)
	S3Gather.MustRegister(S3RequestHistogram)
}

func LoopPushingMetric(name, instance string, gatherer *prometheus.Registry, addr string, intervalSeconds int) {

	if addr == "" || intervalSeconds == 0 {
		return
	}

	pusher := push.New(addr, name).Gatherer(gatherer).Grouping("instance", instance)

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

func SourceName(port uint32) string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return fmt.Sprintf("%s:%d", hostname, port)
}
