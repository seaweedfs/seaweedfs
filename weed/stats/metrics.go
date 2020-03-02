package stats

import (
	"fmt"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	FilerGather        = prometheus.NewRegistry()
	VolumeServerGather = prometheus.NewRegistry()

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

}

func LoopPushingMetric(name, instance string, gatherer *prometheus.Registry, fnGetMetricsDest func() (addr string, intervalSeconds int)) {

	if fnGetMetricsDest == nil {
		return
	}

	addr, intervalSeconds := fnGetMetricsDest()
	pusher := push.New(addr, name).Gatherer(gatherer).Grouping("instance", instance)
	currentAddr := addr

	for {
		if currentAddr != "" {
			err := pusher.Push()
			if err != nil {
				glog.V(0).Infof("could not push metrics to prometheus push gateway %s: %v", addr, err)
			}
		}
		if intervalSeconds <= 0 {
			intervalSeconds = 15
		}
		time.Sleep(time.Duration(intervalSeconds) * time.Second)
		addr, intervalSeconds = fnGetMetricsDest()
		if currentAddr != addr {
			pusher = push.New(addr, name).Gatherer(gatherer).Grouping("instance", instance)
			currentAddr = addr
		}

	}
}

func SourceName(port uint32) string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return fmt.Sprintf("%s:%d", hostname, port)
}
