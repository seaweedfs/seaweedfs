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

	VolumeServerRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "request_total",
			Help:      "Counter of filer requests.",
		}, []string{"type"})

	VolumeServerRequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of filer request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})

	VolumeServerVolumeCounter = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "volumes",
			Help:      "Number of volumes.",
		})

	VolumeServerEcShardCounter = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "ec_shards",
			Help:      "Number of EC shards.",
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

	VolumeServerGather.MustRegister(VolumeServerRequestCounter)
	VolumeServerGather.MustRegister(VolumeServerRequestHistogram)
	VolumeServerGather.MustRegister(VolumeServerVolumeCounter)
	VolumeServerGather.MustRegister(VolumeServerEcShardCounter)
	VolumeServerGather.MustRegister(VolumeServerDiskSizeGauge)

}

func StartPushingMetric(name, instance string, gatherer *prometheus.Registry, addr string, intervalSeconds int) {
	if intervalSeconds == 0 || addr == "" {
		glog.V(0).Info("disable metrics reporting")
		return
	}
	glog.V(0).Infof("push metrics to %s every %d seconds", addr, intervalSeconds)
	go loopPushMetrics(name, instance, gatherer, addr, intervalSeconds)
}

func loopPushMetrics(name, instance string, gatherer *prometheus.Registry, addr string, intervalSeconds int) {

	pusher := push.New(addr, name).Gatherer(gatherer).Grouping("instance", instance)

	for {
		err := pusher.Push()
		if err != nil {
			glog.V(0).Infof("could not push metrics to prometheus push gateway %s: %v", addr, err)
		}
		time.Sleep(time.Duration(intervalSeconds) * time.Second)
	}
}

func SourceName(port int) string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return fmt.Sprintf("%s:%d", hostname, port)
}
