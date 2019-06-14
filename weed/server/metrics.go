package weed_server

import (
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	filerGather        = prometheus.NewRegistry()
	volumeServerGather = prometheus.NewRegistry()

	filerRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "SeaweedFS",
			Subsystem: "filer",
			Name:      "request_total",
			Help:      "Counter of filer requests.",
		}, []string{"type"})

	filerRequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "SeaweedFS",
			Subsystem: "filer",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of filer request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})

	volumeServerRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "request_total",
			Help:      "Counter of filer requests.",
		}, []string{"type"})

	volumeServerRequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of filer request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})

	volumeServerVolumeCounter = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "volumes",
			Help:      "Number of volumes.",
		})

)

func init() {

	filerGather.MustRegister(filerRequestCounter)
	filerGather.MustRegister(filerRequestHistogram)

	volumeServerGather.MustRegister(volumeServerRequestCounter)
	volumeServerGather.MustRegister(volumeServerRequestHistogram)

}

func startPushingMetric(name string, gatherer *prometheus.Registry, addr string, intervalSeconds int) {
	if intervalSeconds == 0 || addr == "" {
		glog.V(0).Info("disable metrics reporting")
		return
	}
	glog.V(0).Infof("push metrics to %s every %d seconds", addr, intervalSeconds)
	go loopPushMetrics(name, gatherer, addr, intervalSeconds)
}

func loopPushMetrics(name string, gatherer *prometheus.Registry, addr string, intervalSeconds int) {

	pusher := push.New(addr, name).Gatherer(gatherer)

	for {
		err := pusher.Push()
		if err != nil {
			glog.V(0).Infof("could not push metrics to prometheus push gateway %s: %v", addr, err)
		}
		time.Sleep(time.Duration(intervalSeconds) * time.Second)
	}
}
