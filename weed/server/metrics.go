package weed_server

import "github.com/prometheus/client_golang/prometheus"

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

	volumeServerHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of filer request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})
)

func init() {

	filerGather.MustRegister(filerRequestCounter)
	filerGather.MustRegister(filerRequestHistogram)

	volumeServerGather.MustRegister(volumeServerRequestCounter)
	volumeServerGather.MustRegister(volumeServerHistogram)

}
