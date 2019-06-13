package weed_server

import "github.com/prometheus/client_golang/prometheus"

var (
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
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 18),
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(filerRequestCounter)
	prometheus.MustRegister(filerRequestHistogram)
}
