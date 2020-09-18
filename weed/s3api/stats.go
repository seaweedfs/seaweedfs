package s3api

import (
	stats_collect "github.com/chrislusf/seaweedfs/weed/stats"
	"net/http"
	"time"
)

func stats(f http.HandlerFunc, action string) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		stats_collect.S3RequestCounter.WithLabelValues(action).Inc()
		f(w, r)
		stats_collect.S3RequestHistogram.WithLabelValues(action).Observe(time.Since(start).Seconds())
	}
}
