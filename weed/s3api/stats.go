package s3api

import (
	stats_collect "github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
	"net/http"
	"time"
)

func track(f http.HandlerFunc, action string) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Server", "Seaweed S3 "+util.VERSION)

		start := time.Now()
		stats_collect.S3RequestCounter.WithLabelValues(action).Inc()
		f(w, r)
		stats_collect.S3RequestHistogram.WithLabelValues(action).Observe(time.Since(start).Seconds())
	}
}
