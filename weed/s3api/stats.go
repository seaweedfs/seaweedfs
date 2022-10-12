package s3api

import (
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"net/http"
	"strconv"
	"time"
)

type StatusRecorder struct {
	http.ResponseWriter
	Status int
}

func NewStatusResponseWriter(w http.ResponseWriter) *StatusRecorder {
	return &StatusRecorder{w, http.StatusOK}
}

func (r *StatusRecorder) WriteHeader(status int) {
	r.Status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *StatusRecorder) Flush() {
	r.ResponseWriter.(http.Flusher).Flush()
}

func track(f http.HandlerFunc, action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bucket, _ := s3_constants.GetBucketAndObject(r)
		w.Header().Set("Server", "SeaweedFS S3")
		recorder := NewStatusResponseWriter(w)
		start := time.Now()
		f(recorder, r)
		if recorder.Status == http.StatusForbidden {
			if m, _ := stats_collect.S3RequestCounter.GetMetricWithLabelValues(
				action, strconv.Itoa(http.StatusOK), bucket); m == nil {
				bucket = ""
			}
		}
		stats_collect.S3RequestHistogram.WithLabelValues(action, bucket).Observe(time.Since(start).Seconds())
		stats_collect.S3RequestCounter.WithLabelValues(action, strconv.Itoa(recorder.Status), bucket).Inc()
	}
}
