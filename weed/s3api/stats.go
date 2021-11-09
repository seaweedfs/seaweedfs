package s3api

import (
	stats_collect "github.com/chrislusf/seaweedfs/weed/stats"
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
		w.Header().Set("Server", "SeaweedFS S3")
		recorder := NewStatusResponseWriter(w)
		start := time.Now()
		f(recorder, r)
		stats_collect.S3RequestHistogram.WithLabelValues(action).Observe(time.Since(start).Seconds())
		stats_collect.S3RequestCounter.WithLabelValues(action, strconv.Itoa(recorder.Status)).Inc()
	}
}
