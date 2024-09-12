package s3api

import (
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"net/http"
	"strconv"
	"time"
)

func track(f http.HandlerFunc, action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bucket, _ := s3_constants.GetBucketAndObject(r)
		w.Header().Set("Server", "SeaweedFS "+util.VERSION)
		recorder := stats_collect.NewStatusResponseWriter(w)
		start := time.Now()
		f(recorder, r)
		if recorder.Status == http.StatusForbidden {
			bucket = ""
		}
		stats_collect.S3RequestHistogram.WithLabelValues(action, bucket).Observe(time.Since(start).Seconds())
		stats_collect.S3RequestCounter.WithLabelValues(action, strconv.Itoa(recorder.Status), bucket).Inc()
	}
}

func TimeToFirstByte(action string, start time.Time, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	stats_collect.S3TimeToFirstByteHistogram.WithLabelValues(action, bucket).Observe(float64(time.Since(start).Milliseconds()))
}
