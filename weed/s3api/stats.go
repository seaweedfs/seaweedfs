package s3api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func track(f http.HandlerFunc, action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		inFlightGauge := stats_collect.S3InFlightRequestsGauge.WithLabelValues(action)
		inFlightGauge.Inc()
		defer inFlightGauge.Dec()

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
		stats_collect.RecordBucketActiveTime(bucket)
	}
}

func TimeToFirstByte(action string, start time.Time, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	stats_collect.S3TimeToFirstByteHistogram.WithLabelValues(action, bucket).Observe(float64(time.Since(start).Milliseconds()))
	stats_collect.RecordBucketActiveTime(bucket)
}

func BucketTrafficSent(bytesTransferred int64, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	stats_collect.RecordBucketActiveTime(bucket)
	stats_collect.S3BucketTrafficSentBytesCounter.WithLabelValues(bucket).Add(float64(bytesTransferred))
}
