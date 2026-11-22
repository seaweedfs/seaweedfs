package s3api

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"

	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
)

func track(f http.HandlerFunc, action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		inFlightGauge := stats_collect.S3InFlightRequestsGauge.WithLabelValues(action)
		inFlightGauge.Inc()
		defer inFlightGauge.Dec()

		bucket, _ := s3_constants.GetBucketAndObject(r)
		w.Header().Set("Server", "SeaweedFS "+version.VERSION)
		recorder := stats_collect.NewStatusResponseWriter(w)
		// Attach an audit-tracking flag to the request so handlers that call
		// PostLog directly mark it; we emit a fallback entry afterward for
		// handlers (e.g. successful GET/HEAD object) that don't.
		r = s3err.EnsureAuditTracking(r)
		// Attach a mutable identity holder before authentication so the fallback
		// entry can report the requester even though auth records it on a
		// request copy this middleware never sees.
		r = s3_constants.EnsureIdentityHolder(r)
		r = timeBodyReads(r)
		start := time.Now()
		f(recorder, r)
		elapsed := time.Since(start)
		if glog.V(2) {
			requester := s3_constants.GetIdentityNameFromContext(r)
			if requester == "" {
				requester = "-"
			}
			// %q keeps requester- and key-supplied bytes (e.g. newlines) on one line
			glog.Infof("%q %s %q %d %s", requester, r.Method, r.URL.Path, recorder.Status, elapsed)
		}
		if recorder.Status == http.StatusForbidden {
			bucket = ""
		}
		stats_collect.S3RequestHistogram.WithLabelValues(action, bucket).Observe(elapsed.Seconds())
		stats_collect.S3RequestCounter.WithLabelValues(action, strconv.Itoa(recorder.Status), bucket).Inc()
		stats_collect.RecordBucketActiveTime(bucket)
		if !s3err.AuditAlreadyLogged(r) {
			s3err.PostLog(r, recorder.Status, s3err.ErrNone)
		}
	}
}

func TimeToFirstByte(action string, start time.Time, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	stats_collect.S3TimeToFirstByteHistogram.WithLabelValues(action, bucket).Observe(float64(time.Since(start).Milliseconds()))
	stats_collect.RecordBucketActiveTime(bucket)
}

func BucketTrafficReceived(bytesReceived int64, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	stats_collect.RecordBucketActiveTime(bucket)
	stats_collect.S3BucketTrafficReceivedBytesCounter.WithLabelValues(bucket).Add(float64(bytesReceived))
}

func BucketTrafficSent(bytesTransferred int64, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	stats_collect.RecordBucketActiveTime(bucket)
	stats_collect.S3BucketTrafficSentBytesCounter.WithLabelValues(bucket).Add(float64(bytesTransferred))
}

// putStages times one object write's stages for S3PutStageHistogram.
type putStages struct {
	r          *http.Request
	operation  string
	readDuring time.Duration
	assign     atomic.Int64
	uploading  time.Duration
	uploaded   time.Time
}

func newPutStages(r *http.Request) *putStages {
	op := "PutObject"
	if isMultipartPartRequest(r) {
		op = "UploadPart"
	}
	return &putStages{r: r, operation: op}
}

// upload runs UploadReaderInChunks, timing volume assigns and the body reads it waits on.
func (p *putStages) upload(ctx context.Context, reader io.Reader, opt *operation.ChunkedUploadOption) (*operation.ChunkedUploadResult, error) {
	if assign := opt.AssignFunc; assign != nil {
		opt.AssignFunc = func(ctx context.Context, count int, expectedDataSize uint64) (*operation.VolumeAssignRequest, *operation.AssignResult, error) {
			start := time.Now()
			defer func() { p.assign.Add(int64(time.Since(start))) }()
			return assign(ctx, count, expectedDataSize)
		}
	}
	readBefore := bodyReadTime(p.r)
	start := time.Now()
	result, err := operation.UploadReaderInChunks(ctx, reader, opt)
	p.uploading = time.Since(start)
	p.readDuring = bodyReadTime(p.r) - readBefore
	if err == nil {
		p.uploaded = time.Now()
	}
	return result, err
}

// record observes the stages reached; metadata only after a successful upload.
func (p *putStages) record() {
	if p.uploading == 0 {
		return
	}
	observe := func(stage string, d time.Duration) {
		stats_collect.S3PutStageHistogram.WithLabelValues(p.operation, stage).Observe(d.Seconds())
	}
	observe("body_read", bodyReadTime(p.r))
	observe("assign", time.Duration(p.assign.Load()))
	observe("chunk_upload", max(p.uploading-p.readDuring, 0))
	if !p.uploaded.IsZero() {
		observe("metadata", time.Since(p.uploaded))
	}
}

type bodyReadTimeKey struct{}

// timeBodyReads makes r count the time its body reads block, for bodyReadTime.
func timeBodyReads(r *http.Request) *http.Request {
	if r.Body == nil || r.Body == http.NoBody {
		return r
	}
	elapsed := new(atomic.Int64)
	r = r.WithContext(context.WithValue(r.Context(), bodyReadTimeKey{}, elapsed))
	r.Body = &timedBody{ReadCloser: r.Body, elapsed: elapsed}
	return r
}

func bodyReadTime(r *http.Request) time.Duration {
	if elapsed, ok := r.Context().Value(bodyReadTimeKey{}).(*atomic.Int64); ok {
		return time.Duration(elapsed.Load())
	}
	return 0
}

type timedBody struct {
	io.ReadCloser
	elapsed *atomic.Int64
}

func (t *timedBody) Read(b []byte) (int, error) {
	start := time.Now()
	n, err := t.ReadCloser.Read(b)
	t.elapsed.Add(int64(time.Since(start)))
	return n, err
}
